package io.grpc.netty;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import io.grpc.Attributes;
import io.grpc.Codec;
import io.grpc.Compressor;
import io.grpc.Decompressor;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.ServerStream;
import io.grpc.internal.ServerStreamListener;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.StreamListener;
import io.grpc.internal.TransportTracer;
import io.grpc.internal.TransportFrameUtil;
import io.grpc.internal.WritableBuffer;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedStream;
import io.perfmark.Link;
import io.perfmark.PerfMark;
import io.perfmark.Tag;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;
import java.util.logging.Level;
import java.util.logging.Logger;

class NettyHttp1ServerStream implements ServerStream {
  private static final Logger logger = Logger.getLogger(NettyServerStream.class.getName());

  // private final Sink sink = new Sink();
  private final StatsTraceContext statsTraceCtx;
  private final TransportState state;
  private final Channel channel;
  private final WriteQueue writeQueue;
  private final TransportTracer transportTracer;
  private final int streamId;

  private boolean outboundClosed;
  private boolean headersSent;

  NettyHttp1ServerStream(
      Channel channel,
      TransportState state,
      StatsTraceContext statsTraceCtx,
      TransportTracer transportTracer) {
    /* super(new NettyWritableBufferAllocator(channel.alloc()), statsTraceCtx); */
    this.statsTraceCtx = statsTraceCtx;
    this.state = checkNotNull(state, "transportState");
    this.channel = checkNotNull(channel, "channel");
    this.writeQueue = state.handler.getWriteQueue();
    this.transportTracer = checkNotNull(transportTracer, "transportTracer");
    this.streamId = state.id();
  }

  @Override
  public void request(int numMessages) {
    state.requestMessages(numMessages);
  }

  @Override
  public void writeMessage(InputStream message) {
    checkNotNull(message, "message");
    state.setResponseContentStream(message);
  }

  @Override
  public void flush() {
    try {
      state.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isReady() {
    return state.isReady();
  }

  @Override
  public void setCompressor(Compressor compressor) {
    if (compressor != Codec.Identity.NONE) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void setMessageCompression(boolean enable) {
    Preconditions.checkState(!enable, "message compression is not supported");
  }

  void writeHeaders(HttpHeaders headers) {
    Preconditions.checkNotNull(headers, "headers");

    headersSent = true;
    state.setResponseHeaders(headers);
    // abstractServerStreamSink().writeHeaders(headers);
  }

  @Override
  public void writeHeaders(Metadata headers) {
    Preconditions.checkNotNull(headers, "headers");

    /* there has to be a better way to get these things out... */
    HttpHeaders httpHeaders = new DefaultHttpHeaders();
    byte[][] http2Headers = TransportFrameUtil.toHttp2Headers(headers);
    for (int i = 0; i < http2Headers.length; i += 2) {
      httpHeaders.set(new String(http2Headers[i]), new String(http2Headers[i + 1]));
    }
    writeHeaders(httpHeaders);
  }

  void setStatus(HttpResponseStatus status) {
    // endOfMessages();
    // addStatusToTrailers(trailers, status);

    // Safe to set without synchronization because access is tightly controlled.
    // responseStatus is only set from here, and is read from a place that has happen-after
    // guarantees with respect to here.
    state.setResponseStatus(status);
    // abstractServerStreamSink().writeTrailers(trailers, headersSent, status);
    // end the content stream maybe?
    // state.writeTrailers(trailers, headersSent, status);
  }

  @Override
  public void close(Status status, Metadata trailers) {
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkNotNull(trailers, "trailers");
    if (!outboundClosed) {
      outboundClosed = true;

      int httpStatus = GrpcUtil.grpcCodeToHttpStatus(status.getCode());
      setStatus(HttpResponseStatus.valueOf(httpStatus));
    }
  }

  @Override
  public void cancel(Status status) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDecompressor(Decompressor decompressor) {
    state.setDecompressor(Preconditions.checkNotNull(decompressor, "decompressor"));
  }

  @Override
  public Attributes getAttributes() {
    return Attributes.EMPTY;
  }

  @Nullable
  @Override
  public String getAuthority() {
    return null;
  }

  @Override
  public StatsTraceContext statsTraceContext() {
    return statsTraceCtx;
  }

  @Override
  public void setListener(ServerStreamListener serverStreamListener) {
    state.setListener(serverStreamListener);
  }

  /*
  @Override
  protected Sink abstractServerStreamSink() {
    return sink;
  }
  */

  @Override
  public int streamId() {
    return streamId;
  }

  public HttpRequest request() {
    return state.request();
  }

  /*
  private class Sink implements AbstractServerStream.Sink {

    private void requestInternal(final int numMessages) {
      if (channel.eventLoop().inEventLoop()) {
        // Processing data read in the event loop so can call into the deframer immediately
        transportState().requestMessagesFromDeframer(numMessages);
      } else {
        final Link link = PerfMark.linkOut();
        channel.eventLoop().execute(new Runnable() {
          @Override
          public void run() {
            PerfMark.startTask(
                "NettyHttp1ServerStream$Sink.requestMessagesFromDeframer",
                transportState().tag());
            PerfMark.linkIn(link);
            try {
              transportState().requestMessagesFromDeframer(numMessages);
            } finally {
              PerfMark.stopTask(
                  "NettyHttp1ServerStream$Sink.requestMessagesFromDeframer",
                  transportState().tag());
            }
          }
        });
      }
    }

    @Override
    public void request(final int numMessages) {
      PerfMark.startTask("NettyHttp1ServerStream$Sink.request");
      try {
        requestInternal(numMessages);
      } finally {
        PerfMark.stopTask("NettyHttp1ServerStream$Sink.request");
      }
    }

    @Override
    public void writeHeaders(Metadata headers) {
      PerfMark.startTask("NettyHttp1ServerStream$Sink.writeHeaders");
      try {
        writeQueue.enqueue(
            SendResponseHeadersCommand.createHeaders(
                transportState(),
                Utils.convertServerHeaders(headers)),
            true);
      } finally {
        PerfMark.stopTask("NettyHttp1ServerStream$Sink.writeHeaders");
      }
    }

    private void writeFrameInternal(WritableBuffer frame, boolean flush, final int numMessages) {
      Preconditions.checkArgument(numMessages >= 0);
      if (frame == null) {
        writeQueue.scheduleFlush();
        return;
      }
      ByteBuf bytebuf = ((NettyWritableBuffer) frame).bytebuf().touch();
      final int numBytes = bytebuf.readableBytes();
      // Add the bytes to outbound flow control.
      onSendingBytes(numBytes);
      writeQueue.enqueue(new SendGrpcFrameCommand(transportState(), bytebuf, false), flush)
          .addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              // Remove the bytes from outbound flow control, optionally notifying
              // the client that they can send more bytes.
              transportState().onSentBytes(numBytes);
              if (future.isSuccess()) {
                transportTracer.reportMessageSent(numMessages);
              }
            }
          });
    }

    @Override
    public void writeFrame(WritableBuffer frame, boolean flush, final int numMessages) {
      PerfMark.startTask("NettyHttp1ServerStream$Sink.writeFrame");
      try {
        writeFrameInternal(frame, flush, numMessages);
      } finally {
        PerfMark.stopTask("NettyHttp1ServerStream$Sink.writeFrame");
      }
    }

    @Override
    public void writeTrailers(Metadata trailers, boolean headersSent, Status status) {
      PerfMark.startTask("NettyHttp1ServerStream$Sink.writeTrailers");
      try {
        Http2Headers http2Trailers = Utils.convertTrailers(trailers, headersSent);
        writeQueue.enqueue(
            SendResponseHeadersCommand.createTrailers(transportState(), http2Trailers, status),
            true);
      } finally {
        PerfMark.stopTask("NettyHttp1ServerStream$Sink.writeTrailers");
      }
    }

    @Override
    public void cancel(Status status) {
      PerfMark.startTask("NettyHttp1ServerStream$Sink.cancel");
      try {
        throw new UnsupportedOperationException();
        // writeQueue.enqueue(new CancelServerStreamCommand(transportState(), status), true);
      } finally {
        PerfMark.startTask("NettyHttp1ServerStream$Sink.cancel");
      }
    }
  }
  */

  static class TransportState implements StreamIdHolder {
    private final Object onReadyLock = new Object();
    private final NettyHttp1ServerHandler handler;
    private final EventLoop eventLoop;
    private final Tag tag;
    private final HttpRequest request;

    /** The status that the application used to close this stream. */
    @Nullable
    private HttpResponseStatus responseStatus = null;
    private HttpHeaders responseHeaders;
    private ServerStreamListener listener;
    private Decompressor decompressor;
    private boolean halfClosed = false;
    private InputStream responseContent = new ByteArrayInputStream(new byte[0]);
    private boolean shouldFlush = false;

    TransportState(
        NettyHttp1ServerHandler handler,
        EventLoop eventLoop,
        int maxMessageSize,
        StatsTraceContext statsTraceCtx,
        TransportTracer transportTracer,
        HttpRequest request) {
      /*
      super(
          maxMessageSize,
          statsTraceCtx,
          checkNotNull(transportTracer, "transportTracer"));
          */
      this.handler = checkNotNull(handler, "handler");
      this.eventLoop = eventLoop;
      this.tag = PerfMark.createTag("unknown method", id());
      this.request = request;
    }

    public void setListener(ServerStreamListener listener) {
      this.listener = listener;
    }

    void setDecompressor(Decompressor decompressor) {
      throw new UnsupportedOperationException();
    }

    // get things to call this
    void requestMessages(int numMessages) {
      // Preconditions.checkState(numMessages == 1, "numMessages > 1 unsupported, was " + numMessages);
      /* will matter on non-unary */
      listener.onReady();
      listener.messagesAvailable(new SingleMessageProducer(new EmptyInputStream()));
      if (halfClosed) {
        listener.halfClosed();
      }
    }

    @Override
    public int id() {
      /* pretty sure this won't matter... */
      return 1;
    }

    @Override
    public Tag tag() {
      return tag;
    }

    void setHalfClosed() {
      halfClosed = true;
    }

    /**
     * Stores the {@code Status} that the application used to close this stream.
     */
    private void setResponseStatus(HttpResponseStatus status) {
      Preconditions.checkState(responseStatus == null, "responseStatus can only be set once");
      this.responseStatus = status;
      // this is our end event, but http sucks, so....
      // figure out the ordering to see if we can send the content, even streaming
      if (shouldFlush) {
        try {
          flushRequest();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        shouldFlush = false;
      }
    }

    private void writeTrailers(Metadata trailers, boolean headersSent, HttpResponseStatus status) {
      /* technically shouldn't flush, should just schedule one */
      /* maybe do something else with the trailers if we also have headers? */
      if (!headersSent) {
        throw new UnsupportedOperationException();
      }
      try {
        flush();
      } catch (IOException e) {
        // shouldn't happen
        e.printStackTrace();
      }
    }

    private void flush() throws IOException {
      System.out.println("flushing...");
      if (responseStatus != null) {
        flushRequest();
      } else {
        shouldFlush = true;
      }
    }

    private void flushRequest() throws IOException {
      System.out.println("flushing content...");

      /* technically this should be reentrant and stateful for periodic flushes */

      /* need to detect if we've sent the response already */
      Preconditions.checkState(responseStatus != null, "transportState was not closed");
      HttpResponse response = new DefaultHttpResponse(
          request.protocolVersion(),
          responseStatus);
      response.headers().setAll(responseHeaders);

      boolean keepAlive = HttpUtil.isKeepAlive(request);
      if (keepAlive) {
        if (!request.protocolVersion().isKeepAliveDefault()) {
          response.headers().set(CONNECTION, KEEP_ALIVE);
        }
      } else {
        // Tell the client we're going to close the connection.
        response.headers().set(CONNECTION, CLOSE);
      }
      ChannelFuture contentWritten = handler.write(response); // should we tie to future?

      /* tie on to existing contentWritten? */
      contentWritten = handler.write(new HttpChunkedInput(new ChunkedStream(responseContent)));
      System.err.println("");
      final byte[] ESC = {27, (byte) '['};
      contentWritten.addListener(
          new ChannelProgressiveFutureListener() {
            long deliveredTotal = -1;

            @Override
            public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
              clear();
              if (total < 0) { // total unknown
                System.err.println(future.channel() + " Transfer progress: " + progress);
              } else {
                deliveredTotal = total;
                System.err.println(future.channel() + " Transfer progress: " + progress + " / " + total);
              }
            }

            void clear() {
              /*
              try {
                System.err.write(ESC);
                System.err.write((byte) 'A');
                System.err.write(ESC);
                System.err.write((byte) 'K');
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              */
            }

            @Override
            public void operationComplete(ChannelProgressiveFuture future) {
              handler.close();
              try {
                responseContent.close();
              } catch (IOException e) {
                e.printStackTrace(); // do something more?
              }
              clear();
              System.err.println(future.channel() + " Transfer of " + deliveredTotal + " complete.");
            }
          });
      /* should be writing the above in phases as set, not just here */
      handler.flush();

      if (!keepAlive) {
        contentWritten.addListener(ChannelFutureListener.CLOSE);
      }
    }

    private void setResponseHeaders(HttpHeaders responseHeaders) {
      this.responseHeaders = responseHeaders;
    }

    private void setResponseContentStream(InputStream responseContent) {
      this.responseContent = responseContent;
    }

    private static class EmptyInputStream extends InputStream {
      @Override
      public int read() {
        return -1;
      }
    }

    private static class SingleMessageProducer implements StreamListener.MessageProducer {
      private InputStream message;

      private SingleMessageProducer(InputStream message) {
        this.message = message;
      }

      @Nullable
      @Override
      public InputStream next() {
        InputStream messageToReturn = message;
        message = null;
        return messageToReturn;
      }
    }

    private boolean isReady() {
      synchronized (onReadyLock) {
        throw new UnsupportedOperationException();
        // return allocated && numSentBytesQueued < DEFAULT_ONREADY_THRESHOLD && !deallocated;
      }
    }

    private void notifyIfReady() {
      boolean doNotify;
      synchronized (onReadyLock) {
        doNotify = isReady();
      }
      if (doNotify) {
        listener.onReady();
      }
    }

    private HttpRequest request() {
      return request;
    }
  }
}
