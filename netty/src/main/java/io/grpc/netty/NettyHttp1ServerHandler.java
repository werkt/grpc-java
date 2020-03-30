/*
 * Copyright 2014 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.netty;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.internal.ServerStream;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.TransportTracer;
import io.grpc.netty.NettyHttp1ServerStream.TransportState;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

class NettyHttp1ServerHandler extends SimpleChannelInboundHandler<HttpObject> {
  private final ServerTransportListener transportListener;
  private final int maxMessageSize;
  private final List<? extends ServerStreamTracer.Factory> streamTracerFactories;
  private final TransportTracer transportTracer;

  private ChannelHandlerContext ctx;
  private WriteQueue serverWriteQueue;
  private TransportState state;

  NettyHttp1ServerHandler(
      ServerTransportListener transportListener,
      List<? extends ServerStreamTracer.Factory> streamTracerFactories,
      TransportTracer transportTracer,
      int maxMessageSize) {
    this.maxMessageSize = maxMessageSize;

    this.transportListener = checkNotNull(transportListener, "transportListener");
    this.streamTracerFactories = checkNotNull(streamTracerFactories, "streamTracerFactories");
    this.transportTracer = checkNotNull(transportTracer, "transportTracer");
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    serverWriteQueue = new WriteQueue(ctx.channel());

    // connection age monitor?
    // connection idle manager?
    // server keepalive?
    // transport tracer?

    this.ctx = ctx;
    super.handlerAdded(ctx);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    state.setHalfClosed();
    // ctx.flush();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
    if (msg instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) msg;
      // get headers out of msg
      Metadata metadata = Utils.convertHeaders(request.headers());
      StatsTraceContext statsTraceCtx =
          StatsTraceContext.newServerContext(streamTracerFactories, "unknown method", metadata);

      state = new TransportState(
          this,
          ctx.channel().eventLoop(),
          maxMessageSize,
          statsTraceCtx,
          transportTracer,
          request);

      ServerStream stream = new NettyHttp1ServerStream(
          ctx.channel(),
          state,
          statsTraceCtx,
          transportTracer);
      try {
        transportListener.httpStreamCreated(stream, request.method().name(), new URI(request.uri()), metadata);
      } catch (URISyntaxException e) {
        // probably shouldn't happen
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }

  ChannelFuture write(FullHttpResponse response) {
    return ctx.write(response);
  }

  void flush() {
    ctx.flush();
  }

  WriteQueue getWriteQueue() {
    return serverWriteQueue;
  }
}
