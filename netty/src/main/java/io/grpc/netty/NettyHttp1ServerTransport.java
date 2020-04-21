/*
 * Copyright 2020 The gRPC Authors
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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.InternalChannelz.SocketStats;
import io.grpc.InternalLogId;
import io.grpc.ServerStreamTracer;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.TransportTracer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.SocketAddress;
import java.util.List;
import java.util.logging.Logger;

class NettyHttp1ServerTransport extends NettyServerTransport {
  // connectionLog is for connection related messages only
  private static final Logger connectionLog = Logger.getLogger(
      String.format("%s.connections", NettyHttp1ServerTransport.class.getName()));

  private final InternalLogId logId;
  private final Channel channel;
  private final ChannelPromise channelUnused;
  private ServerTransportListener listener;
  private boolean terminated;
  private final int maxMessageSize;
  private final List<? extends ServerStreamTracer.Factory> streamTracerFactories;
  private final TransportTracer transportTracer;

  NettyHttp1ServerTransport(
      Channel channel,
      ChannelPromise channelUnused,
      List<? extends ServerStreamTracer.Factory> streamTracerFactories,
      TransportTracer transportTracer,
      int maxMessageSize) {
    this.channel = Preconditions.checkNotNull(channel, "channel");
    this.channelUnused = channelUnused;
    this.streamTracerFactories =
        Preconditions.checkNotNull(streamTracerFactories, "streamTracerFactories");
    this.maxMessageSize = maxMessageSize;
    this.transportTracer = Preconditions.checkNotNull(transportTracer, "transportTracer");
    SocketAddress remote = channel.remoteAddress();
    this.logId = InternalLogId.allocate(getClass(), remote != null ? remote.toString() : null);
  }

  @Override
  public void start(ServerTransportListener listener) {
    Preconditions.checkState(this.listener == null, "Handler already registered");
    this.listener = listener;

    ChannelHandler handler = new NettyHttp1ServerHandler(
        listener,
        streamTracerFactories,
        transportTracer,
        maxMessageSize);

    // Notify when the channel closes.
    final class TerminationNotifier implements ChannelFutureListener {
      boolean done;

      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (!done) {
          done = true;
          notifyTerminated(/* FIXME */ null);
        }
      }
    }

    ChannelFutureListener terminationNotifier = new TerminationNotifier();
    channelUnused.addListener(terminationNotifier);

    /* ssl? */
    channel.pipeline().addLast(new HttpServerCodec());
    channel.pipeline().addLast(new HttpServerExpectContinueHandler());
    channel.pipeline().addLast(new ChunkedWriteHandler());
    channel.pipeline().addLast(handler);
  }

  @Override
  public InternalLogId getLogId() {
    return logId;
  }

  @Override
  Channel channel() {
    return channel;
  }

  private void notifyTerminated(Throwable t) {
    if (t != null) {
      connectionLog.log(getLogLevel(t), "Transport failed", t);
    }
    if (!terminated) {
      terminated = true;
      listener.transportTerminated();
    }
  }

  @Override
  public ListenableFuture<SocketStats> getStats() {
    return Futures.immediateFailedFuture(new UnsupportedOperationException());
  }
}
