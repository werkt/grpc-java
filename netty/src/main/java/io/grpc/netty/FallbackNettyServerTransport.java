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

import static java.lang.Math.min;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.InternalChannelz.SocketStats;
import io.grpc.InternalLogId;
import io.grpc.internal.ServerListener;
import io.grpc.internal.ServerTransportListener;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.net.SocketAddress;

class FallbackNettyServerTransport extends AbstractServerTransport {
  private static final ByteBuf HTTP_1_X_BUF = Unpooled.unreleasableBuffer(
      Unpooled.wrappedBuffer(new byte[] {'H', 'T', 'T', 'P', '/', '1', '.'})).asReadOnly();

  private final InternalLogId logId;
  private final Channel channel;
  private final ChannelPromise channelDone;
  private final ServerTransportFactory http2Factory;
  private final ServerTransportFactory http1Factory;
  private ServerListener serverListener;
  private ServerTransportListener listener;

  class DecodeAndCommitHandler extends ChannelDuplexHandler {
    NettyServerTransport transport;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      super.handlerAdded(ctx);
      // kick off protocol negotiation.
      ctx.pipeline().fireUserEventTriggered(ProtocolNegotiationEvent.DEFAULT);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      super.handlerRemoved(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      assert cause != null;
      cause.printStackTrace();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      Preconditions.checkState(transport == null, "Fallback already occurred");
      if (transport == null && msg instanceof ByteBuf) {
        ByteBuf data = (ByteBuf) msg;
        int maxSearch = 1024; // picked because 512 is too little, and 2048 too much
        int http1Index = ByteBufUtil.indexOf(
            HTTP_1_X_BUF,
            data.slice(data.readerIndex(), min(data.readableBytes(), maxSearch)));
        if (http1Index != -1) {
          /* switch to http1 */
          transport = http1Factory.create(channel, channelDone);
        } else {
          /* switch to http2 */
          transport = http2Factory.create(channel, channelDone);
        }
        ServerTransportListener transportListener = serverListener.transportCreated(transport);
        transport.start(transportListener);
        listener.transportTerminated();
        ctx.channel().pipeline().remove(this);
      }
      super.channelRead(ctx, msg);
    }
  }

  FallbackNettyServerTransport(
      Channel channel,
      ChannelPromise channelDone,
      ServerTransportFactory http2Factory,
      ServerTransportFactory http1Factory) {
    this.channel = channel;
    this.channelDone = channelDone;
    this.http2Factory = http2Factory;
    this.http1Factory = http1Factory;
    SocketAddress remote = channel.remoteAddress();
    this.logId = InternalLogId.allocate(getClass(), remote != null ? remote.toString() : null);
  }

  public void start(ServerListener serverListener, ServerTransportListener listener) {
    this.serverListener = serverListener;
    Preconditions.checkState(this.listener == null, "Handler already registered");
    this.listener = listener;

    channel.pipeline().addLast(new DecodeAndCommitHandler());
  }

  @Override
  public InternalLogId getLogId() {
    return logId;
  }

  @Override
  Channel channel() {
    return channel;
  }

  @Override
  public ListenableFuture<SocketStats> getStats() {
    /* maybe make a future for once we make a protocol decision... */
    return Futures.immediateFailedFuture(new UnsupportedOperationException());
  }
}
