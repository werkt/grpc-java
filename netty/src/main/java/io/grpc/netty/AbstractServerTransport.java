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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import io.grpc.internal.ServerTransport;
import io.netty.channel.Channel;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;

/**
 * The Netty-based server transport interface.
 */
abstract class AbstractServerTransport implements ServerTransport {

  // Some exceptions are not very useful and add too much noise to the log
  private static final ImmutableList<String> QUIET_EXCEPTIONS = ImmutableList.of(
      "NativeIoException" /* Netty exceptions */);

  /**
   * Accepts a throwable and returns the appropriate logging level. Uninteresting exceptions
   * should not clutter the log.
   */
  @VisibleForTesting
  static Level getLogLevel(Throwable t) {
    if (t.getClass().equals(IOException.class)
        || QUIET_EXCEPTIONS.contains(t.getClass().getSimpleName())) {
      return Level.FINE;
    }
    return Level.INFO;
  }

  abstract Channel channel();

  @Override
  public ScheduledExecutorService getScheduledExecutorService() {
    return channel().eventLoop();
  }

  @Override
  public void shutdown() {
    if (channel().isOpen()) {
      channel().close();
    }
  }

  @Override
  public void shutdownNow(Status reason) {
    if (channel().isOpen()) {
      channel().writeAndFlush(new ForcefulCloseCommand(reason));
    }
  }
}
