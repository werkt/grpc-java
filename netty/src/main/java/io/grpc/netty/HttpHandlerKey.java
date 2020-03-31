package io.grpc.netty;

import io.grpc.HttpRequest.Method;

final class HttpHandlerKey {
  private final Method method;
  private final String pattern;

  HttpHandlerKey(Method method, String pattern) {
    this.method = method;
    this.pattern = pattern;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HttpHandlerKey) {
      HttpHandlerKey key = (HttpHandlerKey) obj;
      return method == key.method && pattern.equals(key.pattern);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return method.hashCode() ^ pattern.hashCode();
  }

  boolean matches(Method method, String pattern) {
    // FIXME pattern match
    return this.method == method && this.pattern.equals(pattern);
  }
}
