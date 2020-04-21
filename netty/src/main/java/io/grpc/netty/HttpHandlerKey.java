package io.grpc.netty;

import io.grpc.HttpRequest.Method;
import io.grpc.HttpRulePattern;
import java.net.URI;

final class HttpHandlerKey {
  private final Method method;
  private final String pattern;
  private final HttpRulePattern rulePattern;

  HttpHandlerKey(Method method, String pattern) {
    this.method = method;
    this.pattern = pattern;
    this.rulePattern = HttpRulePattern.parse(pattern);
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

  boolean matches(Method method, URI uri) {
    return this.method == method && rulePattern.matches(uri);
  }
}
