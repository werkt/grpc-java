package io.grpc.netty;

import io.grpc.Metadata;
import io.netty.handler.codec.http.HttpRequest;

public interface HttpHandler {
  boolean handles(HttpRequest request, Metadata headers);

  void handle(HttpRequest request, Metadata headers, HttpResponseObserver responseObserver);
}
