package io.grpc.netty;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.InputStream;

public abstract class HttpResponseObserver {
  public abstract HttpHeaders headers();

  public abstract void onSuccess(InputStream responseStream);

  public abstract void onError(HttpResponseStatus status);
}
