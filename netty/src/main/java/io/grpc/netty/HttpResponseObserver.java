package io.grpc.netty;

import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.OutputStream;

public abstract class HttpResponseObserver extends OutputStream {
  public abstract void setHeader(String key, String value);

  public abstract void onCompleted();

  public abstract void onError(HttpResponseStatus status);
}
