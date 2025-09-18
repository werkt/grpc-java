package io.grpc.netty;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.InputStream;

class NettyHttpResponseObserver extends HttpResponseObserver {
  private final NettyHttp1ServerStream stream;
  private boolean writeHeaders = true;

  private HttpHeaders headers = new DefaultHttpHeaders();

  NettyHttpResponseObserver(NettyHttp1ServerStream stream) {
    this.stream = stream;
  }

  private void close(HttpResponseStatus status) {
    if (writeHeaders) {
      stream.writeHeaders(headers);
    }
    writeHeaders = false;
    stream.setStatus(status);
    stream.flush();
  }

  @Override
  public void onSuccess(InputStream responseStream) {
    if (writeHeaders) {
      stream.writeHeaders(headers);
    }
    writeHeaders = false;
    stream.writeMessage(responseStream);
    close(OK);
  }

  @Override
  public void onError(HttpResponseStatus status) {
    // stream response?
    close(status);
  }

  @Override
  public HttpHeaders headers() {
    return headers;
  }
}
