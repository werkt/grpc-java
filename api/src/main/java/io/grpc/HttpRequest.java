package io.grpc;

public final class HttpRequest {
  public enum Method {
    GET,
    PUT,
    POST,
    DELETE,
    PATCH,
    UNKNOWN
  }

  public static Method parseMethod(String name) {
    if (name.equals("GET")) {
      return Method.GET;
    }
    if (name.equals("PUT")) {
      return Method.PUT;
    }
    if (name.equals("POST")) {
      return Method.POST;
    }
    if (name.equals("DELETE")) {
      return Method.DELETE;
    }
    if (name.equals("PATCH")) {
      return Method.PATCH;
    }
    return Method.UNKNOWN;
  }
}
