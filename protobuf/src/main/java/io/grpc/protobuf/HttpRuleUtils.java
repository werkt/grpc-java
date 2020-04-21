package io.grpc.protobuf;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.MethodDescriptor.HttpRequestDecoder;
import io.grpc.MethodDescriptor.HttpResponseEncoder;
import io.grpc.HttpRequest;
import io.grpc.HttpRequest.Method;
import io.grpc.HttpRulePattern;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;

public final class HttpRuleUtils {
  public static <T extends Message> HttpRequestDecoder<T> decoder(final Method method, final String pattern, final T request) {
    return new HttpRequestDecoder<T>() {
      HttpRulePattern rulePattern = HttpRulePattern.parse(pattern);

      @Override
      public boolean matches(HttpRequest.Method requestMethod, URI uri) {
        return requestMethod == method
            && rulePattern.matches(uri);
      }

      @Override
      public T decode(URI uri, InputStream body) {
        /* deal with the body later */
        Message.Builder builder = request.toBuilder();
        Matcher matcher = rulePattern.matcher(uri);
        Preconditions.checkState(matcher.matches(), "http rule pattern does not match uri " + uri);
        Descriptor descriptor = builder.getDescriptorForType();
        for (int i = 0; i < matcher.groupCount(); i++) {
          String group = matcher.group(i + 1);
          String fieldName = rulePattern.field(i);
          builder.setField(descriptor.findFieldByName(fieldName), group);
        }
        return (T) builder.build();
      }
    };
  }

  public static <T extends Message> HttpResponseEncoder<T> encoder(T response) {
    return new HttpResponseEncoder<T>() {
      @Override
      public InputStream encode(T response) {
        try {
          String json = JsonFormat.printer().print(response);
          return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private HttpRuleUtils() {
  }
}
