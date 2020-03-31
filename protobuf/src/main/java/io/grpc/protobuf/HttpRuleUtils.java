package io.grpc.protobuf;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.CharSource;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.MethodDescriptor.HttpRequestDecoder;
import io.grpc.MethodDescriptor.HttpResponseEncoder;
import io.grpc.HttpRequest;
import io.grpc.HttpRequest.Method;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public final class HttpRuleUtils {
  public static <T extends Message> HttpRequestDecoder<T> decoder(final Method method, final String pattern, final T request) {
    return new HttpRequestDecoder<T>() {
      RulePattern rulePattern = RulePattern.parse(pattern);

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

  public static class RulePattern {
    static RulePattern parse(String input) {
      CharacterIterator it = new StringCharacterIterator(input);
      try {
        return parseImpl(it);
      } catch (Exception e) {
        System.err.println("rule parse error: " + input);
        System.err.println("                  " + String.format("%1$" + it.getIndex() + "s", "") + "^");
        System.err.println(e.getMessage() + " at index " + it.getIndex());
        Throwables.throwIfInstanceOf(e, RuntimeException.class);
        throw new RuntimeException(e);
      }
    }

    static RulePattern parseImpl(CharacterIterator it) {
      StringBuilder pattern = new StringBuilder();
      List<String> fieldNames = new ArrayList<>();
      StringBuilder fieldName = null;
      pattern.append("^");
      int wildcard = 0;
      boolean mustBeSlash = true;
      boolean wildcardValid = false;
      boolean lastWasSlash = false;
      for (char c = it.current(); c != CharacterIterator.DONE; c = it.next()) {
        boolean isFieldEquals = false;
        // override mustBeSlash here
        if (c == '}') {
          if (wildcard < 2) {
            pattern.append("[^/]+");
            wildcard = 0;
          }
          pattern.append(")");
          if (fieldName != null) {
            fieldNames.add(fieldName.toString());
            fieldName = null;
          }
          wildcard = 0;
          mustBeSlash = true;
        } else if (c == '/' || mustBeSlash) {
          Preconditions.checkState(c != '/' || !lastWasSlash, "unexpected '/'");
          Preconditions.checkState(c == '/', "unexpected '" + c + "'");
          if (wildcard == 1) {
            pattern.append("[^/]+");
            wildcard = 0;
          }
          pattern.append(c);
          mustBeSlash = false;
        } else if (c == '*') {
          Preconditions.checkState(wildcard < 2 && wildcardValid, "unexpected '*'");
          wildcard++;
          if (wildcard == 2) {
            pattern.append(".*");
            mustBeSlash = true;
          }
        } else if (c == '{') {
          pattern.append("(");
          fieldName = new StringBuilder();
        } else if (fieldName != null) {
          if (c == '=') {
            fieldNames.add(fieldName.toString());
            fieldName = null;
            isFieldEquals = true;
          } else {
            fieldName.append(c);
          }
        } else {
          Preconditions.checkState(wildcard == 0, "unexpected '" + c + "'");
          pattern.append(c);
        }
        lastWasSlash = c == '/';
        wildcardValid = lastWasSlash || isFieldEquals || c == '*';
      }
      if (wildcard == 1) {
        pattern.append("[^/]+");
      }
      pattern.append("$");

      return new RulePattern(Pattern.compile(pattern.toString()), fieldNames);
    }

    private final Pattern pattern;
    private final List<String> fieldNames;

    private RulePattern(Pattern pattern, List<String> fieldNames) {
      this.pattern = pattern;
      this.fieldNames = fieldNames;
    }

    public boolean matches(URI uri) {
      return matcher(uri).matches();
    }

    public Matcher matcher(URI uri) {
      return pattern.matcher(uri.getPath());
    }

    public String field(int i) {
      return fieldNames.get(i);
    }
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
