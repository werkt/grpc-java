package io.grpc;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.net.URI;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public final class HttpRulePattern {
  public static HttpRulePattern parse(String input) {
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

  static HttpRulePattern parseImpl(CharacterIterator it) {
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

    return new HttpRulePattern(Pattern.compile(pattern.toString()), fieldNames);
  }

  private final Pattern pattern;
  private final List<String> fieldNames;

  private HttpRulePattern(Pattern pattern, List<String> fieldNames) {
    this.pattern = pattern;
    this.fieldNames = fieldNames;
  }

  public boolean matches(URI uri) {
    return matcher(uri).matches();
  }

  public Matcher matcher(URI uri) {
    System.out.println("Getting a matcher for " + uri + " with " + pattern);
    return pattern.matcher(uri.getPath());
  }

  public String field(int i) {
    return fieldNames.get(i);
  }
}
