/**
 * Copyright © 2025 Total Wine &amp; More
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.connect.errors.DataException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A parsed field path specification used to target fields that are nested inside structs, maps and
 * arrays. Transformations use this together with {@link FieldPathUpdater} so that field targeting
 * behaves consistently across the project.
 *
 * <p>Supported syntax:</p>
 * <ul>
 *   <li>{@code ITEM.ATTRIBUTES.IS_DIGITAL_GOOD} &mdash; exact, dotted path.</li>
 *   <li>{@code RATINGS[*].VINTAGE} &mdash; the {@code VINTAGE} field of every element of the
 *   {@code RATINGS} array.</li>
 *   <li>{@code RATINGS[1].VINTAGE} &mdash; the {@code VINTAGE} field of the second element only.</li>
 *   <li>{@code ITEM.*} or {@code ITEM.IS_*} &mdash; glob wildcards ({@code *} and {@code ?}) may
 *   appear anywhere within a name.</li>
 *   <li>{@code **.VINTAGE} &mdash; every {@code VINTAGE} field at any depth.</li>
 *   <li>{@code $} or an empty string &mdash; the value itself.</li>
 * </ul>
 *
 * <p>A leading {@code $} or {@code $.} is accepted so JSONPath style specs can be used as well.
 * Name matching is case sensitive because Kafka Connect field names are case sensitive.</p>
 */
public final class FieldPath {
  private final String spec;
  private final List<Segment> segments;

  private FieldPath(String spec, List<Segment> segments) {
    this.spec = spec;
    this.segments = segments;
  }

  /**
   * Parses a field path specification.
   *
   * @param spec the specification to parse. A null or empty spec targets the value itself.
   * @throws DataException if the specification is malformed.
   */
  public static FieldPath of(String spec) {
    String input = null == spec ? "" : spec.trim();
    return new FieldPath(input, parse(input));
  }

  public static List<FieldPath> ofAll(List<String> specs) {
    List<FieldPath> result = new ArrayList<>(specs.size());
    for (String spec : specs) {
      result.add(of(spec));
    }
    return result;
  }

  /**
   * @return the original specification this path was parsed from.
   */
  public String spec() {
    return this.spec;
  }

  List<Segment> segments() {
    return this.segments;
  }

  @Override
  public String toString() {
    return this.spec;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FieldPath)) {
      return false;
    }
    return this.spec.equals(((FieldPath) o).spec);
  }

  @Override
  public int hashCode() {
    return this.spec.hashCode();
  }

  private static List<Segment> parse(String spec) {
    if (spec.isEmpty() || "$".equals(spec)) {
      return Collections.emptyList();
    }

    int position = 0;
    if ('$' == spec.charAt(0)) {
      position = 1;
    }

    List<Segment> segments = new ArrayList<>();
    StringBuilder name = new StringBuilder();
    while (position < spec.length()) {
      char current = spec.charAt(position);
      if ('.' == current) {
        flush(spec, name, segments);
        position++;
      } else if ('[' == current) {
        flush(spec, name, segments);
        int close = spec.indexOf(']', position);
        if (close < 0) {
          throw new DataException("Field path '" + spec + "' is missing a closing ']'.");
        }
        segments.add(indexSegment(spec, spec.substring(position + 1, close).trim()));
        position = close + 1;
      } else if (']' == current) {
        throw new DataException("Field path '" + spec + "' has an unexpected ']'.");
      } else {
        name.append(current);
        position++;
      }
    }
    flush(spec, name, segments);

    if (segments.isEmpty()) {
      throw new DataException("Field path '" + spec + "' does not contain any fields.");
    }
    return Collections.unmodifiableList(segments);
  }

  private static void flush(String spec, StringBuilder name, List<Segment> segments) {
    if (0 == name.length()) {
      return;
    }
    String text = name.toString();
    name.setLength(0);
    if ("**".equals(text)) {
      segments.add(RecursiveSegment.INSTANCE);
    } else if (text.contains("**")) {
      throw new DataException("Field path '" + spec + "' may only use '**' as an entire path element.");
    } else {
      segments.add(new NameSegment(text));
    }
  }

  private static Segment indexSegment(String spec, String text) {
    if ("*".equals(text) || text.isEmpty()) {
      return IndexSegment.ALL;
    }
    try {
      int index = Integer.parseInt(text);
      if (index < 0) {
        throw new DataException("Field path '" + spec + "' has a negative array index.");
      }
      return new IndexSegment(index);
    } catch (NumberFormatException ex) {
      throw new DataException(
          "Field path '" + spec + "' has an invalid array index '" + text + "'. Use a non negative number or '*'."
      );
    }
  }

  interface Segment {
  }

  /**
   * Matches a field of a struct or a key of a map.
   */
  static final class NameSegment implements Segment {
    private final String name;
    private final Pattern pattern;

    NameSegment(String name) {
      this.name = name;
      this.pattern = isGlob(name) ? Pattern.compile(toRegex(name)) : null;
    }

    boolean matches(String candidate) {
      if (null == candidate) {
        return false;
      }
      return null == this.pattern ? this.name.equals(candidate) : this.pattern.matcher(candidate).matches();
    }

    @Override
    public String toString() {
      return this.name;
    }

    private static boolean isGlob(String name) {
      return name.indexOf('*') >= 0 || name.indexOf('?') >= 0;
    }

    private static String toRegex(String glob) {
      StringBuilder regex = new StringBuilder();
      StringBuilder literal = new StringBuilder();
      for (int i = 0; i < glob.length(); i++) {
        char current = glob.charAt(i);
        if ('*' == current || '?' == current) {
          if (literal.length() > 0) {
            regex.append(Pattern.quote(literal.toString()));
            literal.setLength(0);
          }
          regex.append('*' == current ? ".*" : ".");
        } else {
          literal.append(current);
        }
      }
      if (literal.length() > 0) {
        regex.append(Pattern.quote(literal.toString()));
      }
      return regex.toString();
    }
  }

  /**
   * Matches one or every element of an array.
   */
  static final class IndexSegment implements Segment {
    static final IndexSegment ALL = new IndexSegment(-1);

    private final int index;

    IndexSegment(int index) {
      this.index = index;
    }

    boolean matches(int candidate) {
      return this.index < 0 || this.index == candidate;
    }

    @Override
    public String toString() {
      return "[" + (this.index < 0 ? "*" : Integer.toString(this.index)) + "]";
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof IndexSegment && ((IndexSegment) o).index == this.index;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.index);
    }
  }

  /**
   * Matches zero or more levels of nesting.
   */
  static final class RecursiveSegment implements Segment {
    static final RecursiveSegment INSTANCE = new RecursiveSegment();

    private RecursiveSegment() {
    }

    @Override
    public String toString() {
      return "**";
    }
  }
}
