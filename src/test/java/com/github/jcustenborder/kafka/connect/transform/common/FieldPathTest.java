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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FieldPathTest {

  @Test
  public void dottedPath() {
    List<FieldPath.Segment> segments = FieldPath.of("ITEM.ATTRIBUTES.IS_DIGITAL_GOOD").segments();
    assertEquals(3, segments.size());
    assertEquals("ITEM", segments.get(0).toString());
    assertEquals("ATTRIBUTES", segments.get(1).toString());
    assertEquals("IS_DIGITAL_GOOD", segments.get(2).toString());
  }

  @Test
  public void jsonPathPrefixIsIgnored() {
    assertEquals(
        FieldPath.of("ITEM.ATTRIBUTES").segments().size(),
        FieldPath.of("$.ITEM.ATTRIBUTES").segments().size()
    );
    assertEquals("ITEM", FieldPath.of("$.ITEM.ATTRIBUTES").segments().get(0).toString());
  }

  @Test
  public void wildcardArrayIndex() {
    List<FieldPath.Segment> segments = FieldPath.of("RATINGS[*].VINTAGE").segments();
    assertEquals(3, segments.size());
    assertEquals("RATINGS", segments.get(0).toString());
    assertEquals("[*]", segments.get(1).toString());
    assertEquals("VINTAGE", segments.get(2).toString());
  }

  @Test
  public void specificArrayIndex() {
    List<FieldPath.Segment> segments = FieldPath.of("RATINGS[1].VINTAGE").segments();
    assertEquals("[1]", segments.get(1).toString());
    FieldPath.IndexSegment index = (FieldPath.IndexSegment) segments.get(1);
    assertFalse(index.matches(0));
    assertTrue(index.matches(1));
  }

  @Test
  public void allIndexesMatch() {
    FieldPath.IndexSegment index = (FieldPath.IndexSegment) FieldPath.of("A[*]").segments().get(1);
    assertTrue(index.matches(0));
    assertTrue(index.matches(7));
  }

  @Test
  public void nestedArrays() {
    List<FieldPath.Segment> segments = FieldPath.of("A[0][1].B").segments();
    assertEquals(4, segments.size());
    assertEquals("[0]", segments.get(1).toString());
    assertEquals("[1]", segments.get(2).toString());
  }

  @Test
  public void nameGlob() {
    FieldPath.NameSegment segment = (FieldPath.NameSegment) FieldPath.of("IS_*").segments().get(0);
    assertTrue(segment.matches("IS_DIGITAL_GOOD"));
    assertTrue(segment.matches("IS_"));
    assertFalse(segment.matches("ITEM_KEY"));
    assertFalse(segment.matches("is_digital_good"));
  }

  @Test
  public void nameGlobIsAnchored() {
    FieldPath.NameSegment segment = (FieldPath.NameSegment) FieldPath.of("*_CODE").segments().get(0);
    assertTrue(segment.matches("CLASS_CODE"));
    assertFalse(segment.matches("CLASS_CODE_X"));
  }

  @Test
  public void nameGlobEscapesRegexCharacters() {
    FieldPath.NameSegment segment = (FieldPath.NameSegment) FieldPath.of("a+b*").segments().get(0);
    assertTrue(segment.matches("a+bc"));
    assertFalse(segment.matches("aab"));
  }

  @Test
  public void singleCharacterGlob() {
    FieldPath.NameSegment segment = (FieldPath.NameSegment) FieldPath.of("A?C").segments().get(0);
    assertTrue(segment.matches("ABC"));
    assertFalse(segment.matches("AC"));
  }

  @Test
  public void exactNameDoesNotMatchSubstring() {
    FieldPath.NameSegment segment = (FieldPath.NameSegment) FieldPath.of("ITEM").segments().get(0);
    assertTrue(segment.matches("ITEM"));
    assertFalse(segment.matches("ITEM_KEY"));
    assertFalse(segment.matches(null));
  }

  @Test
  public void recursiveDescent() {
    List<FieldPath.Segment> segments = FieldPath.of("**.VINTAGE").segments();
    assertEquals(2, segments.size());
    assertTrue(segments.get(0) instanceof FieldPath.RecursiveSegment);
    assertEquals("VINTAGE", segments.get(1).toString());
  }

  @Test
  public void emptySpecTargetsTheValue() {
    assertTrue(FieldPath.of("").segments().isEmpty());
    assertTrue(FieldPath.of(null).segments().isEmpty());
    assertTrue(FieldPath.of("$").segments().isEmpty());
  }

  @Test
  public void unclosedBracket() {
    assertThrows(DataException.class, () -> FieldPath.of("RATINGS[0"));
  }

  @Test
  public void unexpectedBracket() {
    assertThrows(DataException.class, () -> FieldPath.of("RATINGS]"));
  }

  @Test
  public void invalidIndex() {
    assertThrows(DataException.class, () -> FieldPath.of("RATINGS[abc]"));
  }

  @Test
  public void negativeIndex() {
    assertThrows(DataException.class, () -> FieldPath.of("RATINGS[-1]"));
  }

  @Test
  public void recursiveSegmentMustStandAlone() {
    assertThrows(DataException.class, () -> FieldPath.of("A**B"));
  }

  @Test
  public void pathWithoutFields() {
    assertThrows(DataException.class, () -> FieldPath.of("..."));
  }
}
