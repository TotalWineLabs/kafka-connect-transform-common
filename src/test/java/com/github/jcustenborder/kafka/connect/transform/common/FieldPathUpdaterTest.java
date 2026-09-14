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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FieldPathUpdaterTest {
  static final FieldPathUpdater.FieldTransform TO_STRING =
      (schema, value) -> Cast.cast(Schema.Type.STRING, schema, value);

  static List<FieldPath> path(String spec) {
    return Collections.singletonList(FieldPath.of(spec));
  }

  @Test
  public void schemalessMapIsRebuiltWithoutMutatingTheInput() {
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("VINTAGE", 2024);
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("ITEM", inner);

    SchemaAndValue actual = FieldPathUpdater.update(null, input, path("ITEM.VINTAGE"), TO_STRING, true);

    assertNull(actual.schema());
    Map<?, ?> actualItem = (Map<?, ?>) ((Map<?, ?>) actual.value()).get("ITEM");
    assertEquals("2024", actualItem.get("VINTAGE"));
    assertEquals(2024, inner.get("VINTAGE"), "the input map should not be modified");
  }

  @Test
  public void unmatchedPathReturnsTheInputUntouched() {
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("ITEM", "value");

    SchemaAndValue actual = FieldPathUpdater.update(null, input, path("MISSING.FIELD"), TO_STRING, true);

    assertSame(input, actual.value());
  }

  @Test
  public void unmatchedPathFailsWhenNotSkipping() {
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("ITEM", "value");

    assertThrows(
        DataException.class,
        () -> FieldPathUpdater.update(null, input, path("MISSING.FIELD"), TO_STRING, false)
    );
  }

  @Test
  public void nullFieldFailsWhenNotSkipping() {
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("ITEM", null);

    assertThrows(
        DataException.class,
        () -> FieldPathUpdater.update(null, input, path("ITEM"), TO_STRING, false)
    );
  }

  @Test
  public void nullFieldIsLeftAloneWhenSkipping() {
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("ITEM", null);

    SchemaAndValue actual = FieldPathUpdater.update(null, input, path("ITEM"), TO_STRING, true);

    assertSame(input, actual.value());
  }

  @Test
  public void nullStructFieldStillPicksUpTheNewSchema() {
    Schema inputSchema = SchemaBuilder.struct()
        .field("VINTAGE", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    Struct input = new Struct(inputSchema).put("VINTAGE", null);

    SchemaAndValue actual = FieldPathUpdater.update(inputSchema, input, path("VINTAGE"), TO_STRING, true);

    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, actual.schema().field("VINTAGE").schema());
    assertNull(((Struct) actual.value()).get("VINTAGE"));
  }

  @Test
  public void schemafulMapValuesAreUpdatedTogether() {
    Schema inputSchema = SchemaBuilder.struct()
        .field("COUNTS", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
        .build();
    Map<String, Integer> counts = new LinkedHashMap<>();
    counts.put("a", 1);
    counts.put("b", 2);
    Struct input = new Struct(inputSchema).put("COUNTS", counts);

    SchemaAndValue actual = FieldPathUpdater.update(inputSchema, input, path("COUNTS.*"), TO_STRING, true);

    Schema actualMapSchema = actual.schema().field("COUNTS").schema();
    assertEquals(Schema.Type.MAP, actualMapSchema.type());
    assertEquals(Schema.STRING_SCHEMA, actualMapSchema.valueSchema());
    Map<?, ?> actualCounts = (Map<?, ?>) ((Struct) actual.value()).get("COUNTS");
    assertEquals("1", actualCounts.get("a"));
    assertEquals("2", actualCounts.get("b"));
  }

  @Test
  public void schemafulMapWithAPartialUpdateIsRejected() {
    Schema inputSchema = SchemaBuilder.struct()
        .field("COUNTS", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
        .build();
    Map<String, Integer> counts = new LinkedHashMap<>();
    counts.put("a", 1);
    counts.put("b", 2);
    Struct input = new Struct(inputSchema).put("COUNTS", counts);

    assertThrows(
        DataException.class,
        () -> FieldPathUpdater.update(inputSchema, input, path("COUNTS.a"), TO_STRING, true)
    );
  }

  @Test
  public void schemafulArrayWithAnIndexSpecificSchemaChangeIsRejected() {
    Schema elementSchema = SchemaBuilder.struct()
        .field("VINTAGE", Schema.INT32_SCHEMA)
        .build();
    Schema inputSchema = SchemaBuilder.struct()
        .field("RATINGS", SchemaBuilder.array(elementSchema).build())
        .build();
    List<Struct> ratings = Arrays.asList(
        new Struct(elementSchema).put("VINTAGE", 2024),
        new Struct(elementSchema).put("VINTAGE", 2022)
    );
    Struct input = new Struct(inputSchema).put("RATINGS", ratings);

    DataException exception = assertThrows(
        DataException.class,
        () -> FieldPathUpdater.update(inputSchema, input, path("RATINGS[1].VINTAGE"), TO_STRING, true)
    );
    assertEquals(true, exception.getMessage().contains("RATINGS[1].VINTAGE"));
  }

  @Test
  public void schemalessArrayWithAnIndexSpecificUpdateIsAllowed() {
    List<Object> ratings = new ArrayList<>();
    Map<String, Object> first = new LinkedHashMap<>();
    first.put("VINTAGE", 2024);
    Map<String, Object> second = new LinkedHashMap<>();
    second.put("VINTAGE", 2022);
    ratings.add(first);
    ratings.add(second);
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("RATINGS", ratings);

    SchemaAndValue actual = FieldPathUpdater.update(
        null, input, path("RATINGS[1].VINTAGE"), TO_STRING, true
    );

    List<?> actualRatings = (List<?>) ((Map<?, ?>) actual.value()).get("RATINGS");
    assertEquals(2024, ((Map<?, ?>) actualRatings.get(0)).get("VINTAGE"));
    assertEquals("2022", ((Map<?, ?>) actualRatings.get(1)).get("VINTAGE"));
  }

  @Test
  public void indexBeyondTheEndOfTheArrayMatchesNothing() {
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("RATINGS", new ArrayList<>(Collections.singletonList(1)));

    assertThrows(
        DataException.class,
        () -> FieldPathUpdater.update(null, input, path("RATINGS[5]"), TO_STRING, false)
    );
  }

  @Test
  public void emptyPathTargetsTheValueItself() {
    SchemaAndValue actual = FieldPathUpdater.update(
        Schema.INT32_SCHEMA, 42, path(""), TO_STRING, true
    );

    assertEquals(Schema.STRING_SCHEMA, actual.schema());
    assertEquals("42", actual.value());
  }

  @Test
  public void structSchemaMetadataIsPreserved() {
    Schema inputSchema = SchemaBuilder.struct()
        .name("com.example.Item")
        .doc("An item.")
        .version(3)
        .parameter("owner", "catalog")
        .field("VINTAGE", Schema.INT32_SCHEMA)
        .build();
    Struct input = new Struct(inputSchema).put("VINTAGE", 2024);

    SchemaAndValue actual = FieldPathUpdater.update(inputSchema, input, path("VINTAGE"), TO_STRING, true);

    assertEquals("com.example.Item", actual.schema().name());
    assertEquals("An item.", actual.schema().doc());
    assertEquals(Integer.valueOf(3), actual.schema().version());
    assertEquals("catalog", actual.schema().parameters().get("owner"));
  }
}
