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

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CastTest {

  static SinkRecord record(Schema schema, Object value) {
    return new SinkRecord("test", 1, null, null, schema, value, 1234L);
  }

  static Cast.Value<SinkRecord> valueTransform(String spec) {
    Cast.Value<SinkRecord> transform = new Cast.Value<>();
    transform.configure(ImmutableMap.of(CastConfig.SPEC_CONFIG, spec));
    return transform;
  }

  static Cast.Value<SinkRecord> valueTransform(String spec, boolean skipMissingOrNull) {
    Cast.Value<SinkRecord> transform = new Cast.Value<>();
    transform.configure(
        ImmutableMap.of(
            CastConfig.SPEC_CONFIG, spec,
            CastConfig.SKIP_MISSING_OR_NULL_CONFIG, skipMissingOrNull
        )
    );
    return transform;
  }

  /**
   * The sample product document from the transformation documentation.
   */
  static Map<String, Object> schemalessProduct() {
    Map<String, Object> product = new LinkedHashMap<>();
    product.put("PRODUCT_KEY", "158425");

    List<Object> ratings = new ArrayList<>();
    ratings.add(rating("6695543", "93", "2024"));
    ratings.add(rating("6695544", "91", "2022"));

    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("IS_DIGITAL_GOOD", "0");

    Map<String, Object> item = new LinkedHashMap<>();
    item.put("ITEM_KEY", "1223750");
    item.put("VINTAGE_DEFINED", "0");
    item.put("CONTAINER_SIZE", "750ml");
    item.put("ATTRIBUTES", attributes);

    Map<String, Object> hierarchy = new LinkedHashMap<>();
    hierarchy.put("DEPARTMENT", "Wine");
    hierarchy.put("DEPARTMENT_CODE", "20");
    hierarchy.put("CLASS_CODE", "541.01");

    Map<String, Object> value = new LinkedHashMap<>();
    value.put("PRODUCT", product);
    value.put("RATINGS", ratings);
    value.put("ITEM", item);
    value.put("ITEM_HIERARCHY", hierarchy);
    return value;
  }

  static Map<String, Object> rating(String key, String text, String vintage) {
    Map<String, Object> rating = new LinkedHashMap<>();
    rating.put("RATING_KEY", key);
    rating.put("RATING_TEXT", text);
    rating.put("RATING_TYPE", "RATING");
    rating.put("VINTAGE", vintage);
    return rating;
  }

  @Test
  public void schemalessDeeplyNestedStringToBoolean() {
    SinkRecord actual = valueTransform("ITEM.ATTRIBUTES.IS_DIGITAL_GOOD:boolean")
        .apply(record(null, schemalessProduct()));

    Map<?, ?> value = (Map<?, ?>) actual.value();
    Map<?, ?> attributes = (Map<?, ?>) ((Map<?, ?>) value.get("ITEM")).get("ATTRIBUTES");
    assertEquals(Boolean.FALSE, attributes.get("IS_DIGITAL_GOOD"));
    assertNull(actual.valueSchema());
    assertEquals("1223750", ((Map<?, ?>) value.get("ITEM")).get("ITEM_KEY"), "sibling fields are untouched");
  }

  @Test
  public void schemalessAllArrayElements() {
    SinkRecord actual = valueTransform("RATINGS[*].VINTAGE:int32")
        .apply(record(null, schemalessProduct()));

    List<?> ratings = (List<?>) ((Map<?, ?>) actual.value()).get("RATINGS");
    assertEquals(2024, ((Map<?, ?>) ratings.get(0)).get("VINTAGE"));
    assertEquals(2022, ((Map<?, ?>) ratings.get(1)).get("VINTAGE"));
    assertEquals("93", ((Map<?, ?>) ratings.get(0)).get("RATING_TEXT"), "sibling fields are untouched");
  }

  @Test
  public void schemalessSingleArrayElement() {
    SinkRecord actual = valueTransform("RATINGS[1].VINTAGE:int32")
        .apply(record(null, schemalessProduct()));

    List<?> ratings = (List<?>) ((Map<?, ?>) actual.value()).get("RATINGS");
    assertEquals("2024", ((Map<?, ?>) ratings.get(0)).get("VINTAGE"));
    assertEquals(2022, ((Map<?, ?>) ratings.get(1)).get("VINTAGE"));
  }

  @Test
  public void schemalessRecursiveGlob() {
    SinkRecord actual = valueTransform("**.IS_*:boolean").apply(record(null, schemalessProduct()));

    Map<?, ?> value = (Map<?, ?>) actual.value();
    Map<?, ?> attributes = (Map<?, ?>) ((Map<?, ?>) value.get("ITEM")).get("ATTRIBUTES");
    assertEquals(Boolean.FALSE, attributes.get("IS_DIGITAL_GOOD"));
  }

  @Test
  public void schemalessMultipleSpecs() {
    SinkRecord actual = valueTransform(
        "ITEM.ATTRIBUTES.IS_DIGITAL_GOOD:boolean,RATINGS[*].RATING_TEXT:int32,ITEM_HIERARCHY.CLASS_CODE:float64"
    ).apply(record(null, schemalessProduct()));

    Map<?, ?> value = (Map<?, ?>) actual.value();
    assertEquals(
        Boolean.FALSE,
        ((Map<?, ?>) ((Map<?, ?>) value.get("ITEM")).get("ATTRIBUTES")).get("IS_DIGITAL_GOOD")
    );
    List<?> ratings = (List<?>) value.get("RATINGS");
    assertEquals(93, ((Map<?, ?>) ratings.get(0)).get("RATING_TEXT"));
    assertEquals(91, ((Map<?, ?>) ratings.get(1)).get("RATING_TEXT"));
    assertEquals(541.01D, ((Map<?, ?>) value.get("ITEM_HIERARCHY")).get("CLASS_CODE"));
  }

  @Test
  public void schemalessMissingFieldIsSkipped() {
    Map<String, Object> input = schemalessProduct();
    SinkRecord actual = valueTransform("ITEM.NOT_THERE:int32", true).apply(record(null, input));
    assertSame(input, actual.value());
  }

  @Test
  public void schemalessMissingFieldFails() {
    assertThrows(
        DataException.class,
        () -> valueTransform("ITEM.NOT_THERE:int32", false).apply(record(null, schemalessProduct()))
    );
  }

  @Test
  public void schemalessNullFieldIsSkipped() {
    Map<String, Object> input = schemalessProduct();
    ((Map<String, Object>) input.get("ITEM")).put("VINTAGE_DEFINED", null);

    SinkRecord actual = valueTransform("ITEM.VINTAGE_DEFINED:boolean", true).apply(record(null, input));

    assertNull(((Map<?, ?>) ((Map<?, ?>) actual.value()).get("ITEM")).get("VINTAGE_DEFINED"));
  }

  @Test
  public void schemalessNullFieldFails() {
    Map<String, Object> input = schemalessProduct();
    ((Map<String, Object>) input.get("ITEM")).put("VINTAGE_DEFINED", null);

    assertThrows(
        DataException.class,
        () -> valueTransform("ITEM.VINTAGE_DEFINED:boolean", false).apply(record(null, input))
    );
  }

  @Test
  public void nullValueIsSkipped() {
    SinkRecord input = record(null, null);
    assertSame(input, valueTransform("ITEM:int32", true).apply(input));
  }

  @Test
  public void nullValueFails() {
    assertThrows(
        DataException.class,
        () -> valueTransform("ITEM:int32", false).apply(record(null, null))
    );
  }

  @Test
  public void wholeValueCast() {
    SinkRecord actual = valueTransform("int64").apply(record(Schema.STRING_SCHEMA, "1234"));
    assertEquals(1234L, actual.value());
    assertEquals(Schema.INT64_SCHEMA, actual.valueSchema());
  }

  @Test
  public void wholeKeyCast() {
    Cast.Key<SinkRecord> transform = new Cast.Key<>();
    transform.configure(ImmutableMap.of(CastConfig.SPEC_CONFIG, "boolean"));

    SinkRecord input = new SinkRecord("test", 1, Schema.STRING_SCHEMA, "1", null, null, 1234L);
    SinkRecord actual = transform.apply(input);

    assertEquals(Boolean.TRUE, actual.key());
    assertEquals(Schema.BOOLEAN_SCHEMA, actual.keySchema());
  }

  static Schema ratingSchema() {
    return SchemaBuilder.struct()
        .name("com.example.Rating")
        .field("RATING_KEY", Schema.STRING_SCHEMA)
        .field("VINTAGE", Schema.STRING_SCHEMA)
        .build();
  }

  static Schema productSchema() {
    Schema attributesSchema = SchemaBuilder.struct()
        .name("com.example.Attributes")
        .field("IS_DIGITAL_GOOD", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Schema itemSchema = SchemaBuilder.struct()
        .name("com.example.Item")
        .field("ITEM_KEY", Schema.STRING_SCHEMA)
        .field("ATTRIBUTES", attributesSchema)
        .build();
    return SchemaBuilder.struct()
        .name("com.example.Product")
        .field("ITEM", itemSchema)
        .field("RATINGS", SchemaBuilder.array(ratingSchema()).build())
        .build();
  }

  static Struct product() {
    Schema schema = productSchema();
    Schema itemSchema = schema.field("ITEM").schema();
    Schema attributesSchema = itemSchema.field("ATTRIBUTES").schema();
    Schema ratingSchema = schema.field("RATINGS").schema().valueSchema();

    Struct attributes = new Struct(attributesSchema).put("IS_DIGITAL_GOOD", "0");
    Struct item = new Struct(itemSchema).put("ITEM_KEY", "1223750").put("ATTRIBUTES", attributes);
    List<Struct> ratings = Arrays.asList(
        new Struct(ratingSchema).put("RATING_KEY", "6695543").put("VINTAGE", "2024"),
        new Struct(ratingSchema).put("RATING_KEY", "6695544").put("VINTAGE", "2022")
    );
    return new Struct(schema).put("ITEM", item).put("RATINGS", ratings);
  }

  @Test
  public void structDeeplyNestedStringToBoolean() {
    SinkRecord actual = valueTransform("ITEM.ATTRIBUTES.IS_DIGITAL_GOOD:boolean")
        .apply(record(productSchema(), product()));

    Schema attributesSchema = actual.valueSchema().field("ITEM").schema().field("ATTRIBUTES").schema();
    assertEquals(Schema.OPTIONAL_BOOLEAN_SCHEMA, attributesSchema.field("IS_DIGITAL_GOOD").schema());
    assertEquals("com.example.Attributes", attributesSchema.name(), "schema metadata is preserved");

    Struct value = (Struct) actual.value();
    Struct attributes = (Struct) ((Struct) value.get("ITEM")).get("ATTRIBUTES");
    assertEquals(Boolean.FALSE, attributes.get("IS_DIGITAL_GOOD"));
    assertEquals("1223750", ((Struct) value.get("ITEM")).get("ITEM_KEY"));
  }

  @Test
  public void structArrayElements() {
    SinkRecord actual = valueTransform("RATINGS[*].VINTAGE:int32")
        .apply(record(productSchema(), product()));

    Schema ratingsSchema = actual.valueSchema().field("RATINGS").schema();
    assertEquals(Schema.Type.ARRAY, ratingsSchema.type());
    assertEquals(Schema.INT32_SCHEMA, ratingsSchema.valueSchema().field("VINTAGE").schema());
    assertEquals("com.example.Rating", ratingsSchema.valueSchema().name());

    List<?> ratings = (List<?>) ((Struct) actual.value()).get("RATINGS");
    assertEquals(2024, ((Struct) ratings.get(0)).get("VINTAGE"));
    assertEquals(2022, ((Struct) ratings.get(1)).get("VINTAGE"));
  }

  @Test
  public void structArraySingleIndexIsRejected() {
    DataException exception = assertThrows(
        DataException.class,
        () -> valueTransform("RATINGS[1].VINTAGE:int32").apply(record(productSchema(), product()))
    );
    assertTrue(exception.getMessage().contains("RATINGS[1].VINTAGE"));
  }

  @Test
  public void structNothingToDoReturnsTheSameRecord() {
    SinkRecord input = record(productSchema(), product());
    assertSame(input, valueTransform("ITEM.ATTRIBUTES.IS_DIGITAL_GOOD:string").apply(input));
  }

  @Test
  public void structOptionalityIsPreserved() {
    Schema inputSchema = SchemaBuilder.struct()
        .field("required", Schema.STRING_SCHEMA)
        .field("optional", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Struct input = new Struct(inputSchema).put("required", "1").put("optional", "1");

    SinkRecord actual = valueTransform("*:int32").apply(record(inputSchema, input));

    assertEquals(Schema.INT32_SCHEMA, actual.valueSchema().field("required").schema());
    assertEquals(Schema.OPTIONAL_INT32_SCHEMA, actual.valueSchema().field("optional").schema());
  }

  @Test
  public void structLogicalTypesAreCastFromTheirPrimitiveForm() {
    Schema inputSchema = SchemaBuilder.struct()
        .field("timestamp", Timestamp.SCHEMA)
        .field("decimal", Decimal.schema(2))
        .build();
    Struct input = new Struct(inputSchema)
        .put("timestamp", new Date(1743984000000L))
        .put("decimal", new BigDecimal("12.35"));

    SinkRecord actual = valueTransform("timestamp:int64,decimal:float64").apply(record(inputSchema, input));

    assertEquals(Schema.INT64_SCHEMA, actual.valueSchema().field("timestamp").schema());
    assertEquals(1743984000000L, ((Struct) actual.value()).get("timestamp"));
    assertEquals(12.35D, ((Struct) actual.value()).get("decimal"));
  }

  @Test
  public void structDecimalToString() {
    Schema inputSchema = SchemaBuilder.struct()
        .field("decimal", Decimal.schema(4))
        .build();
    Struct input = new Struct(inputSchema).put("decimal", new BigDecimal("0.0001"));

    SinkRecord actual = valueTransform("decimal:string").apply(record(inputSchema, input));

    assertEquals("0.0001", ((Struct) actual.value()).get("decimal"));
  }

  @Test
  public void booleanTextForms() {
    assertTrue(Cast.toBoolean("1"));
    assertTrue(Cast.toBoolean("TRUE"));
    assertTrue(Cast.toBoolean(" yes "));
    assertTrue(Cast.toBoolean("Y"));
    assertTrue(Cast.toBoolean("on"));
    assertFalse(Cast.toBoolean("0"));
    assertFalse(Cast.toBoolean("false"));
    assertFalse(Cast.toBoolean("N"));
    assertFalse(Cast.toBoolean("off"));
    assertTrue(Cast.toBoolean("2"), "any non zero number is true");
    assertFalse(Cast.toBoolean("0.0"));
    assertTrue(Cast.toBoolean(3));
    assertFalse(Cast.toBoolean(0));
  }

  @Test
  public void booleanTextThatIsNotABoolean() {
    assertThrows(DataException.class, () -> Cast.toBoolean("maybe"));
  }

  @Test
  public void numericNarrowing() {
    assertEquals((byte) 1, Cast.convert(Schema.Type.INT8, null, "1"));
    assertEquals((short) 1234, Cast.convert(Schema.Type.INT16, null, "1234"));
    assertEquals(1, Cast.convert(Schema.Type.INT32, null, "1.9"), "decimal strings are truncated");
    assertEquals(1L, Cast.convert(Schema.Type.INT64, null, true));
    assertEquals(1.5F, Cast.convert(Schema.Type.FLOAT32, null, "1.5"));
    assertEquals(1.5D, Cast.convert(Schema.Type.FLOAT64, null, "1.5"));
    assertEquals("1.5", Cast.convert(Schema.Type.STRING, null, 1.5D));
  }

  @Test
  public void stringThatIsNotANumber() {
    assertThrows(DataException.class, () -> Cast.convert(Schema.Type.INT32, null, "abc"));
    assertThrows(DataException.class, () -> Cast.convert(Schema.Type.FLOAT64, null, "abc"));
  }

  @Test
  public void invalidSpecType() {
    assertThrows(
        ConfigException.class,
        () -> new Cast.Value<SinkRecord>().configure(ImmutableMap.of(CastConfig.SPEC_CONFIG, "field:widget"))
    );
  }

  @Test
  public void invalidSpecPath() {
    assertThrows(
        ConfigException.class,
        () -> new Cast.Value<SinkRecord>().configure(ImmutableMap.of(CastConfig.SPEC_CONFIG, "field[:int32"))
    );
  }

  @Test
  public void skipMissingOrNullDefaultsToTrue() {
    CastConfig config = new CastConfig(ImmutableMap.of(CastConfig.SPEC_CONFIG, "field:int32"));
    assertTrue(config.skipMissingOrNull);
  }
}
