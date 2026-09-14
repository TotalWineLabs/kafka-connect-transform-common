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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RoundDecimalTest {

  static SinkRecord record(Schema schema, Object value) {
    return new SinkRecord("test", 1, null, null, schema, value, 1234L);
  }

  static RoundDecimal.Value<SinkRecord> transform(Map<String, Object> settings) {
    RoundDecimal.Value<SinkRecord> transform = new RoundDecimal.Value<>();
    transform.configure(settings);
    return transform;
  }

  static RoundDecimal.Value<SinkRecord> transform(String fields, int scale) {
    return transform(
        ImmutableMap.of(
            RoundDecimalConfig.FIELDS_CONFIG, fields,
            RoundDecimalConfig.SCALE_CONFIG, scale
        )
    );
  }

  static Map<String, Object> schemalessOrder() {
    Map<String, Object> item = new LinkedHashMap<>();
    item.put("PRICE", new BigDecimal("12.3456"));
    item.put("WEIGHT", 1.23456D);
    item.put("QUANTITY", 3);
    item.put("TAX_RATE", "0.06255");

    List<Object> lines = new ArrayList<>();
    lines.add(line("10.005"));
    lines.add(line("20.994"));

    Map<String, Object> order = new LinkedHashMap<>();
    order.put("ITEM", item);
    order.put("LINES", lines);
    return order;
  }

  static Map<String, Object> line(String amount) {
    Map<String, Object> line = new LinkedHashMap<>();
    line.put("AMOUNT", new BigDecimal(amount));
    return line;
  }

  @Test
  public void schemalessNestedDecimal() {
    SinkRecord actual = transform("ITEM.PRICE", 2).apply(record(null, schemalessOrder()));

    Map<?, ?> item = (Map<?, ?>) ((Map<?, ?>) actual.value()).get("ITEM");
    assertEquals(new BigDecimal("12.35"), item.get("PRICE"));
    assertEquals(1.23456D, item.get("WEIGHT"), "sibling fields are untouched");
  }

  @Test
  public void schemalessArrayElements() {
    SinkRecord actual = transform("LINES[*].AMOUNT", 2).apply(record(null, schemalessOrder()));

    List<?> lines = (List<?>) ((Map<?, ?>) actual.value()).get("LINES");
    assertEquals(new BigDecimal("10.01"), ((Map<?, ?>) lines.get(0)).get("AMOUNT"));
    assertEquals(new BigDecimal("20.99"), ((Map<?, ?>) lines.get(1)).get("AMOUNT"));
  }

  @Test
  public void schemalessSingleArrayElement() {
    SinkRecord actual = transform("LINES[1].AMOUNT", 2).apply(record(null, schemalessOrder()));

    List<?> lines = (List<?>) ((Map<?, ?>) actual.value()).get("LINES");
    assertEquals(new BigDecimal("10.005"), ((Map<?, ?>) lines.get(0)).get("AMOUNT"));
    assertEquals(new BigDecimal("20.99"), ((Map<?, ?>) lines.get(1)).get("AMOUNT"));
  }

  @Test
  public void schemalessDoubleKeepsItsType() {
    SinkRecord actual = transform("ITEM.WEIGHT", 3).apply(record(null, schemalessOrder()));

    Map<?, ?> item = (Map<?, ?>) ((Map<?, ?>) actual.value()).get("ITEM");
    assertEquals(1.235D, item.get("WEIGHT"));
  }

  @Test
  public void schemalessNumericStringKeepsItsType() {
    SinkRecord actual = transform("ITEM.TAX_RATE", 3).apply(record(null, schemalessOrder()));

    Map<?, ?> item = (Map<?, ?>) ((Map<?, ?>) actual.value()).get("ITEM");
    assertEquals("0.063", item.get("TAX_RATE"));
  }

  @Test
  public void integersAreLeftAlone() {
    SinkRecord input = record(null, schemalessOrder());
    assertSame(input, transform("ITEM.QUANTITY", 2).apply(input));
  }

  @Test
  public void multipleFields() {
    SinkRecord actual = transform("ITEM.PRICE,LINES[*].AMOUNT", 1).apply(record(null, schemalessOrder()));

    Map<?, ?> value = (Map<?, ?>) actual.value();
    assertEquals(new BigDecimal("12.3"), ((Map<?, ?>) value.get("ITEM")).get("PRICE"));
    List<?> lines = (List<?>) value.get("LINES");
    assertEquals(new BigDecimal("10.0"), ((Map<?, ?>) lines.get(0)).get("AMOUNT"));
  }

  @Test
  public void recursiveGlob() {
    SinkRecord actual = transform("**.AMOUNT", 1).apply(record(null, schemalessOrder()));

    List<?> lines = (List<?>) ((Map<?, ?>) actual.value()).get("LINES");
    assertEquals(new BigDecimal("10.0"), ((Map<?, ?>) lines.get(0)).get("AMOUNT"));
    assertEquals(new BigDecimal("21.0"), ((Map<?, ?>) lines.get(1)).get("AMOUNT"));
  }

  @Test
  public void roundingMode() {
    SinkRecord actual = transform(
        ImmutableMap.of(
            RoundDecimalConfig.FIELDS_CONFIG, "ITEM.PRICE",
            RoundDecimalConfig.SCALE_CONFIG, 2,
            RoundDecimalConfig.ROUNDING_MODE_CONFIG, "FLOOR"
        )
    ).apply(record(null, schemalessOrder()));

    Map<?, ?> item = (Map<?, ?>) ((Map<?, ?>) actual.value()).get("ITEM");
    assertEquals(new BigDecimal("12.34"), item.get("PRICE"));
  }

  @Test
  public void missingFieldIsSkipped() {
    SinkRecord input = record(null, schemalessOrder());
    assertSame(input, transform("ITEM.NOT_THERE", 2).apply(input));
  }

  @Test
  public void missingFieldFails() {
    assertThrows(
        DataException.class,
        () -> transform(
            ImmutableMap.of(
                RoundDecimalConfig.FIELDS_CONFIG, "ITEM.NOT_THERE",
                RoundDecimalConfig.SCALE_CONFIG, 2,
                RoundDecimalConfig.SKIP_MISSING_OR_NULL_CONFIG, false
            )
        ).apply(record(null, schemalessOrder()))
    );
  }

  @Test
  public void nullFieldIsSkipped() {
    Map<String, Object> input = schemalessOrder();
    ((Map<String, Object>) input.get("ITEM")).put("PRICE", null);

    SinkRecord actual = transform("ITEM.PRICE", 2).apply(record(null, input));

    assertNull(((Map<?, ?>) ((Map<?, ?>) actual.value()).get("ITEM")).get("PRICE"));
  }

  @Test
  public void nullFieldFails() {
    Map<String, Object> input = schemalessOrder();
    ((Map<String, Object>) input.get("ITEM")).put("PRICE", null);

    assertThrows(
        DataException.class,
        () -> transform(
            ImmutableMap.of(
                RoundDecimalConfig.FIELDS_CONFIG, "ITEM.PRICE",
                RoundDecimalConfig.SCALE_CONFIG, 2,
                RoundDecimalConfig.SKIP_MISSING_OR_NULL_CONFIG, false
            )
        ).apply(record(null, input))
    );
  }

  @Test
  public void nonNumericFieldFails() {
    Map<String, Object> input = schemalessOrder();
    ((Map<String, Object>) input.get("ITEM")).put("PRICE", "not a number");

    assertThrows(DataException.class, () -> transform("ITEM.PRICE", 2).apply(record(null, input)));
  }

  static Schema lineSchema() {
    return SchemaBuilder.struct()
        .name("com.example.Line")
        .field("AMOUNT", Decimal.builder(4).parameter("connect.decimal.precision", "38").build())
        .build();
  }

  static Schema orderSchema() {
    Schema itemSchema = SchemaBuilder.struct()
        .name("com.example.Item")
        .field("PRICE", Decimal.builder(4).optional().build())
        .field("WEIGHT", Schema.FLOAT64_SCHEMA)
        .build();
    return SchemaBuilder.struct()
        .name("com.example.Order")
        .field("ITEM", itemSchema)
        .field("LINES", SchemaBuilder.array(lineSchema()).build())
        .build();
  }

  static Struct order() {
    Schema schema = orderSchema();
    Schema itemSchema = schema.field("ITEM").schema();
    Schema lineSchema = schema.field("LINES").schema().valueSchema();

    Struct item = new Struct(itemSchema)
        .put("PRICE", new BigDecimal("12.3456"))
        .put("WEIGHT", 1.23456D);
    List<Struct> lines = Arrays.asList(
        new Struct(lineSchema).put("AMOUNT", new BigDecimal("10.0050")),
        new Struct(lineSchema).put("AMOUNT", new BigDecimal("20.9940"))
    );
    return new Struct(schema).put("ITEM", item).put("LINES", lines);
  }

  @Test
  public void structNestedDecimalUpdatesTheSchema() {
    SinkRecord actual = transform("ITEM.PRICE", 2).apply(record(orderSchema(), order()));

    Schema priceSchema = actual.valueSchema().field("ITEM").schema().field("PRICE").schema();
    assertEquals(Decimal.LOGICAL_NAME, priceSchema.name());
    assertEquals("2", priceSchema.parameters().get(Decimal.SCALE_FIELD));
    assertTrue(priceSchema.isOptional(), "optionality is preserved");
    assertEquals("com.example.Item", actual.valueSchema().field("ITEM").schema().name());

    Struct item = (Struct) ((Struct) actual.value()).get("ITEM");
    assertEquals(new BigDecimal("12.35"), item.get("PRICE"));
    assertEquals(1.23456D, item.get("WEIGHT"));
  }

  @Test
  public void structArrayElementsUpdateTheElementSchema() {
    SinkRecord actual = transform("LINES[*].AMOUNT", 2).apply(record(orderSchema(), order()));

    Schema amountSchema = actual.valueSchema().field("LINES").schema().valueSchema().field("AMOUNT").schema();
    assertEquals("2", amountSchema.parameters().get(Decimal.SCALE_FIELD));
    assertEquals("38", amountSchema.parameters().get("connect.decimal.precision"),
        "existing parameters are preserved");

    List<?> lines = (List<?>) ((Struct) actual.value()).get("LINES");
    assertEquals(new BigDecimal("10.01"), ((Struct) lines.get(0)).get("AMOUNT"));
    assertEquals(new BigDecimal("20.99"), ((Struct) lines.get(1)).get("AMOUNT"));
  }

  @Test
  public void structArraySingleIndexIsRejected() {
    DataException exception = assertThrows(
        DataException.class,
        () -> transform("LINES[1].AMOUNT", 2).apply(record(orderSchema(), order()))
    );
    assertTrue(exception.getMessage().contains("LINES[1].AMOUNT"));
  }

  @Test
  public void structPrecisionIsWrittenToTheSchema() {
    SinkRecord actual = transform(
        ImmutableMap.of(
            RoundDecimalConfig.FIELDS_CONFIG, "ITEM.PRICE",
            RoundDecimalConfig.SCALE_CONFIG, 2,
            RoundDecimalConfig.PRECISION_CONFIG, 18
        )
    ).apply(record(orderSchema(), order()));

    Schema priceSchema = actual.valueSchema().field("ITEM").schema().field("PRICE").schema();
    assertEquals("18", priceSchema.parameters().get("connect.decimal.precision"));
  }

  @Test
  public void structFloatKeepsItsSchema() {
    SinkRecord actual = transform("ITEM.WEIGHT", 2).apply(record(orderSchema(), order()));

    assertEquals(
        Schema.FLOAT64_SCHEMA,
        actual.valueSchema().field("ITEM").schema().field("WEIGHT").schema()
    );
    assertEquals(1.23D, ((Struct) ((Struct) actual.value()).get("ITEM")).get("WEIGHT"));
  }

  @Test
  public void structNothingToDoReturnsTheSameRecord() {
    SinkRecord input = record(orderSchema(), order());
    assertSame(input, transform("ITEM.PRICE", 4).apply(input));
  }

  @Test
  public void keyIsSupported() {
    Schema keySchema = SchemaBuilder.struct().field("PRICE", Decimal.schema(4)).build();
    Struct key = new Struct(keySchema).put("PRICE", new BigDecimal("12.3456"));

    RoundDecimal.Key<SinkRecord> transform = new RoundDecimal.Key<>();
    transform.configure(
        ImmutableMap.of(
            RoundDecimalConfig.FIELDS_CONFIG, "PRICE",
            RoundDecimalConfig.SCALE_CONFIG, 2
        )
    );

    SinkRecord actual = transform.apply(new SinkRecord("test", 1, keySchema, key, null, null, 1234L));

    assertEquals(new BigDecimal("12.35"), ((Struct) actual.key()).get("PRICE"));
    assertEquals("2", actual.keySchema().field("PRICE").schema().parameters().get(Decimal.SCALE_FIELD));
  }

  @Test
  public void invalidFieldPath() {
    assertThrows(
        ConfigException.class,
        () -> transform(
            ImmutableMap.of(
                RoundDecimalConfig.FIELDS_CONFIG, "ITEM[",
                RoundDecimalConfig.SCALE_CONFIG, 2
            )
        )
    );
  }

  @Test
  public void invalidRoundingMode() {
    assertThrows(
        ConfigException.class,
        () -> transform(
            ImmutableMap.of(
                RoundDecimalConfig.FIELDS_CONFIG, "ITEM.PRICE",
                RoundDecimalConfig.SCALE_CONFIG, 2,
                RoundDecimalConfig.ROUNDING_MODE_CONFIG, "NOPE"
            )
        )
    );
  }

  @Test
  public void skipMissingOrNullDefaultsToTrue() {
    RoundDecimalConfig config = new RoundDecimalConfig(
        ImmutableMap.of(
            RoundDecimalConfig.FIELDS_CONFIG, "ITEM.PRICE",
            RoundDecimalConfig.SCALE_CONFIG, 2
        )
    );
    assertTrue(config.skipMissingOrNull);
    assertEquals(java.math.RoundingMode.HALF_UP, config.roundingMode);
  }
}
