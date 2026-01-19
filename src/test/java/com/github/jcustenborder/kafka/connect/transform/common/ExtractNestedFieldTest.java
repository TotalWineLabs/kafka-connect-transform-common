/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExtractNestedFieldTest {
  private ExtractNestedField.Value<SinkRecord> transformation;

  @BeforeEach
  public void before() {
    this.transformation = new ExtractNestedField.Value<>();
  }

  @Test
  public void test() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "state",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "address",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "state"
        )
    );

    final Schema innerSchema = SchemaBuilder.struct()
        .name("Address")
        .field("city", Schema.STRING_SCHEMA)
        .field("state", Schema.STRING_SCHEMA)
        .build();
    final Schema inputSchema = SchemaBuilder.struct()
        .field("first_name", Schema.STRING_SCHEMA)
        .field("last_name", Schema.STRING_SCHEMA)
        .field("address", innerSchema)
        .build();
    final Schema expectedSchema = SchemaBuilder.struct()
        .field("first_name", Schema.STRING_SCHEMA)
        .field("last_name", Schema.STRING_SCHEMA)
        .field("address", innerSchema)
        .field("state", Schema.STRING_SCHEMA)
        .build();
    final Struct innerStruct = new Struct(innerSchema)
        .put("city", "Austin")
        .put("state", "tx");
    final Struct inputStruct = new Struct(inputSchema)
        .put("first_name", "test")
        .put("last_name", "developer")
        .put("address", innerStruct);
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("first_name", "test")
        .put("last_name", "developer")
        .put("address", innerStruct)
        .put("state", "tx");

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );
    for (int i = 0; i < 50; i++) {
      final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
      assertNotNull(transformedRecord, "transformedRecord should not be null.");
      assertSchema(expectedSchema, transformedRecord.valueSchema());
      assertStruct(expectedStruct, (Struct) transformedRecord.value());
    }
  }

  @Test
  public void testArrayExtractionFirstElement() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "0",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "items",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "firstItem"
        )
    );

    final Schema itemSchema = SchemaBuilder.struct()
        .name("Item")
        .field("name", Schema.STRING_SCHEMA)
        .field("price", Schema.FLOAT64_SCHEMA)
        .build();
    
    final Schema inputSchema = SchemaBuilder.struct()
        .field("orderId", Schema.STRING_SCHEMA)
        .field("items", SchemaBuilder.array(itemSchema).build())
        .build();
    
    final Schema expectedSchema = SchemaBuilder.struct()
        .field("orderId", Schema.STRING_SCHEMA)
        .field("items", SchemaBuilder.array(itemSchema).build())
        .field("firstItem", itemSchema)
        .build();

    final Struct item1 = new Struct(itemSchema)
        .put("name", "product1")
        .put("price", 19.99);
    final Struct item2 = new Struct(itemSchema)
        .put("name", "product2")
        .put("price", 29.99);

    final Struct inputStruct = new Struct(inputSchema)
        .put("orderId", "order123")
        .put("items", Arrays.asList(item1, item2));

    final Struct expectedStruct = new Struct(expectedSchema)
        .put("orderId", "order123")
        .put("items", Arrays.asList(item1, item2))
        .put("firstItem", item1);

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    assertSchema(expectedSchema, transformedRecord.valueSchema());
    assertStruct(expectedStruct, (Struct) transformedRecord.value());
  }

  @Test
  public void testArrayExtractionSecondElement() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "1",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "items",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "secondItem"
        )
    );

    final Schema itemSchema = SchemaBuilder.struct()
        .name("Item")
        .field("name", Schema.STRING_SCHEMA)
        .build();
    
    final Schema inputSchema = SchemaBuilder.struct()
        .field("items", SchemaBuilder.array(itemSchema).build())
        .build();

    final Struct item1 = new Struct(itemSchema).put("name", "product1");
    final Struct item2 = new Struct(itemSchema).put("name", "product2");
    final Struct item3 = new Struct(itemSchema).put("name", "product3");

    final Struct inputStruct = new Struct(inputSchema)
        .put("items", Arrays.asList(item1, item2, item3));

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    
    final Struct result = (Struct) transformedRecord.value();
    final Struct extractedItem = result.getStruct("secondItem");
    assertEquals("product2", extractedItem.getString("name"));
  }

  @Test
  public void testArrayExtractionOutOfBounds() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "5",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "items",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "item"
        )
    );

    final Schema itemSchema = SchemaBuilder.struct()
        .name("Item")
        .field("name", Schema.STRING_SCHEMA)
        .build();
    
    final Schema inputSchema = SchemaBuilder.struct()
        .field("items", SchemaBuilder.array(itemSchema).build())
        .build();

    final Struct item1 = new Struct(itemSchema).put("name", "product1");

    final Struct inputStruct = new Struct(inputSchema)
        .put("items", Arrays.asList(item1));

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );

    assertThrows(IllegalArgumentException.class, () -> {
      this.transformation.apply(inputRecord);
    });
  }

  @Test
  public void testArrayExtractionNegativeIndex() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "-1",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "items",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "item"
        )
    );

    final Schema itemSchema = SchemaBuilder.struct()
        .name("Item")
        .field("name", Schema.STRING_SCHEMA)
        .build();
    
    final Schema inputSchema = SchemaBuilder.struct()
        .field("items", SchemaBuilder.array(itemSchema).build())
        .build();

    final Struct item1 = new Struct(itemSchema).put("name", "product1");

    final Struct inputStruct = new Struct(inputSchema)
        .put("items", Arrays.asList(item1));

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );

    assertThrows(IllegalArgumentException.class, () -> {
      this.transformation.apply(inputRecord);
    });
  }

  @Test
  public void testArrayExtractionWithPrimitiveTypes() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "0",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "tags",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "firstTag"
        )
    );

    final Schema inputSchema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .build();
    
    final Schema expectedSchema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("firstTag", Schema.STRING_SCHEMA)
        .build();

    final Struct inputStruct = new Struct(inputSchema)
        .put("id", "item123")
        .put("tags", Arrays.asList("electronics", "sale", "new"));

    final Struct expectedStruct = new Struct(expectedSchema)
        .put("id", "item123")
        .put("tags", Arrays.asList("electronics", "sale", "new"))
        .put("firstTag", "electronics");

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    assertSchema(expectedSchema, transformedRecord.valueSchema());
    assertStruct(expectedStruct, (Struct) transformedRecord.value());
  }

  @Test
  public void testBackwardCompatibilityWithStructAccess() {
    // This test ensures the original struct-based behavior still works
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "zipCode",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "location",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "zip"
        )
    );

    final Schema locationSchema = SchemaBuilder.struct()
        .name("Location")
        .field("city", Schema.STRING_SCHEMA)
        .field("zipCode", Schema.STRING_SCHEMA)
        .build();
    
    final Schema inputSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("location", locationSchema)
        .build();
    
    final Schema expectedSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("location", locationSchema)
        .field("zip", Schema.STRING_SCHEMA)
        .build();

    final Struct locationStruct = new Struct(locationSchema)
        .put("city", "New York")
        .put("zipCode", "10001");
    
    final Struct inputStruct = new Struct(inputSchema)
        .put("name", "Office")
        .put("location", locationStruct);
    
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("name", "Office")
        .put("location", locationStruct)
        .put("zip", "10001");

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    assertSchema(expectedSchema, transformedRecord.valueSchema());
    assertStruct(expectedStruct, (Struct) transformedRecord.value());
  }

  @Test
  public void testMapExtraction() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "data",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "after",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "data"
        )
    );

    // Create a schema with a MAP field
    final Schema dataSchema = SchemaBuilder.struct()
        .name("Data")
        .field("key", Schema.STRING_SCHEMA)
        .field("value", Schema.STRING_SCHEMA)
        .build();
    
    final Schema inputSchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("after", SchemaBuilder.map(Schema.STRING_SCHEMA, dataSchema).build())
        .build();
    
    final Schema expectedSchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("after", SchemaBuilder.map(Schema.STRING_SCHEMA, dataSchema).build())
        .field("data", dataSchema)
        .build();

    final Struct dataStruct = new Struct(dataSchema)
        .put("key", "testKey")
        .put("value", "testValue");
    
    final Struct inputStruct = new Struct(inputSchema)
        .put("id", 123)
        .put("after", ImmutableMap.of("data", dataStruct, "other", new Struct(dataSchema).put("key", "otherKey").put("value", "otherValue")));

    final Struct expectedStruct = new Struct(expectedSchema)
        .put("id", 123)
        .put("after", ImmutableMap.of("data", dataStruct, "other", new Struct(dataSchema).put("key", "otherKey").put("value", "otherValue")))
        .put("data", dataStruct);

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    assertSchema(expectedSchema, transformedRecord.valueSchema());
    assertStruct(expectedStruct, (Struct) transformedRecord.value());
  }

  @Test
  public void testMapExtractionMissingKey() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "missing",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "after",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "extracted"
        )
    );

    final Schema dataSchema = SchemaBuilder.struct()
        .field("value", Schema.STRING_SCHEMA)
        .build();
    
    final Schema inputSchema = SchemaBuilder.struct()
        .field("after", SchemaBuilder.map(Schema.STRING_SCHEMA, dataSchema).build())
        .build();

    final Struct dataStruct = new Struct(dataSchema)
        .put("value", "testValue");
    
    final Struct inputStruct = new Struct(inputSchema)
        .put("after", ImmutableMap.of("data", dataStruct));

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      this.transformation.apply(inputRecord);
    });
    assertEquals("Cannot find key 'missing' in map field 'after'", exception.getMessage());
  }

  @Test
  public void testSchemalessMapExtraction() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "data",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "after",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "data"
        )
    );

    // Simulate schemaless JSON (no schema, Map value)
    final Map<Object, Object> nestedData = new HashMap<>();
    nestedData.put("key", "testKey");
    nestedData.put("value", "testValue");
    nestedData.put("timestamp", "2026-01-06T17:45:12Z");

    final Map<Object, Object> afterMap = new HashMap<>();
    afterMap.put("data", nestedData);
    afterMap.put("other", "otherValue");

    final Map<Object, Object> inputValue = new HashMap<>();
    inputValue.put("after", afterMap);
    inputValue.put("updated", "123456789");

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        null,  // No schema
        inputValue,
        1L
    );

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    
    final Map<?, ?> result = (Map<?, ?>) transformedRecord.value();
    assertEquals("123456789", result.get("updated"), "Original field should be preserved");
    assertEquals(afterMap, result.get("after"), "Original after field should be preserved");
    assertEquals(nestedData, result.get("data"), "Extracted data field should be present");
    assertEquals("testKey", ((Map<?, ?>) result.get("data")).get("key"));
  }

  @Test
  public void testSchemalessMapExtractionChain() {
    // Simulate the user's actual use case: chaining multiple extractions
    final ExtractNestedField.Value<SinkRecord> transform1 = new ExtractNestedField.Value<>();
    final ExtractNestedField.Value<SinkRecord> transform2 = new ExtractNestedField.Value<>();
    final ExtractNestedField.Value<SinkRecord> transform3 = new ExtractNestedField.Value<>();
    
    transform1.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "data",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "after",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "data"
        )
    );
    
    transform2.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "value",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "data",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "value"
        )
    );
    
    transform3.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "price",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "value",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "price"
        )
    );

    // Build nested structure similar to user's real data
    final Map<Object, Object> priceData = new HashMap<>();
    priceData.put("StoreKey", 916);
    priceData.put("ItemPositionKey", "1000016750-SINGLE");
    priceData.put("RetailPrice", 199.99);

    final Map<Object, Object> price = new HashMap<>();
    price.put("data", priceData);

    final Map<Object, Object> value = new HashMap<>();
    value.put("price", price);

    final Map<Object, Object> data = new HashMap<>();
    data.put("value", value);
    data.put("key", "1000016750-916-SINGLE");

    final Map<Object, Object> after = new HashMap<>();
    after.put("data", data);

    final Map<Object, Object> inputValue = new HashMap<>();
    inputValue.put("after", after);
    inputValue.put("updated", "1767721512577677111.0000000000");

    SinkRecord record = new SinkRecord(
        "topic",
        1,
        null,
        null,
        null,
        inputValue,
        1L
    );

    // Apply transform chain
    record = transform1.apply(record);
    assertNotNull(((Map<?, ?>) record.value()).get("data"), "data should be extracted");
    
    record = transform2.apply(record);
    assertNotNull(((Map<?, ?>) record.value()).get("value"), "value should be extracted");
    
    record = transform3.apply(record);
    final Map<?, ?> result = (Map<?, ?>) record.value();
    assertNotNull(result.get("price"), "price should be extracted");
    assertEquals(price, result.get("price"));
    
    // Verify original fields are preserved
    assertEquals("1767721512577677111.0000000000", result.get("updated"));
    assertEquals(after, result.get("after"));
  }

  @Test
  public void testSchemalessMapExtractionMissingKey() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "missing",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "after",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "extracted"
        )
    );

    final Map<Object, Object> data = new HashMap<>();
    data.put("value", "testValue");

    final Map<Object, Object> after = new HashMap<>();
    after.put("data", data);

    final Map<Object, Object> inputValue = new HashMap<>();
    inputValue.put("after", after);

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        null,
        inputValue,
        1L
    );

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      this.transformation.apply(inputRecord);
    });
    assertEquals("Cannot find key 'missing' in nested map field 'after'", exception.getMessage());
  }

  @Test
  public void testSchemalessMapExtractionNullOuterField() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "data",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "missing",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "extracted"
        )
    );

    final Map<Object, Object> inputValue = new HashMap<>();
    inputValue.put("after", new HashMap<>());

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        null,
        inputValue,
        1L
    );

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      this.transformation.apply(inputRecord);
    });
    assertEquals("Outer field 'missing' not found or is null", exception.getMessage());
  }

  @Test
  public void testSchemalessArrayExtraction() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "0",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "ClusterPrice",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "ClusterPriceFirst"
        )
    );

    final Map<Object, Object> clusterPrice1 = new HashMap<>();
    clusterPrice1.put("ClusterKey", 17);
    clusterPrice1.put("ClusterPrice", 164.99);
    clusterPrice1.put("ItemPositionKey", "1000016750-SINGLE");

    final Map<Object, Object> clusterPrice2 = new HashMap<>();
    clusterPrice2.put("ClusterKey", 17);
    clusterPrice2.put("ClusterPrice", 199.99);
    clusterPrice2.put("ItemPositionKey", "1000016750-SINGLE");

    final Map<Object, Object> inputValue = new HashMap<>();
    inputValue.put("ClusterPrice", Arrays.asList(clusterPrice1, clusterPrice2));
    inputValue.put("StoreKey", 916);

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        null,
        inputValue,
        1L
    );

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) transformedRecord.value();
    assertEquals(916, result.get("StoreKey"));
    
    @SuppressWarnings("unchecked")
    final Map<String, Object> extractedClusterPrice = (Map<String, Object>) result.get("ClusterPriceFirst");
    assertEquals(17, extractedClusterPrice.get("ClusterKey"));
    assertEquals(164.99, extractedClusterPrice.get("ClusterPrice"));
  }

  @Test
  public void testSchemalessArrayExtractionOutOfBounds() {
    this.transformation.configure(
        ImmutableMap.of(
            ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "5",
            ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "items",
            ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "item"
        )
    );

    final Map<Object, Object> inputValue = new HashMap<>();
    inputValue.put("items", Arrays.asList("item1", "item2"));

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        null,
        inputValue,
        1L
    );

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      this.transformation.apply(inputRecord);
    });
    assertEquals("Cannot access index 5 in array field 'items' (size: 2)", exception.getMessage());
  }

}
