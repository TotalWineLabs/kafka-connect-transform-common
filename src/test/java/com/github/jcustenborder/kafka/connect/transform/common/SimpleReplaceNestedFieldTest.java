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
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleReplaceNestedFieldTest {

  @Test
  public void testValueTransformationWithSchema() {
    ReplaceNestedField.Value<SinkRecord> transformation = new ReplaceNestedField.Value<>();

    // Configure the transformation
    Map<String, String> config = ImmutableMap.of(
      ReplaceNestedFieldConfig.FIELDS_CONF, "shipping.shipping_first_name,shipping.shipping_last_name"
    );
    transformation.configure(config);

    // Create test schema with optional fields
    Schema shippingSchema = SchemaBuilder.struct()
      .field("shipping_first_name", Schema.OPTIONAL_STRING_SCHEMA)
      .field("shipping_last_name", Schema.OPTIONAL_STRING_SCHEMA)
      .field("shipping_line1", Schema.STRING_SCHEMA)
      .build();

    Schema rootSchema = SchemaBuilder.struct()
      .field("shipping", shippingSchema)
      .field("order_id", Schema.STRING_SCHEMA)
      .build();

    // Create test data
    Struct shippingStruct = new Struct(shippingSchema)
      .put("shipping_first_name", "John")
      .put("shipping_last_name", "Doe")
      .put("shipping_line1", "123 Main St");

    Struct rootStruct = new Struct(rootSchema)
      .put("shipping", shippingStruct)
      .put("order_id", "12345");

    SinkRecord record = new SinkRecord("test", 0, null, null, rootSchema, rootStruct, 0);

    // Print input
    System.out.println("\n=== Schema-aware Value Transformation Test ===");
    System.out.println("Input record value: " + rootStruct);
    System.out.println("Input schema: " + rootSchema);

    // Execute transformation
    SinkRecord transformedRecord = transformation.apply(record);

    // Print output
    System.out.println("Output record value: " + transformedRecord.value());
    System.out.println("Output schema: " + transformedRecord.valueSchema());
    System.out.println("========================================\n");

    // Verify results
    assertNotNull(transformedRecord);
    Struct transformedValue = (Struct) transformedRecord.value();
    Struct transformedShipping = transformedValue.getStruct("shipping");

    assertNull(transformedShipping.get("shipping_first_name"));
    assertNull(transformedShipping.get("shipping_last_name"));
    assertEquals("123 Main St", transformedShipping.get("shipping_line1"));
    assertEquals("12345", transformedValue.get("order_id"));
  }

  @Test
  public void testValueTransformationSchemaless() {
    ReplaceNestedField.Value<SinkRecord> transformation = new ReplaceNestedField.Value<>();

    // Configure the transformation
    Map<String, String> config = ImmutableMap.of(
      ReplaceNestedFieldConfig.FIELDS_CONF, "delivery.delivery_first_name,delivery.delivery_last_name"
    );
    transformation.configure(config);

    // Create test data without schema
    Map<String, Object> deliveryData = new LinkedHashMap<>();
    deliveryData.put("delivery_first_name", "Jane");
    deliveryData.put("delivery_last_name", "Smith");
    deliveryData.put("delivery_line1", "456 Oak Ave");

    Map<String, Object> rootData = new LinkedHashMap<>();
    rootData.put("delivery", deliveryData);
    rootData.put("order_id", "67890");

    SinkRecord record = new SinkRecord("test", 0, null, null, null, rootData, 0);

    // Print input
    System.out.println("\n=== Schemaless Value Transformation Test ===");
    System.out.println("Input record value: " + rootData);
    System.out.println("Input schema: null (schemaless)");

    // Execute transformation
    SinkRecord transformedRecord = transformation.apply(record);

    // Print output
    System.out.println("Output record value: " + transformedRecord.value());
    System.out.println("Output schema: " + transformedRecord.valueSchema());
    System.out.println("==========================================\n");

    // Verify results
    assertNotNull(transformedRecord);
    @SuppressWarnings("unchecked")
    Map<String, Object> transformedValue = (Map<String, Object>) transformedRecord.value();
    @SuppressWarnings("unchecked")
    Map<String, Object> transformedDelivery = (Map<String, Object>) transformedValue.get("delivery");

    assertNull(transformedDelivery.get("delivery_first_name"));
    assertNull(transformedDelivery.get("delivery_last_name"));
    assertEquals("456 Oak Ave", transformedDelivery.get("delivery_line1"));
    assertEquals("67890", transformedValue.get("order_id"));
  }

  @Test
  public void testKeyTransformation() {
    ReplaceNestedField.Key<SinkRecord> transformation = new ReplaceNestedField.Key<>();

    // Configure the transformation
    Map<String, String> config = ImmutableMap.of(
      ReplaceNestedFieldConfig.FIELDS_CONF, "user.user_id"
    );
    transformation.configure(config);

    // Create test data for key
    Map<String, Object> userData = new LinkedHashMap<>();
    userData.put("user_id", "sensitive_user_123");
    userData.put("user_name", "testuser");

    Map<String, Object> keyData = new LinkedHashMap<>();
    keyData.put("user", userData);

    SinkRecord record = new SinkRecord("test", 0, null, keyData, null, null, 0);

    // Print input
    System.out.println("\n=== Key Transformation Test ===");
    System.out.println("Input record key: " + keyData);
    System.out.println("Input key schema: null (schemaless)");

    // Execute transformation
    SinkRecord transformedRecord = transformation.apply(record);

    // Print output
    System.out.println("Output record key: " + transformedRecord.key());
    System.out.println("Output key schema: " + transformedRecord.keySchema());
    System.out.println("===============================\n");

    // Verify results
    assertNotNull(transformedRecord);
    @SuppressWarnings("unchecked")
    Map<String, Object> transformedKey = (Map<String, Object>) transformedRecord.key();
    @SuppressWarnings("unchecked")
    Map<String, Object> transformedUser = (Map<String, Object>) transformedKey.get("user");

    assertNull(transformedUser.get("user_id"));
    assertEquals("testuser", transformedUser.get("user_name"));
  }
}