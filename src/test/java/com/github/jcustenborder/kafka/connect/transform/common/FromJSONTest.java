  package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.kafka.connect.errors.DataException;

public class FromJSONTest {

  @Test
  public void testStruct() {
    // Build Struct schema and value equivalent to the Map test
    org.apache.kafka.connect.data.Schema afterSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
        .field("item_location_key", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
        .field("item_location", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .build();
    org.apache.kafka.connect.data.Schema valueSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
        .field("after", afterSchema)
        .build();

    org.apache.kafka.connect.data.Struct afterStruct = new org.apache.kafka.connect.data.Struct(afterSchema)
        .put("item_location_key", 74576656)
        .put("item_location", "{\"headers\":{\"source\":\"DataSourceA\",\"servicemodifier\":\"DataService\",\"action\":\"UPSERT\",\"messagetype\":\"Webhook\",\"messageformatversion\":\"1.0\",\"correlationId\":\"991a2a96-b594-4101-9da5-789dc1bf3225\",\"tracestate\":\"\"},\"key\":\"74576656\",\"value\":{\"location\":{\"itemlocation\":{\"ItemLocationKey\":74576656,\"IsActive\":true,\"CreateDate\":\"2022-02-14T22:28:00.97\",\"CreateUserKey\":789,\"LastModifyDate\":\"2022-02-15T10:23:34.26\",\"LastModifyUserKey\":789},\"itemlocationkey\":999999}},\"timestamp\":\"2025-05-27T18:28:15.553459Z\"}");
    org.apache.kafka.connect.data.Struct valueStruct = new org.apache.kafka.connect.data.Struct(valueSchema)
        .put("after", afterStruct);

    final SinkRecord input = new SinkRecord(
        "source-topic",
        1,
        null,
        "",
        valueSchema,
        valueStruct,
        1234123L,
        12341312L,
        TimestampType.NO_TIMESTAMP_TYPE);

    try (FromJSON<SinkRecord> transform = new FromJSON.Value<>()) {
      transform.configure(
          ImmutableMap.of(
              FromJSONConfig.FIELD_CONFIG, "$.after.item_location",
              FromJSONConfig.FIELD_REPLACEMENT, "$.after.item_location",
              FromJSONConfig.FIELD_FORMAT_CONFIG, "JSON_PATH",
              FromJSONConfig.SKIP_MISSING_OR_NULL_CONFIG, true));
      final SinkRecord actual = transform.apply(input);
      // The output should be a schemaless Map
      Map<?, ?> actualValue = (Map<?, ?>) actual.value();
      Map<?, ?> after = (Map<?, ?>) actualValue.get("after");
      Object itemLocationObj = after.get("item_location");
      assertEquals(true, itemLocationObj instanceof Map, "item_location should be a Map");
      Map<?, ?> itemLocation = (Map<?, ?>) itemLocationObj;
      assertEquals(true, itemLocation.containsKey("headers"), "item_location should have 'headers'");
      assertEquals(true, itemLocation.containsKey("key"), "item_location should have 'key'");
      assertEquals(true, itemLocation.containsKey("value"), "item_location should have 'value'");

      // Additional assertions
      assertEquals(true, itemLocation.get("headers") instanceof Map, "'headers' should be a Map");
      assertEquals(true, itemLocation.get("key") instanceof String, "'key' should be a String");
      assertEquals(true, itemLocation.get("value") instanceof Map, "'value' should be a Map");
    }
  }

  @Test
  public void testMap() {

    Map<String, Object> value = Map.of(
        "after", 
          Map.of(
            "item_location_key", 74576656, 
            "item_location", "{\"headers\":{\"source\":\"DataSourceA\",\"servicemodifier\":\"DataService\",\"action\":\"UPSERT\",\"messagetype\":\"Webhook\",\"messageformatversion\":\"1.0\",\"correlationId\":\"991a2a96-b594-4101-9da5-789dc1bf3225\",\"tracestate\":\"\"},\"key\":\"74576656\",\"value\":{\"location\":{\"itemlocation\":{\"ItemLocationKey\":74576656,\"IsActive\":true,\"CreateDate\":\"2022-02-14T22:28:00.97\",\"CreateUserKey\":789,\"LastModifyDate\":\"2022-02-15T10:23:34.26\",\"LastModifyUserKey\":789},\"itemlocationkey\":999999}},\"timestamp\":\"2025-05-27T18:28:15.553459Z\"}"
          )
    );

    final SinkRecord input = new SinkRecord(
        "source-topic",
        1,
        null,
        "",
        null,
        value,
        1234123L,
        12341312L,
        TimestampType.NO_TIMESTAMP_TYPE);

    try (FromJSON<SinkRecord> transform = new FromJSON.Value<>()) {
      transform.configure(
          ImmutableMap.of(
              FromJSONConfig.FIELD_CONFIG, "$.after.item_location",
              FromJSONConfig.FIELD_REPLACEMENT, "$.after.item_location",
              FromJSONConfig.FIELD_FORMAT_CONFIG, "JSON_PATH",
              FromJSONConfig.SKIP_MISSING_OR_NULL_CONFIG, true));
      final SinkRecord actual = transform.apply(input);
      // TODO assert that the item_location field is a map with the expected structure.
      Map<?, ?> actualValue = (Map<?, ?>) actual.value();
      Map<?, ?> after = (Map<?, ?>) actualValue.get("after");
      Object itemLocationObj = after.get("item_location");
      assertEquals(true, itemLocationObj instanceof Map, "item_location should be a Map");
      Map<?, ?> itemLocation = (Map<?, ?>) itemLocationObj;
      assertEquals(true, itemLocation.containsKey("headers"), "item_location should have 'headers'");
      assertEquals(true, itemLocation.containsKey("key"), "item_location should have 'key'");
      assertEquals(true, itemLocation.containsKey("value"), "item_location should have 'value'");

      // Additional assertions
      assertEquals(true, itemLocation.get("headers") instanceof Map, "'headers' should be a Map");
      assertEquals(true, itemLocation.get("key") instanceof String, "'key' should be a String");
      assertEquals(true, itemLocation.get("value") instanceof Map, "'value' should be a Map");
      
    }
  }

@Test
  public void testComplexStructWithNestedListsAndStructs() {
    // Build the innermost column struct
    org.apache.kafka.connect.data.Schema enumValuesSchema = org.apache.kafka.connect.data.SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build();
    org.apache.kafka.connect.data.Schema columnSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
        .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("jdbcType", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
        .field("typeName", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("typeExpression", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("charsetName", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("length", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
        .field("scale", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
        .field("position", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
        .field("optional", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
        .field("autoIncremented", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
        .field("generated", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
        .field("comment", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("hasDefaultValue", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
        .field("enumValues", enumValuesSchema)
        .build();

    org.apache.kafka.connect.data.Schema columnsSchema = org.apache.kafka.connect.data.SchemaBuilder.array(columnSchema).build();

    org.apache.kafka.connect.data.Schema tableSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
        .field("defaultCharsetName", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("primaryKeyColumnNames", org.apache.kafka.connect.data.SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build())
        .field("columns", columnsSchema)
        .field("attributes", org.apache.kafka.connect.data.SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build())
        .build();

    org.apache.kafka.connect.data.Schema tableChangeSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
        .field("type", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("table", tableSchema)
        .field("comment", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    org.apache.kafka.connect.data.Schema tableChangesSchema = org.apache.kafka.connect.data.SchemaBuilder.array(tableChangeSchema).build();

    org.apache.kafka.connect.data.Schema rootSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
        .field("source", org.apache.kafka.connect.data.SchemaBuilder.struct()
            .field("server", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("database", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build())
        .field("position", org.apache.kafka.connect.data.SchemaBuilder.struct()
            .field("commit_lsn", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("snapshot", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
            .field("snapshot_completed", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
            .build())
        .field("ts_ms", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("databaseName", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("schemaName", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("tableChanges", tableChangesSchema)
        .build();

    // Build the column structs (only a few for brevity)
    org.apache.kafka.connect.data.Struct column1 = new org.apache.kafka.connect.data.Struct(columnSchema)
        .put("name", "layout_store_area_key")
        .put("jdbcType", 4)
        .put("typeName", "int identity")
        .put("typeExpression", "int identity")
        .put("charsetName", null)
        .put("length", 10)
        .put("scale", 0)
        .put("position", 1)
        .put("optional", false)
        .put("autoIncremented", true)
        .put("generated", false)
        .put("comment", null)
        .put("hasDefaultValue", false)
        .put("enumValues", java.util.Collections.emptyList());

    org.apache.kafka.connect.data.Struct column2 = new org.apache.kafka.connect.data.Struct(columnSchema)
        .put("name", "store_key")
        .put("jdbcType", 4)
        .put("typeName", "int")
        .put("typeExpression", "int")
        .put("charsetName", null)
        .put("length", 10)
        .put("scale", 0)
        .put("position", 2)
        .put("optional", false)
        .put("autoIncremented", false)
        .put("generated", false)
        .put("comment", null)
        .put("hasDefaultValue", false)
        .put("enumValues", java.util.Collections.emptyList());

    java.util.List<org.apache.kafka.connect.data.Struct> columns = java.util.Arrays.asList(column1, column2);

    org.apache.kafka.connect.data.Struct tableStruct = new org.apache.kafka.connect.data.Struct(tableSchema)
        .put("defaultCharsetName", null)
        .put("primaryKeyColumnNames", java.util.Collections.emptyList())
        .put("columns", columns)
        .put("attributes", java.util.Collections.emptyList());

    org.apache.kafka.connect.data.Struct tableChangeStruct = new org.apache.kafka.connect.data.Struct(tableChangeSchema)
        .put("type", "CREATE")
        .put("id", "{\"headers\":{\"source\":\"DataSourceA\",\"servicemodifier\":\"DataService\",\"action\":\"UPSERT\",\"messagetype\":\"Webhook\",\"messageformatversion\":\"1.0\",\"correlationId\":\"991a2a96-b594-4101-9da5-789dc1bf3225\",\"tracestate\":\"\"},\"key\":\"74576656\",\"value\":{\"location\":{\"itemlocation\":{\"ItemLocationKey\":74576656,\"IsActive\":true,\"CreateDate\":\"2022-02-14T22:28:00.97\",\"CreateUserKey\":789,\"LastModifyDate\":\"2022-02-15T10:23:34.26\",\"LastModifyUserKey\":789},\"itemlocationkey\":999999}},\"timestamp\":\"2025-05-27T18:28:15.553459Z\"}")
        .put("table", tableStruct)
        .put("comment", null);

    java.util.List<org.apache.kafka.connect.data.Struct> tableChanges = java.util.Collections.singletonList(tableChangeStruct);

    org.apache.kafka.connect.data.Struct rootStruct = new org.apache.kafka.connect.data.Struct(rootSchema)
        .put("source", new org.apache.kafka.connect.data.Struct(rootSchema.field("source").schema())
            .put("server", "qa.storelayout.cdc.json")
            .put("database", "StoreLayout_TR"))
        .put("position", new org.apache.kafka.connect.data.Struct(rootSchema.field("position").schema())
            .put("commit_lsn", "0017e79d:0009c888:0003")
            .put("snapshot", true)
            .put("snapshot_completed", false))
        .put("ts_ms", 1748961871960L)
        .put("databaseName", "StoreLayout_TR")
        .put("schemaName", "dbo")
        .put("tableChanges", tableChanges);

    final SinkRecord input = new SinkRecord(
        "source-topic",
        1,
        null,
        "",
        rootSchema,
        rootStruct,
        1234123L,
        12341312L,
        TimestampType.NO_TIMESTAMP_TYPE);

    try (FromJSON<SinkRecord> transform = new FromJSON.Value<>()) {
      transform.configure(
          ImmutableMap.of(
              FromJSONConfig.FIELD_CONFIG, "$.tableChanges[0].id",
              FromJSONConfig.FIELD_REPLACEMENT, "$.tableChanges[0].id",
              FromJSONConfig.FIELD_FORMAT_CONFIG, "JSON_PATH",
              FromJSONConfig.SKIP_MISSING_OR_NULL_CONFIG, false));
      final SinkRecord actual = transform.apply(input);
      Map<?, ?> actualValue = (Map<?, ?>) actual.value();
      java.util.List<?> tableChangesOut = (java.util.List<?>) actualValue.get("tableChanges");
      Map<?, ?> tableChange0 = (Map<?, ?>) tableChangesOut.get(0);
      Map<?, ?> table = (Map<?, ?>) tableChange0.get("table");
      java.util.List<?> columnsOut = (java.util.List<?>) table.get("columns");
      Map<?, ?> column0 = (Map<?, ?>) columnsOut.get(0);
      // The field should be replaced with the string value (since the JSON path points to a string field)
      assertEquals("layout_store_area_key", column0.get("name"));
    }
  }  

}
