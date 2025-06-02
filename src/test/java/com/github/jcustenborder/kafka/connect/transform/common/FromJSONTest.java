package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

public class FromJSONTest {

  @Test
  public void test() {

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
}
