package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

public class ExtractTopicTest {

  @Test
  public void test() {

    Map<String, Object> value = ImmutableMap.of(
        "object", ImmutableMap.of("id", "336f5547-9224-4b7b-a9e0-5cc87b48367a", "type", "SALE"));

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

    try (ExtractTopicName<SinkRecord> transform = new ExtractTopicName.Value<>()) {
      transform.configure(
          ImmutableMap.of(
              ExtractTopicNameConfig.FIELD_CONFIG, "$.object.type",
              ExtractTopicNameConfig.FIELD_FORMAT_CONFIG, "JSON_PATH",
              ExtractTopicNameConfig.SKIP_MISSING_OR_NULL_CONFIG, true));
      final SinkRecord actual = transform.apply(input);
      assertEquals("SALE", actual.topic(), "Topic should match.");
    }
  }
}
