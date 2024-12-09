package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

public class FilterTest {
  public Filter.Value<SinkRecord> transform_include;
  public Filter.Value<SinkRecord> transform_exclude;
  public Filter.Value<SinkRecord> transform_missing;
  public Filter.Value<SinkRecord> transform_empty;

  @BeforeEach
  public void before() {
    this.transform_include = new Filter.Value<SinkRecord>();
    this.transform_exclude = new Filter.Value<SinkRecord>();
    this.transform_missing = new Filter.Value<SinkRecord>();
    this.transform_empty = new Filter.Value<SinkRecord>();

    this.transform_include.configure(
        ImmutableMap.of(
            FilterConfig.FILTER_CONDITION_CONFIG, "$[?(@.object.type == 'SALE')]",
            FilterConfig.FILTER_TYPE_CONFIG, "include",
            FilterConfig.MISSING_OR_NULL_BEHAVIOR_CONFIG, "exclude"));
    this.transform_exclude.configure(
      ImmutableMap.of(
          FilterConfig.FILTER_CONDITION_CONFIG, "$[?(@.object.type == 'SALE')]",
          FilterConfig.FILTER_TYPE_CONFIG, "exclude",
          FilterConfig.MISSING_OR_NULL_BEHAVIOR_CONFIG, "exclude"));
    this.transform_missing.configure(
      ImmutableMap.of(
          FilterConfig.FILTER_CONDITION_CONFIG, "$[?(@.object.type == 'SALE')]",
          FilterConfig.FILTER_TYPE_CONFIG, "include",
          FilterConfig.MISSING_OR_NULL_BEHAVIOR_CONFIG, "exclude"));
    this.transform_empty.configure(
      ImmutableMap.of(
          FilterConfig.FILTER_CONDITION_CONFIG, "$[?(@.object.type == null)]",
          FilterConfig.FILTER_TYPE_CONFIG, "exclude",
          FilterConfig.MISSING_OR_NULL_BEHAVIOR_CONFIG, "include"));          
  }  

  SinkRecord map(Map value) {
    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        null,
        value,
        1234L);
  }

  SinkRecord struct(String value) {

    Schema schema = SchemaBuilder.struct()
        .field("input", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("input", value);

    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        schema,
        struct,
        1234L);
  }

  @Test
  public void filter_include() {

    Map<String, Object> value = Map.of(
        "object", Map.of(
          "id", "336f5547-9224-4b7b-a9e0-5cc87b48367a", 
          "type", "SALE"
        )
    );   
    assertNotNull(this.transform_include.apply(map(value)));
  }

  @Test
  public void filter_exclude() {

    Map<String, Object> value = Map.of(
        "object", Map.of(
          "id", "336f5547-9224-4b7b-a9e0-5cc87b48367a", 
          "type", "SALE"
        )
    );
    assertNull(this.transform_exclude.apply(map(value)));
  }

  @Test
  public void filter_missing() {

    Map<String, Object> value = Map.of(
        "object", Map.of(
          "id", "336f5547-9224-4b7b-a9e0-5cc87b48367a"
        )
    );
    assertNull(this.transform_missing.apply(map(value)));
  }  

  @Test
  public void filter_empty() {

    Map<String, Object> value = new HashMap<>();
    Map<String, Object> innerObject = new HashMap<>();
    innerObject.put("id", "336f5547-9224-4b7b-a9e0-5cc87b48367a");
    innerObject.put("type", null); // Allow null here
    value.put("object", innerObject);
    assertNull(this.transform_empty.apply(map(value)));
  }    
}