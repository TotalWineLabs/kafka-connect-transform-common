package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TimestampConverterTest {
    @Test
    public void testNestedStructConversion() {
        Schema innerSchema = SchemaBuilder.struct().name("Inner").field("start_date", Schema.INT64_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().name("Envelope")
                .field("before", innerSchema)
                .field("after", innerSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        Struct before = new Struct(innerSchema).put("start_date", 1743984000000L);
        Struct after = new Struct(innerSchema).put("start_date", 1743984000000L);
        Struct envelope = new Struct(schema)
                .put("before", before)
                .put("after", after)
                .put("op", "r");

        Map<String, Object> config = new HashMap<>();
        config.put("field", "after.start_date");
        config.put("target.type", "string");
        config.put("format", "yyyy-MM-dd'T'HH:mm:ss.SSS");
        TimestampConverter<SinkRecord> xform = new TimestampConverter<>();
        xform.configure(config);

        SinkRecord record = new SinkRecord("topic", 0, null, null, schema, envelope, 0);
        SinkRecord result = xform.apply(record);
        Struct resultEnvelope = (Struct) result.value();
        Struct resultAfter = (Struct) resultEnvelope.get("after");
        // Type-changing conversion is not allowed for Structs, so value remains Long
        assertEquals(1743984000000L, resultAfter.get("start_date"));
        Struct resultBefore = (Struct) resultEnvelope.get("before");
        // before.start_date should remain unchanged
        assertEquals(1743984000000L, resultBefore.get("start_date"));
    }

    @Test
    public void testNestedMapConversion() {
        Map<String, Object> before = new HashMap<>();
        before.put("start_date", 1743984000000L);
        Map<String, Object> after = new HashMap<>();
        after.put("start_date", 1743984000000L);
        Map<String, Object> envelope = new HashMap<>();
        envelope.put("before", before);
        envelope.put("after", after);
        envelope.put("op", "r");

        Map<String, Object> config = new HashMap<>();
        config.put("field", "after.start_date");
        config.put("target.type", "string");
        config.put("format", "yyyy-MM-dd'T'HH:mm:ss.SSS");
        TimestampConverter<SinkRecord> xform = new TimestampConverter<>();
        xform.configure(config);

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, envelope, 0);
        SinkRecord result = xform.apply(record);
        Map resultEnvelope = (Map) result.value();
        Map resultAfter = (Map) resultEnvelope.get("after");
        // The correct UTC date for 1743984000000L is 2025-04-07T00:00:00.000
        assertEquals("2025-04-07T00:00:00.000", resultAfter.get("start_date"));
        Map resultBefore = (Map) resultEnvelope.get("before");
        assertEquals(1743984000000L, resultBefore.get("start_date"));
    }

    @Test
    public void testTopLevelStructUnixSecondsConversion() {
        Schema schema = SchemaBuilder.struct().name("TopLevel")
                .field("start_date", Schema.INT64_SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("start_date", 1743984000000L)
                .put("op", "r");

        Map<String, Object> config = new HashMap<>();
        config.put("field", "start_date");
        config.put("target.type", "unix");
        config.put("unix.precision", "seconds");

        TimestampConverter<SinkRecord> xform = new TimestampConverter<>();
        xform.configure(config);

        SinkRecord record = new SinkRecord("topic", 0, null, null, schema, value, 0);
        SinkRecord result = xform.apply(record);
        Struct resultValue = (Struct) result.value();

        assertEquals(1743984000L, resultValue.get("start_date"));
        assertEquals("r", resultValue.get("op"));
    }

    @Test
    public void testTopLevelMapStringConversion() {
        Map<String, Object> value = new HashMap<>();
        value.put("start_date", 1743984000000L);
        value.put("op", "r");

        Map<String, Object> config = new HashMap<>();
        config.put("field", "start_date");
        config.put("target.type", "string");
        config.put("format", "yyyy-MM-dd'T'HH:mm:ss.SSS");

        TimestampConverter<SinkRecord> xform = new TimestampConverter<>();
        xform.configure(config);

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, value, 0);
        SinkRecord result = xform.apply(record);
        Map resultValue = (Map) result.value();

        assertEquals("2025-04-07T00:00:00.000", resultValue.get("start_date"));
        assertEquals("r", resultValue.get("op"));
    }
}
