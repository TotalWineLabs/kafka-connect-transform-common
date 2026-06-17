package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.DataException;

import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;

/**
 * SMT that converts timestamp fields (including nested fields) to a target type.
 * Supports dotted field paths for nested fields (e.g., after.start_date).
 * Handles both schemaful (Struct) and schemaless (Map) records, rebuilding
 * schemas as needed for type-changing conversions.
 */
public class TimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Convert timestamp fields (including nested fields) to a target type. " +
            "Supports dotted field paths for nested fields (e.g., after.start_date).";

    public static final String FIELD_CONFIG = "field";
    public static final String TARGET_TYPE_CONFIG = "target.type";
    public static final String FORMAT_CONFIG = "format";
    public static final String UNIX_PRECISION_CONFIG = "unix.precision";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Field path to convert (dotted for nested)")
            .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target type: string, unix, Date, Time, Timestamp")
            .define(FORMAT_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Format string for string type")
            .define(UNIX_PRECISION_CONFIG, ConfigDef.Type.STRING, "milliseconds", ConfigDef.Importance.LOW, "Unix precision: milliseconds, seconds, microseconds, nanoseconds");

    private String fieldPath;
    private String targetType;
    private String format;
    private String unixPrecision;
    private List<String> fieldPathParts;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        Map<String, Object> parsed = CONFIG_DEF.parse(configs);
        this.fieldPath = (String) parsed.get(FIELD_CONFIG);
        this.targetType = (String) parsed.get(TARGET_TYPE_CONFIG);
        this.format = (String) parsed.get(FORMAT_CONFIG);
        this.unixPrecision = (String) parsed.get(UNIX_PRECISION_CONFIG);
        this.fieldPathParts = Arrays.asList(fieldPath.split("\\."));
    }

    @Override
    public R apply(R record) {
        Object value = record.value();
        Schema schema = record.valueSchema();
        ConvertResult result = convertField(value, schema, fieldPathParts, 0);
        if (result.value == value) return record;
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                result.schema, result.value, record.timestamp());
    }

    /**
     * Holds both the converted value and updated schema (schema may change for type-changing conversions on Structs).
     */
    private static class ConvertResult {
        final Object value;
        final Schema schema;
        ConvertResult(Object value, Schema schema) {
            this.value = value;
            this.schema = schema;
        }
    }

    @SuppressWarnings("unchecked")
    private ConvertResult convertField(Object value, Schema schema, List<String> path, int idx) {
        if (value == null || path == null || idx >= path.size()) return new ConvertResult(value, schema);
        String field = path.get(idx);

        if (schema != null && schema.type() == Schema.Type.STRUCT) {
            Struct struct = (Struct) value;
            Field f = schema.field(field);
            if (f == null) return new ConvertResult(value, schema);
            Object orig = struct.get(field);

            ConvertResult nested;
            if (idx == path.size() - 1) {
                // Leaf field: perform conversion
                Object converted = convertTimestamp(orig);
                Schema newFieldSchema = schemaForConvertedValue(converted, f.schema());
                nested = new ConvertResult(converted, newFieldSchema);
            } else {
                // Intermediate field: recurse
                nested = convertField(orig, f.schema(), path, idx + 1);
            }

            if (Objects.equals(orig, nested.value) && nested.schema == f.schema()) {
                return new ConvertResult(value, schema);
            }

            // Rebuild schema with updated field type
            Schema updatedSchema = rebuildSchema(schema, field, nested.schema);
            Struct copy = new Struct(updatedSchema);
            for (Field sf : updatedSchema.fields()) {
                if (sf.name().equals(field)) {
                    copy.put(sf, nested.value);
                } else {
                    copy.put(sf, struct.get(sf.name()));
                }
            }
            return new ConvertResult(copy, updatedSchema);

        } else if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            Object orig = map.get(field);
            Object updated;
            if (idx == path.size() - 1) {
                updated = convertTimestamp(orig);
            } else {
                ConvertResult nested = convertField(orig, null, path, idx + 1);
                updated = nested.value;
            }
            if (Objects.equals(orig, updated)) return new ConvertResult(value, schema);
            Map<String, Object> copy = new HashMap<>(map);
            copy.put(field, updated);
            return new ConvertResult(copy, null);
        }
        return new ConvertResult(value, schema);
    }

    /**
     * Determine the appropriate schema for the converted value.
     */
    private Schema schemaForConvertedValue(Object converted, Schema originalSchema) {
        if (converted == null) {
            // Preserve optionality
            return originalSchema.isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : originalSchema;
        }
        if (converted instanceof String) {
            return originalSchema.isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
        }
        if (converted instanceof Long) {
            return originalSchema.isOptional() ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
        }
        return originalSchema;
    }

    /**
     * Rebuild a Struct schema replacing one field's schema with a new one.
     */
    private Schema rebuildSchema(Schema structSchema, String fieldName, Schema newFieldSchema) {
        SchemaBuilder builder = SchemaBuilder.struct();
        if (structSchema.name() != null) builder.name(structSchema.name());
        if (structSchema.isOptional()) builder.optional();
        for (Field f : structSchema.fields()) {
            if (f.name().equals(fieldName)) {
                builder.field(f.name(), newFieldSchema);
            } else {
                builder.field(f.name(), f.schema());
            }
        }
        return builder.build();
    }

    private Object convertTimestamp(Object orig) {
        if (orig == null) return null;
        if (orig instanceof Long) {
            long ts = (Long) orig;
            switch (targetType.toLowerCase(Locale.ROOT)) {
                case "string":
                    if (format != null && !format.isEmpty()) {
                        SimpleDateFormat sdf = new SimpleDateFormat(format);
                        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                        return sdf.format(new Date(ts));
                    } else {
                        return Long.toString(ts);
                    }
                case "unix":
                    switch (unixPrecision) {
                        case "seconds": return ts / 1000L;
                        case "milliseconds": return ts;
                        case "microseconds": return ts * 1000L;
                        case "nanoseconds": return ts * 1000000L;
                        default: return ts;
                    }
                case "date":
                case "time":
                case "timestamp":
                    return ts;
                default:
                    throw new DataException("Unsupported target.type: " + targetType);
            }
        } else if (orig instanceof String) {
            if (targetType.equalsIgnoreCase("unix")) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat(format);
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    Date d = sdf.parse((String) orig);
                    return d.getTime();
                } catch (ParseException e) {
                    throw new DataException("Failed to parse date string: " + orig, e);
                }
            }
            return orig;
        }
        return orig;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "TimestampConverter{" +
                "fieldPath='" + fieldPath + '\'' +
                ", targetType='" + targetType + '\'' +
                ", format='" + format + '\'' +
                ", unixPrecision='" + unixPrecision + '\'' +
                '}';
    }
}
