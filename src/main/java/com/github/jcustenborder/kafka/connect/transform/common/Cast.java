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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Title("Cast")
@Description("This transformation casts fields, or the entire key or value, to a different type. "
    + "Unlike the stock cast transformation fields nested inside structs, maps and arrays can be "
    + "targeted with a field path such as `ITEM.ATTRIBUTES.IS_DIGITAL_GOOD`, `RATINGS[*].VINTAGE` "
    + "or `**.IS_*`. Records with and without a schema are both supported.")
@DocumentationTip("Strings are cast to booleans using the common textual representations, so `0`, "
    + "`false`, `f`, `no`, `n` and `off` all become false while `1`, `true`, `t`, `yes`, `y` and "
    + "`on` all become true.")
public abstract class Cast<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Set<String> TRUE_VALUES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList("true", "t", "yes", "y", "on", "1"))
  );
  private static final Set<String> FALSE_VALUES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList("false", "f", "no", "n", "off", "0"))
  );

  CastConfig config;

  @Override
  public ConfigDef config() {
    return CastConfig.config();
  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new CastConfig(settings);
  }

  @Override
  public void close() {
  }

  @Override
  public R apply(R record) {
    Schema inputSchema = inputSchema(record);
    Object inputValue = inputValue(record);

    if (null == inputValue) {
      if (this.config.skipMissingOrNull) {
        return record;
      }
      throw new DataException("Record is null and 'skip.missing.or.null' is false.");
    }

    Schema schema = inputSchema;
    Object value = inputValue;
    for (CastConfig.CastSpec spec : this.config.specs) {
      SchemaAndValue result = FieldPathUpdater.update(
          schema,
          value,
          Collections.singletonList(spec.path),
          (fieldSchema, fieldValue) -> cast(spec.type, fieldSchema, fieldValue),
          this.config.skipMissingOrNull
      );
      schema = result.schema();
      value = result.value();
    }

    if (value == inputValue && Objects.equals(schema, inputSchema)) {
      return record;
    }
    return newRecord(record, schema, value);
  }

  protected abstract Schema inputSchema(R record);

  protected abstract Object inputValue(R record);

  protected abstract R newRecord(R record, Schema schema, Object value);

  static SchemaAndValue cast(Schema.Type type, Schema schema, Object value) {
    Object converted = convert(type, schema, value);
    Schema convertedSchema = null == schema ? null : castSchema(type, schema.isOptional());
    return new SchemaAndValue(convertedSchema, converted);
  }

  static Schema castSchema(Schema.Type type, boolean optional) {
    switch (type) {
      case INT8:
        return optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
      case INT16:
        return optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
      case INT32:
        return optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
      case INT64:
        return optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
      case FLOAT32:
        return optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA;
      case FLOAT64:
        return optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
      case BOOLEAN:
        return optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
      case STRING:
        return optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
      default:
        throw new DataException("Cannot cast to " + type + ".");
    }
  }

  static Object convert(Schema.Type type, Schema schema, Object value) {
    if (null == value) {
      return null;
    }
    Object input = Schema.Type.STRING == type ? value : fromLogical(schema, value);

    switch (type) {
      case INT8:
        return input instanceof Byte ? input : (byte) toLong(input);
      case INT16:
        return input instanceof Short ? input : (short) toLong(input);
      case INT32:
        return input instanceof Integer ? input : (int) toLong(input);
      case INT64:
        return input instanceof Long ? input : toLong(input);
      case FLOAT32:
        return input instanceof Float ? input : (float) toDouble(input);
      case FLOAT64:
        return input instanceof Double ? input : toDouble(input);
      case BOOLEAN:
        return input instanceof Boolean ? input : toBoolean(input);
      case STRING:
        return input instanceof String ? input : toText(schema, input);
      default:
        throw new DataException("Cannot cast to " + type + ".");
    }
  }

  /**
   * Replaces a logical value with the primitive Kafka Connect stores it as.
   */
  private static Object fromLogical(Schema schema, Object value) {
    if (null == schema || null == schema.name()) {
      return value;
    }
    if (Decimal.LOGICAL_NAME.equals(schema.name()) && value instanceof BigDecimal) {
      return value;
    }
    if (Date.LOGICAL_NAME.equals(schema.name()) && value instanceof java.util.Date) {
      return Date.fromLogical(schema, (java.util.Date) value);
    }
    if (Time.LOGICAL_NAME.equals(schema.name()) && value instanceof java.util.Date) {
      return Time.fromLogical(schema, (java.util.Date) value);
    }
    if (Timestamp.LOGICAL_NAME.equals(schema.name()) && value instanceof java.util.Date) {
      return Timestamp.fromLogical(schema, (java.util.Date) value);
    }
    return value;
  }

  static boolean toBoolean(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof Number) {
      return 0 != ((Number) value).doubleValue();
    }
    if (value instanceof String) {
      String text = ((String) value).trim().toLowerCase(Locale.ROOT);
      if (TRUE_VALUES.contains(text)) {
        return true;
      }
      if (FALSE_VALUES.contains(text)) {
        return false;
      }
      try {
        return 0 != new BigDecimal(text).signum();
      } catch (NumberFormatException ex) {
        throw new DataException("Could not cast '" + value + "' to a boolean.");
      }
    }
    throw new DataException("Could not cast " + value.getClass().getName() + " to a boolean.");
  }

  static long toLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? 1L : 0L;
    }
    if (value instanceof java.util.Date) {
      return ((java.util.Date) value).getTime();
    }
    if (value instanceof String) {
      String text = ((String) value).trim();
      try {
        return Long.parseLong(text);
      } catch (NumberFormatException ex) {
        try {
          return new BigDecimal(text).longValue();
        } catch (NumberFormatException inner) {
          throw new DataException("Could not cast '" + value + "' to a number.");
        }
      }
    }
    throw new DataException("Could not cast " + value.getClass().getName() + " to a number.");
  }

  static double toDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? 1.0D : 0.0D;
    }
    if (value instanceof java.util.Date) {
      return ((java.util.Date) value).getTime();
    }
    if (value instanceof String) {
      String text = ((String) value).trim();
      try {
        return Double.parseDouble(text);
      } catch (NumberFormatException ex) {
        throw new DataException("Could not cast '" + value + "' to a number.");
      }
    }
    throw new DataException("Could not cast " + value.getClass().getName() + " to a number.");
  }

  static String toText(Schema schema, Object value) {
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    }
    if (value instanceof java.util.Date || value instanceof byte[] || value instanceof ByteBuffer) {
      return Values.convertToString(schema, value);
    }
    return String.valueOf(value);
  }

  public static class Key<R extends ConnectRecord<R>> extends Cast<R> {
    @Override
    protected Schema inputSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object inputValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema schema, Object value) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          schema,
          value,
          record.valueSchema(),
          record.value(),
          record.timestamp()
      );
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends Cast<R> {
    @Override
    protected Schema inputSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object inputValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema schema, Object value) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          schema,
          value,
          record.timestamp()
      );
    }
  }
}
