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
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@Title("RoundDecimal")
@Description("This transformation rounds numeric fields to a fixed number of decimal places. "
    + "Fields nested inside structs, maps and arrays can be targeted with a field path such as "
    + "`ITEM.PRICE`, `PRICES[*].AMOUNT` or `**.*_AMOUNT`. Records with and without a schema are "
    + "both supported.")
@DocumentationNote("Decimal fields are rewritten with the new scale, so the schema of a schemaful "
    + "record changes. Float, double and numeric string fields keep their original type.")
public abstract class RoundDecimal<R extends ConnectRecord<R>> implements Transformation<R> {

  RoundDecimalConfig config;

  @Override
  public ConfigDef config() {
    return RoundDecimalConfig.config();
  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new RoundDecimalConfig(settings);
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

    SchemaAndValue result = FieldPathUpdater.update(
        inputSchema,
        inputValue,
        this.config.fields,
        this::round,
        this.config.skipMissingOrNull
    );

    if (result.value() == inputValue && Objects.equals(result.schema(), inputSchema)) {
      return record;
    }
    return newRecord(record, result.schema(), result.value());
  }

  protected abstract Schema inputSchema(R record);

  protected abstract Object inputValue(R record);

  protected abstract R newRecord(R record, Schema schema, Object value);

  SchemaAndValue round(Schema schema, Object value) {
    Schema outputSchema = null == schema ? null : roundSchema(schema);
    return new SchemaAndValue(outputSchema, roundValue(value));
  }

  private Schema roundSchema(Schema schema) {
    if (!Decimal.LOGICAL_NAME.equals(schema.name())) {
      return schema;
    }

    Map<String, String> parameters = new LinkedHashMap<>();
    if (null != schema.parameters()) {
      parameters.putAll(schema.parameters());
    }
    parameters.put(Decimal.SCALE_FIELD, Integer.toString(this.config.scale));
    if (this.config.precision > 0) {
      parameters.put(CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(this.config.precision));
    }

    SchemaBuilder builder = SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .version(null == schema.version() ? 1 : schema.version())
        .doc(schema.doc())
        .parameters(parameters);
    if (schema.isOptional()) {
      builder.optional();
    }
    return builder.build();
  }

  private Object roundValue(Object value) {
    if (null == value) {
      return null;
    }
    if (value instanceof BigDecimal) {
      return apply((BigDecimal) value);
    }
    if (value instanceof Double) {
      double input = (Double) value;
      return finite(input) ? apply(BigDecimal.valueOf(input)).doubleValue() : value;
    }
    if (value instanceof Float) {
      float input = (Float) value;
      return finite(input) ? apply(new BigDecimal(Float.toString(input))).floatValue() : value;
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      return value;
    }
    if (value instanceof String) {
      String text = ((String) value).trim();
      try {
        return apply(new BigDecimal(text)).toPlainString();
      } catch (NumberFormatException ex) {
        throw new DataException("Could not round '" + value + "' because it is not a number.");
      }
    }
    throw new DataException("Could not round " + value.getClass().getName() + ".");
  }

  private BigDecimal apply(BigDecimal value) {
    return value.setScale(this.config.scale, this.config.roundingMode);
  }

  private static boolean finite(double value) {
    return !Double.isNaN(value) && !Double.isInfinite(value);
  }

  static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";

  public static class Key<R extends ConnectRecord<R>> extends RoundDecimal<R> {
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

  public static class Value<R extends ConnectRecord<R>> extends RoundDecimal<R> {
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
