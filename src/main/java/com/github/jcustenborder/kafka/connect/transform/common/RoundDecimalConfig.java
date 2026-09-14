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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.math.RoundingMode;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RoundDecimalConfig extends AbstractConfig {
  public final List<FieldPath> fields;
  public final int scale;
  public final int precision;
  public final RoundingMode roundingMode;
  public final boolean skipMissingOrNull;

  public static final String FIELDS_CONFIG = "fields";
  static final String FIELDS_DOC = "The fields to round. Fields may be nested using dotted paths, "
      + "array indexes and wildcards, for example `ITEM.PRICE`, `PRICES[*].AMOUNT`, `PRICES[1].AMOUNT` "
      + "or `**.*_AMOUNT`.";

  public static final String SCALE_CONFIG = "scale";
  static final String SCALE_DOC = "The number of decimal places to round to.";

  public static final String PRECISION_CONFIG = "precision";
  static final String PRECISION_DOC = "The value written to the `connect.decimal.precision` schema "
      + "parameter of rounded decimal fields. `0`, the default, leaves the existing precision alone. "
      + "This only affects the schema, never the value.";
  static final int PRECISION_DEFAULT = 0;

  public static final String ROUNDING_MODE_CONFIG = "rounding.mode";
  static final String ROUNDING_MODE_DOC = "The `java.math.RoundingMode` used when discarding "
      + "decimal places.";
  static final String ROUNDING_MODE_DEFAULT = RoundingMode.HALF_UP.name();

  public static final String SKIP_MISSING_OR_NULL_CONFIG = "skip.missing.or.null";
  static final String SKIP_MISSING_OR_NULL_DOC = "How to handle fields that are not present in the "
      + "record or that are null. When true the field is left alone, when false a `DataException` "
      + "is thrown.";
  static final boolean SKIP_MISSING_OR_NULL_DEFAULT = true;

  public RoundDecimalConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.fields = parseFields(getList(FIELDS_CONFIG));
    this.scale = getInt(SCALE_CONFIG);
    this.precision = getInt(PRECISION_CONFIG);
    this.roundingMode = RoundingMode.valueOf(getString(ROUNDING_MODE_CONFIG).toUpperCase(Locale.ROOT));
    this.skipMissingOrNull = getBoolean(SKIP_MISSING_OR_NULL_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(FIELDS_CONFIG, ConfigDef.Type.LIST)
                .documentation(FIELDS_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .validator(new FieldsValidator())
                .build()
        )
        .define(
            ConfigKeyBuilder.of(SCALE_CONFIG, ConfigDef.Type.INT)
                .documentation(SCALE_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .validator(ConfigDef.Range.between(0, 127))
                .build()
        )
        .define(
            ConfigKeyBuilder.of(ROUNDING_MODE_CONFIG, ConfigDef.Type.STRING)
                .documentation(ROUNDING_MODE_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .defaultValue(ROUNDING_MODE_DEFAULT)
                .validator(ConfigDef.ValidString.in(roundingModes()))
                .build()
        )
        .define(
            ConfigKeyBuilder.of(PRECISION_CONFIG, ConfigDef.Type.INT)
                .documentation(PRECISION_DOC)
                .importance(ConfigDef.Importance.LOW)
                .defaultValue(PRECISION_DEFAULT)
                .validator(ConfigDef.Range.between(0, 127))
                .build()
        )
        .define(
            ConfigKeyBuilder.of(SKIP_MISSING_OR_NULL_CONFIG, ConfigDef.Type.BOOLEAN)
                .documentation(SKIP_MISSING_OR_NULL_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .defaultValue(SKIP_MISSING_OR_NULL_DEFAULT)
                .build()
        );
  }

  static List<FieldPath> parseFields(List<String> specs) {
    try {
      return FieldPath.ofAll(specs);
    } catch (Exception ex) {
      throw new ConfigException(FIELDS_CONFIG, specs, ex.getMessage());
    }
  }

  private static String[] roundingModes() {
    RoundingMode[] modes = RoundingMode.values();
    String[] names = new String[modes.length];
    for (int i = 0; i < modes.length; i++) {
      names[i] = modes[i].name();
    }
    return names;
  }

  static class FieldsValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
      if (!(value instanceof List)) {
        return;
      }
      for (Object spec : (List<?>) value) {
        try {
          FieldPath.of((String) spec);
        } catch (Exception ex) {
          throw new ConfigException(name, spec, ex.getMessage());
        }
      }
    }

    @Override
    public String toString() {
      return "Field paths such as ITEM.PRICE, PRICES[*].AMOUNT or **.*_AMOUNT";
    }
  }
}
