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
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CastConfig extends AbstractConfig {
  public final List<CastSpec> specs;
  public final boolean skipMissingOrNull;

  public static final String SPEC_CONFIG = "spec";
  static final String SPEC_DOC = "List of fields and the type to cast them to, of the form "
      + "`field1:type1,field2:type2`. A bare `type` casts the entire key or value. Fields may be "
      + "nested using dotted paths, array indexes and wildcards, for example "
      + "`ITEM.ATTRIBUTES.IS_DIGITAL_GOOD:boolean`, `RATINGS[*].VINTAGE:int32`, "
      + "`RATINGS[1].VINTAGE:int32` or `**.IS_*:boolean`. Valid types are "
      + "`int8`, `int16`, `int32`, `int64`, `float32`, `float64`, `boolean` and `string`.";

  public static final String SKIP_MISSING_OR_NULL_CONFIG = "skip.missing.or.null";
  static final String SKIP_MISSING_OR_NULL_DOC = "How to handle fields that are not present in the "
      + "record or that are null. When true the field is left alone, when false a "
      + "`DataException` is thrown.";
  static final boolean SKIP_MISSING_OR_NULL_DEFAULT = true;

  private static final Map<String, Schema.Type> TYPES;

  static {
    Map<String, Schema.Type> types = new LinkedHashMap<>();
    types.put("int8", Schema.Type.INT8);
    types.put("int16", Schema.Type.INT16);
    types.put("int32", Schema.Type.INT32);
    types.put("int64", Schema.Type.INT64);
    types.put("float32", Schema.Type.FLOAT32);
    types.put("float64", Schema.Type.FLOAT64);
    types.put("boolean", Schema.Type.BOOLEAN);
    types.put("string", Schema.Type.STRING);
    TYPES = Collections.unmodifiableMap(types);
  }

  public CastConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.specs = parseSpecs(getList(SPEC_CONFIG));
    this.skipMissingOrNull = getBoolean(SKIP_MISSING_OR_NULL_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(SPEC_CONFIG, ConfigDef.Type.LIST)
                .documentation(SPEC_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .validator(new SpecValidator())
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

  static List<CastSpec> parseSpecs(List<String> entries) {
    List<CastSpec> specs = new ArrayList<>(entries.size());
    for (String entry : entries) {
      specs.add(parseSpec(entry));
    }
    return Collections.unmodifiableList(specs);
  }

  static CastSpec parseSpec(String entry) {
    String text = null == entry ? "" : entry.trim();
    int separator = text.lastIndexOf(':');
    String field = separator < 0 ? "" : text.substring(0, separator).trim();
    String type = separator < 0 ? text : text.substring(separator + 1).trim();

    Schema.Type schemaType = TYPES.get(type.toLowerCase(Locale.ROOT));
    if (null == schemaType) {
      throw new ConfigException(
          SPEC_CONFIG,
          entry,
          "'" + type + "' is not a supported type. Valid types are " + TYPES.keySet() + "."
      );
    }
    try {
      return new CastSpec(FieldPath.of(field), schemaType);
    } catch (Exception ex) {
      throw new ConfigException(SPEC_CONFIG, entry, ex.getMessage());
    }
  }

  public static class CastSpec {
    public final FieldPath path;
    public final Schema.Type type;

    CastSpec(FieldPath path, Schema.Type type) {
      this.path = path;
      this.type = type;
    }

    @Override
    public String toString() {
      return this.path.spec() + ":" + this.type;
    }
  }

  static class SpecValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
      if (!(value instanceof List)) {
        return;
      }
      for (Object entry : (List<?>) value) {
        parseSpec((String) entry);
      }
    }

    @Override
    public String toString() {
      return "Entries of the form field:type where type is one of " + TYPES.keySet();
    }
  }
}
