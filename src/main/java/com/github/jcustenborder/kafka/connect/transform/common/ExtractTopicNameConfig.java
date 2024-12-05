/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class ExtractTopicNameConfig extends AbstractConfig {
  public final String field;
  public final String fieldFormat;
  public final Boolean skipMissingOrNull;

  public static final String FIELD_FORMAT_JSON_PATH = "JSON_PATH";
  public static final String FIELD_FORMAT_PLAIN = "PLAIN";

  public static final String FIELD_CONFIG = "field";
  public static final String FIELD_DOC = "Field name to use as the topic name.";

  public static final String FIELD_FORMAT_CONFIG = "field.format";
  public static final String FIELD_FORMAT_DOC = "Specify field path format. Currently two formats are supported: JSON_PATH and PLAIN.";

  public static final String SKIP_MISSING_OR_NULL_CONFIG = "skip.missing.or.null";
  public static final String SKIP_MISSING_OR_NULL_DOC = "How to handle missing fields and null fields, keys, and values.";  


  public ExtractTopicNameConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.field = getString(FIELD_CONFIG);
    this.fieldFormat = getString(FIELD_FORMAT_CONFIG);
    this.skipMissingOrNull = getBoolean(SKIP_MISSING_OR_NULL_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, FIELD_DOC)
        .define(FIELD_FORMAT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, FIELD_FORMAT_DOC)
        .define(SKIP_MISSING_OR_NULL_CONFIG, ConfigDef.Type.BOOLEAN, ConfigDef.Importance.LOW, SKIP_MISSING_OR_NULL_DOC);
  }
}
