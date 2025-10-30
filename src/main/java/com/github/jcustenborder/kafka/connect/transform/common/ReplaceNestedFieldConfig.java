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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ReplaceNestedFieldConfig extends AbstractConfig {

  public static final String FIELDS_CONF = "fields";
  static final String FIELDS_DOC = "Comma-separated list of nested field paths to replace with null. " +
      "Use dot notation to specify nested fields (e.g., 'shipping.shipping_first_name,delivery.delivery_last_name').";

  public final Set<String> fieldsToReplace;

  public ReplaceNestedFieldConfig(Map<String, ?> parsedConfig) {
    super(config(), parsedConfig);
    List<String> fieldsList = getList(FIELDS_CONF);
    this.fieldsToReplace = new HashSet<>(fieldsList);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(FIELDS_CONF, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, 
               ConfigDef.Importance.HIGH, FIELDS_DOC);
  }
}