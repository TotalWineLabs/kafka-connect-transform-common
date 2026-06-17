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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.data.Struct;
import com.jayway.jsonpath.JsonPath;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@Title("ExtractTopicName")
@Description("Extract data from a message and use it as the topic name.")
@DocumentationTip("Extract data from a message and use it as the topic name.")
public abstract class ExtractTopicName<R extends ConnectRecord<R>> implements Transformation<R> {

  ExtractTopicNameConfig config;

  @Override
  public ConfigDef config() {
    return ExtractTopicNameConfig.config();
  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new ExtractTopicNameConfig(settings);
  }

  @Override
  public R apply(R record) {
    Object target = extractTarget(record);

    if (target == null) {
      if (this.config.skipMissingOrNull) {
        return record; // Pass through unchanged
      }
      throw new DataException("Target (key, value, or header) is null and 'skip.missing.or.null' is false");
    }

    Object extractedField = (this.config.field == null) ? target : extractFieldWithJsonPath(target);

    if (extractedField == null) {
      if (this.config.skipMissingOrNull) {
        return record; // Pass through unchanged
      }
      throw new DataException("Field is null and 'skip.missing.or.null' is false");
    }

    if (!(extractedField instanceof String)) {
      if (this.config.skipMissingOrNull) {
        return record; // Pass through unchanged
      }
      throw new DataException("Extracted field is not a String");
    }

    String newTopic = (String) extractedField;
    return record.newRecord(
        newTopic,
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        record.value(),
        record.timestamp());
  }

  @Override
  public void close() {

  }

  private Object extractFieldWithJsonPath(Object target) {
    if (ExtractTopicNameConfig.FIELD_FORMAT_JSON_PATH.equals(this.config.fieldFormat)) {
      try {
        if (target instanceof Map) {
          ObjectMapper mapper = new ObjectMapper();
          String json = mapper.writeValueAsString(target);
          return JsonPath.read(json, this.config.field); // Extract using JsonPath
        } else {
          return JsonPath.read(target, this.config.field); // Extract using JsonPath
        }
      } catch (Exception e) {
        if (this.config.skipMissingOrNull) {
          return null;
        }
        throw new DataException("Error reading JSON path: " + this.config.field, e);
      }
    } else if (ExtractTopicNameConfig.FIELD_FORMAT_PLAIN.equals(this.config.fieldFormat)
        || this.config.fieldFormat == null) {
      if (target instanceof Struct struct) {
        return struct.get(this.config.field);
      } else if (target instanceof Map<?, ?> map) {
        return map.get(this.config.field);
      }
    }

    throw new DataException("Unsupported field format: " + this.config.fieldFormat);
  }

  protected abstract Object extractTarget(R record);

  public static class Key<R extends ConnectRecord<R>> extends ExtractTopicName<R> {
    @Override
    protected Object extractTarget(R record) {
      return record.key();
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends ExtractTopicName<R> {
    @Override
    protected Object extractTarget(R record) {
      return record.value();
    }
  }

  public static class Header<R extends ConnectRecord<R>> extends ExtractTopicName<R> {
    @Override
    protected Object extractTarget(R record) {
      if (record.headers() == null) {
        return null;
      }
      if (this.config.field == null) {
        throw new DataException("Field configuration must be specified for header extraction");
      }
      return record.headers().lastWithName(this.config.field).value();
    }
  }
}
