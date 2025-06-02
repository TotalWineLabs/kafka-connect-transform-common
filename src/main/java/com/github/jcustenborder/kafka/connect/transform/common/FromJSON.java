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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Struct;
import com.jayway.jsonpath.JsonPath;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@Title("FromJSON")
@Description("Extracts a JSON-encoded string from a specified field using a JSON path, parses it, and inserts the resulting structured object into the payload under the same or new field.")
@DocumentationTip("Use this SMT when dealing with nested JSON stored as strings and you want to promote them to structured fields")
public abstract class FromJSON<R extends ConnectRecord<R>> implements Transformation<R> {

  FromJSONConfig config;

  @Override
  public ConfigDef config() {
    return FromJSONConfig.config();
  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new FromJSONConfig(settings);
  }

  @Override
  public R apply(R record) {

    /*  
      1. Extract the field value using the specified format (JSON_PATH or PLAIN)
      2. If the field value is null or not a string, handle according to the configuration
      4. Parse the field value into a structured object if it is a valid JSON string, otherwise handle according to the configuration
      5. Replace the original field with the parsed object and return the modified record
    */

    Object target = extractTarget(record);

    if (target == null) {
      if (this.config.skipMissingOrNull) {
        return record; // Pass through unchanged
      }
      throw new DataException("Target (key or value) is null and 'skip.missing.or.null' is false");
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

    Object parsedObject;
    try {
      ObjectMapper mapper = new ObjectMapper();
      parsedObject = mapper.readValue((String) extractedField, Map.class);
    } catch (Exception e) {
      if (this.config.skipMissingOrNull) {
        return record; // Pass through unchanged
      }
      throw new DataException("Failed to parse extracted field as JSON", e);
    }

    // Replace the original field with the parsed object
    Object newTarget;
    if (target instanceof Struct) {
      Struct struct = (Struct) target;
      Struct updatedStruct = new Struct(struct.schema());
      // Copy all fields first
      for (org.apache.kafka.connect.data.Field f : struct.schema().fields()) {
        updatedStruct.put(f.name(), struct.get(f));
      }
      // Handle JSON path for replacementField (only top-level for Struct)
      String path = this.config.replacementField;
      if (path.startsWith("$")) {
        path = path.substring(1);
        if (path.startsWith(".")) {
          path = path.substring(1);
        }
      }
      String[] pathParts = path.split("\\.");
      if (pathParts.length == 1) {
        updatedStruct.put(pathParts[0], parsedObject);
      } else {
        throw new DataException("Nested JSON path for replacementField is not supported for Struct type");
      }
      newTarget = updatedStruct;
    } else if (target instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) target;
      // Deep copy to ensure all nested maps are mutable
      Map<String, Object> updatedMap = deepCopyMap(map);

      // Handle JSON path for replacementField
      String path = this.config.replacementField;
      if (path.startsWith("$")) {
        path = path.substring(1);
        if (path.startsWith(".")) {
          path = path.substring(1);
        }
      }
      String[] pathParts = path.split("\\.");
      Map<String, Object> current = updatedMap;
      for (int i = 0; i < pathParts.length - 1; i++) {
        Object next = current.get(pathParts[i]);
        if (!(next instanceof Map)) {
          next = new java.util.HashMap<String, Object>();
          current.put(pathParts[i], next);
        } else if (!(next instanceof java.util.HashMap)) {
          // Convert to mutable map if needed
          next = new java.util.HashMap<>((Map<String, Object>) next);
          current.put(pathParts[i], next);
        }
        current = (Map<String, Object>) next;
      }
      current.put(pathParts[pathParts.length - 1], parsedObject);

      newTarget = updatedMap;
    } else {
      // If target is not Struct or Map, cannot update field
      if (this.config.skipMissingOrNull) {
        return record;
      }
      throw new DataException("Target is not a Struct or Map, cannot update field");
    }

    // Build new record with updated key or value
    if (this instanceof FromJSON.Key) {
      return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        newTarget,
        record.valueSchema(),
        record.value(),
        record.timestamp()
      );
    } else {
      return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        newTarget,
        record.timestamp()
      );
    }
  }

  @Override
  public void close() {

  }

  private Object extractFieldWithJsonPath(Object target) {
    if (FromJSONConfig.FIELD_FORMAT_JSON_PATH.equals(this.config.fieldFormat)) {
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
    } else if (FromJSONConfig.FIELD_FORMAT_PLAIN.equals(this.config.fieldFormat)
        || this.config.fieldFormat == null) {
      if (target instanceof Struct) {
        return ((Struct) target).get(this.config.field);
      } else if (target instanceof Map) {
        return ((Map<?, ?>) target).get(this.config.field);
      }
    }

    throw new DataException("Unsupported field format: " + this.config.fieldFormat);
  }

  protected abstract Object extractTarget(R record);

  public static class Key<R extends ConnectRecord<R>> extends FromJSON<R> {
    @Override
    protected Object extractTarget(R record) {
      return record.key();
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends FromJSON<R> {
    @Override
    protected Object extractTarget(R record) {
      return record.value();
    }
  }

  /**
   * Deep copy a map and all nested maps to ensure mutability.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> deepCopyMap(Map<String, Object> original) {
    Map<String, Object> copy = new java.util.HashMap<>();
    for (Map.Entry<String, Object> entry : original.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {
        value = deepCopyMap((Map<String, Object>) value);
      }
      copy.put(entry.getKey(), value);
    }
    return copy;
  }
}
