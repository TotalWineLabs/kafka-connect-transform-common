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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class Filter<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(Filter.class);

  @Override
  public ConfigDef config() {
    return FilterConfig.config();
  }

  FilterConfig config;

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new FilterConfig(settings);
  }

  @Override
  public void close() {

  }

  R filter(R record, Struct struct) {
    if (struct == null) {
      return handleMissingOrNull(record);
    }

    Map<String, Object> structMap = struct.schema().fields().stream()
        .collect(Collectors.toMap(Field::name, field -> struct.get(field)));
    return applyFilter(record, structMap);
  }

  R filter(R record, Map map) {
    if (map == null) {
      return handleMissingOrNull(record);
    }
    return applyFilter(record, (Map<String, Object>) map);
  }

  R filter(R record, final boolean key) {
    final SchemaAndValue input = key ?
        new SchemaAndValue(record.keySchema(), record.key()) :
        new SchemaAndValue(record.valueSchema(), record.value());
    final R result;
    if (input.schema() != null) {
      if (Schema.Type.STRUCT == input.schema().type()) {
        result = filter(record, (Struct) input.value());
      } else if (Schema.Type.MAP == input.schema().type()) {
        result = filter(record, (Map) input.value());
      } else {
        result = record;
      }
    } else if (input.value() instanceof Map) {
      result = filter(record, (Map) input.value());
    } else {
      result = record;
    }

    return result;
  }

  private R applyFilter(R record, Map<String, Object> data) {
    try {
      // Wrap into a List because JsonPath filter expression only works on lists
      List<Map<String, Object>> dataList = List.of(data);
      // Evaluate the JSONPath expression
      DocumentContext document = JsonPath.parse(dataList);
      boolean isMatch = ((List) document.read(config.filterCondition)).size() > 0;

      // Determine whether to include or exclude the record
      boolean includeRecord = config.filterType.equals("include") ? isMatch : !isMatch;

      return includeRecord ? record : null;

    } catch (PathNotFoundException e) {
      // Field(s) referenced in filter.condition are missing
      return handleMissingOrNull(record);
    } catch (Exception e) {
      log.error("Error evaluating filter condition", e);
      throw new ConnectException("Failed to apply filter condition", e);
    }
  }

  private R handleMissingOrNull(R record) {
    switch (this.config.missingOrNullBehavior) {
      case "fail":
        throw new IllegalStateException("Missing or null field(s) in record for filter condition.");
      case "include":
        return record;
      case "exclude":
      default:
        return null;
    }
  }

  @Title("Filter(Key)")
  @Description("This transformation is used to filter records based on a JsonPath expression.")
  @DocumentationTip("This transformation is used to filter records based on fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends Filter<R> {
    @Override
    public R apply(R r) {
      return filter(r, true);
    }
  }

  @Title("Filter(Value)")
  @Description("This transformation is used to filter records based on a JsonPath expression.")
  @DocumentationTip("This transformation is used to filter records based on fields in the Value of the record.")
  public static class Value<R extends ConnectRecord<R>> extends Filter<R> {
    @Override
    public R apply(R r) {
      return filter(r, false);
    }
  }
}
