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
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaBuilder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public abstract class ReplaceNestedField<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final String PURPOSE = "nested field replacement";
  
  private Set<String> fieldsToReplace;
  private Cache<Schema, Schema> schemaUpdateCache;
  
  @Override
  public ConfigDef config() {
    return ReplaceNestedFieldConfig.config();
  }
  
  ReplaceNestedFieldConfig config;
  
  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new ReplaceNestedFieldConfig(settings);
    this.fieldsToReplace = this.config.fieldsToReplace;
    this.schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }
  
  @Override
  public void close() {
    this.schemaUpdateCache = null;
  }
  
  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct inputStruct) {
    if (inputStruct == null) {
      return new SchemaAndValue(inputSchema, null);
    }
    
    Schema updatedSchema = schemaUpdateCache.get(inputSchema);
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(inputSchema);
      schemaUpdateCache.put(inputSchema, updatedSchema);
    }
    
    final Struct updatedStruct = new Struct(updatedSchema);
    copyStructWithReplacements(inputStruct, updatedStruct, "", inputSchema, updatedSchema);
    
    return new SchemaAndValue(updatedSchema, updatedStruct);
  }
  
  @Override
  protected SchemaAndValue processMap(R record, Map<String, Object> input) {
    if (input == null) {
      return new SchemaAndValue(null, null);
    }
    
    final Map<String, Object> updatedMap = new LinkedHashMap<>(input.size());
    copyMapWithReplacements(input, updatedMap, "");
    
    return new SchemaAndValue(null, updatedMap);
  }
  
  /**
   * Creates an updated schema where fields to be replaced are made optional.
   */
  private Schema makeUpdatedSchema(Schema schema) {
    return makeUpdatedSchemaInternal(schema, "");
  }
  
  private Schema makeUpdatedSchemaInternal(Schema schema, String pathPrefix) {
    if (schema.type() != Schema.Type.STRUCT) {
      return schema;
    }
    
    org.apache.kafka.connect.data.SchemaBuilder builder = org.apache.kafka.connect.data.SchemaBuilder.struct()
        .name(schema.name())
        .version(schema.version())
        .doc(schema.doc());
    
    if (schema.parameters() != null) {
      builder.parameters(schema.parameters());
    }
    
    for (Field field : schema.fields()) {
      String currentPath = pathPrefix.isEmpty() ? field.name() : pathPrefix + "." + field.name();
      Schema fieldSchema = field.schema();
      
      if (shouldReplaceField(currentPath)) {
        // Make the field optional since it will be replaced with null, but preserve the type
        org.apache.kafka.connect.data.SchemaBuilder fieldBuilder = new org.apache.kafka.connect.data.SchemaBuilder(fieldSchema.type())
            .optional();
            
        if (fieldSchema.name() != null) {
          fieldBuilder.name(fieldSchema.name());
        }
        if (fieldSchema.version() != null) {
          fieldBuilder.version(fieldSchema.version());
        }
        if (fieldSchema.doc() != null) {
          fieldBuilder.doc(fieldSchema.doc());
        }
        if (fieldSchema.parameters() != null) {
          fieldBuilder.parameters(fieldSchema.parameters());
        }
        
        builder.field(field.name(), fieldBuilder.build());
      } else if (fieldSchema.type() == Schema.Type.STRUCT) {
        // Recursively process nested struct schemas
        Schema updatedNestedSchema = makeUpdatedSchemaInternal(fieldSchema, currentPath);
        builder.field(field.name(), updatedNestedSchema);
      } else {
        // Keep the field as-is
        builder.field(field.name(), fieldSchema);
      }
    }
    
    return builder.build();
  }
  
  /**
   * Recursively copies a Struct while replacing specified nested fields with null.
   */
  private void copyStructWithReplacements(Struct source, Struct target, String pathPrefix, 
                                        Schema sourceSchema, Schema targetSchema) {
    for (Field field : sourceSchema.fields()) {
      String currentPath = pathPrefix.isEmpty() ? field.name() : pathPrefix + "." + field.name();
      Object fieldValue = source.get(field.name());
      
      if (shouldReplaceField(currentPath)) {
        target.put(field.name(), null);
      } else if (fieldValue instanceof Struct) {
        // Recursively process nested structs
        Struct nestedSource = (Struct) fieldValue;
        Struct nestedTarget = new Struct(field.schema());
        copyStructWithReplacements(nestedSource, nestedTarget, currentPath, 
                                 field.schema(), field.schema());
        target.put(field.name(), nestedTarget);
      } else {
        // Copy the field value as-is
        target.put(field.name(), fieldValue);
      }
    }
  }
  
  /**
   * Recursively copies a Map while replacing specified nested fields with null.
   */
  @SuppressWarnings("unchecked")
  private void copyMapWithReplacements(Map<String, Object> source, Map<String, Object> target, String pathPrefix) {
    for (Map.Entry<String, Object> entry : source.entrySet()) {
      String fieldName = entry.getKey();
      String currentPath = pathPrefix.isEmpty() ? fieldName : pathPrefix + "." + fieldName;
      Object fieldValue = entry.getValue();
      
      if (shouldReplaceField(currentPath)) {
        target.put(fieldName, null);
      } else if (fieldValue instanceof Map) {
        // Recursively process nested maps
        Map<String, Object> nestedSource = (Map<String, Object>) fieldValue;
        Map<String, Object> nestedTarget = new LinkedHashMap<>();
        copyMapWithReplacements(nestedSource, nestedTarget, currentPath);
        target.put(fieldName, nestedTarget);
      } else {
        // Copy the field value as-is
        target.put(fieldName, fieldValue);
      }
    }
  }
  
  /**
   * Checks if a field path should be replaced with null.
   */
  private boolean shouldReplaceField(String fieldPath) {
    return fieldsToReplace.contains(fieldPath);
  }
  
  @Title("ReplaceNestedField(Key)")
  @Description("This transformation is used to replace nested fields in the key of an input struct with null values based on field paths.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends ReplaceNestedField<R> {
    
    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.keySchema(), r.key());
      
      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          transformed.schema(),
          transformed.value(),
          r.valueSchema(),
          r.value(),
          r.timestamp()
      );
    }
  }
  
  @Title("ReplaceNestedField(Value)")
  @Description("This transformation is used to replace nested fields in the value of an input struct with null values based on field paths.")
  @DocumentationTip("This transformation is used to manipulate fields in the Value of the record.")
  public static class Value<R extends ConnectRecord<R>> extends ReplaceNestedField<R> {
    
    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.valueSchema(), r.value());
      
      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          transformed.schema(),
          transformed.value(),
          r.timestamp()
      );
    }
  }
}