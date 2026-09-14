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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

/**
 * Applies a transformation to every field targeted by a {@link FieldPath}, rebuilding the
 * surrounding structs, maps, arrays and (when the record has one) schemas as needed.
 *
 * <p>Both schemaful records ({@link Struct}) and schemaless records ({@link Map}) are supported.
 * Kafka Connect arrays and maps carry a single element schema, so a schemaful record cannot end up
 * with elements of differing schemas. When a targeted update would produce such a structure a
 * {@link DataException} is raised instead of emitting an invalid record.</p>
 */
public final class FieldPathUpdater {

  private FieldPathUpdater() {
  }

  /**
   * Produces the replacement for a single targeted field.
   */
  @FunctionalInterface
  public interface FieldTransform {
    /**
     * @param schema the schema of the targeted field, or null when the record is schemaless.
     * @param value  the current value of the targeted field. May be null when null values are being
     *               skipped, in which case only the returned schema is used.
     * @return the replacement schema and value. The returned schema is ignored when {@code schema}
     * is null, and must be non null otherwise.
     */
    SchemaAndValue apply(Schema schema, Object value);
  }

  /**
   * Applies {@code transform} to every field matched by {@code paths}.
   *
   * @param skipMissingOrNull when true, paths that match nothing and matched fields that are null
   *                          are silently left alone. When false either case raises a
   *                          {@link DataException}.
   * @return the resulting schema and value. The input schema and value are returned unchanged when
   * nothing matched or nothing changed.
   */
  public static SchemaAndValue update(
      Schema schema,
      Object value,
      Collection<FieldPath> paths,
      FieldTransform transform,
      boolean skipMissingOrNull) {
    Schema currentSchema = schema;
    Object currentValue = value;

    for (FieldPath path : paths) {
      Counter counter = new Counter();
      Node node = walk(currentSchema, currentValue, path, 0, transform, skipMissingOrNull, counter);
      if (0 == counter.matches && !skipMissingOrNull) {
        throw new DataException(
            "Field '" + path.spec() + "' was not found and '"
                + "skip.missing.or.null' is false."
        );
      }
      currentSchema = node.schema;
      currentValue = node.value;
    }

    return new SchemaAndValue(currentSchema, currentValue);
  }

  private static Node walk(
      Schema schema,
      Object value,
      FieldPath path,
      int index,
      FieldTransform transform,
      boolean skipMissingOrNull,
      Counter counter) {
    List<FieldPath.Segment> segments = path.segments();

    if (index >= segments.size()) {
      counter.matches++;
      if (null == value && !skipMissingOrNull) {
        throw new DataException(
            "Field '" + path.spec() + "' is null and 'skip.missing.or.null' is false."
        );
      }
      // A null value is still handed to the transform so that it can report the schema the
      // surrounding array, map or struct has to be rebuilt with.
      SchemaAndValue result = transform.apply(schema, value);
      Schema resultSchema = null == schema ? schema : result.schema();
      boolean changed = result.value() != value || !Objects.equals(resultSchema, schema);
      return new Node(resultSchema, result.value(), changed);
    }

    FieldPath.Segment segment = segments.get(index);

    if (segment instanceof FieldPath.RecursiveSegment) {
      // '**' matches zero levels (continue with the rest of the path here) and any number of
      // deeper levels (re-apply '**' to every child).
      Node here = walk(schema, value, path, index + 1, transform, skipMissingOrNull, counter);
      Node deeper = descend(here.schema, here.value, path, index, transform, skipMissingOrNull, counter);
      return new Node(deeper.schema, deeper.value, here.changed || deeper.changed);
    }

    if (segment instanceof FieldPath.IndexSegment) {
      FieldPath.IndexSegment indexSegment = (FieldPath.IndexSegment) segment;
      return updateArray(
          schema, value, path, index + 1, indexSegment::matches, transform, skipMissingOrNull, counter
      );
    }

    FieldPath.NameSegment nameSegment = (FieldPath.NameSegment) segment;
    if (value instanceof Struct) {
      return updateStruct(
          schema, (Struct) value, path, index + 1, nameSegment::matches, transform, skipMissingOrNull, counter
      );
    }
    if (value instanceof Map) {
      return updateMap(
          schema, value, path, index + 1, nameSegment::matches, transform, skipMissingOrNull, counter
      );
    }
    return new Node(schema, value, false);
  }

  /**
   * Re-applies the '**' segment at {@code index} to every child of the current value.
   */
  private static Node descend(
      Schema schema,
      Object value,
      FieldPath path,
      int index,
      FieldTransform transform,
      boolean skipMissingOrNull,
      Counter counter) {
    if (value instanceof Struct) {
      return updateStruct(schema, (Struct) value, path, index, name -> true, transform, skipMissingOrNull, counter);
    }
    if (value instanceof Map) {
      return updateMap(schema, value, path, index, name -> true, transform, skipMissingOrNull, counter);
    }
    if (value instanceof List) {
      return updateArray(schema, value, path, index, candidate -> true, transform, skipMissingOrNull, counter);
    }
    return new Node(schema, value, false);
  }

  private static Node updateStruct(
      Schema schema,
      Struct struct,
      FieldPath path,
      int nextIndex,
      Predicate<String> selector,
      FieldTransform transform,
      boolean skipMissingOrNull,
      Counter counter) {
    Schema structSchema = null != schema && Schema.Type.STRUCT == schema.type() ? schema : struct.schema();

    Map<String, Node> updates = null;
    boolean schemaChanged = false;
    for (Field field : structSchema.fields()) {
      if (!selector.test(field.name())) {
        continue;
      }
      Node child = walk(
          field.schema(), struct.get(field.name()), path, nextIndex, transform, skipMissingOrNull, counter
      );
      if (!child.changed) {
        continue;
      }
      if (null == updates) {
        updates = new LinkedHashMap<>();
      }
      updates.put(field.name(), child);
      schemaChanged |= !Objects.equals(child.schema, field.schema());
    }

    if (null == updates) {
      return new Node(schema, struct, false);
    }

    Schema outputSchema = structSchema;
    if (schemaChanged) {
      SchemaBuilder builder = SchemaBuilder.struct()
          .name(structSchema.name())
          .doc(structSchema.doc())
          .version(structSchema.version());
      if (structSchema.isOptional()) {
        builder.optional();
      }
      if (null != structSchema.parameters() && !structSchema.parameters().isEmpty()) {
        builder.parameters(structSchema.parameters());
      }
      for (Field field : structSchema.fields()) {
        Node update = updates.get(field.name());
        builder.field(field.name(), null == update ? field.schema() : update.schema);
      }
      outputSchema = builder.build();
    }

    Struct outputStruct = new Struct(outputSchema);
    for (Field field : outputSchema.fields()) {
      Node update = updates.get(field.name());
      outputStruct.put(field.name(), null == update ? struct.get(field.name()) : update.value);
    }
    return new Node(outputSchema, outputStruct, true);
  }

  @SuppressWarnings("unchecked")
  private static Node updateMap(
      Schema schema,
      Object value,
      FieldPath path,
      int nextIndex,
      Predicate<String> selector,
      FieldTransform transform,
      boolean skipMissingOrNull,
      Counter counter) {
    Map<Object, Object> map = (Map<Object, Object>) value;
    boolean schemaful = null != schema && Schema.Type.MAP == schema.type();
    Schema valueSchema = schemaful ? schema.valueSchema() : null;

    Map<Object, Node> updates = null;
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      if (!(entry.getKey() instanceof String) || !selector.test((String) entry.getKey())) {
        continue;
      }
      Node child = walk(valueSchema, entry.getValue(), path, nextIndex, transform, skipMissingOrNull, counter);
      if (!child.changed) {
        continue;
      }
      if (null == updates) {
        updates = new LinkedHashMap<>();
      }
      updates.put(entry.getKey(), child);
    }

    if (null == updates) {
      return new Node(schema, map, false);
    }

    Map<Object, Object> outputMap = new LinkedHashMap<>(map);
    for (Map.Entry<Object, Node> update : updates.entrySet()) {
      outputMap.put(update.getKey(), update.getValue().value);
    }

    Schema outputSchema = schema;
    if (schemaful) {
      Schema elementSchema = commonSchema(
          valueSchema, updates.values(), updates.size() < map.size(), path, "map"
      );
      if (!Objects.equals(elementSchema, valueSchema)) {
        SchemaBuilder builder = SchemaBuilder.map(schema.keySchema(), elementSchema)
            .name(schema.name())
            .doc(schema.doc())
            .version(schema.version());
        if (schema.isOptional()) {
          builder.optional();
        }
        outputSchema = builder.build();
      }
    }
    return new Node(outputSchema, outputMap, true);
  }

  @SuppressWarnings("unchecked")
  private static Node updateArray(
      Schema schema,
      Object value,
      FieldPath path,
      int nextIndex,
      IntPredicate selector,
      FieldTransform transform,
      boolean skipMissingOrNull,
      Counter counter) {
    if (!(value instanceof List)) {
      return new Node(schema, value, false);
    }
    List<Object> list = (List<Object>) value;
    boolean schemaful = null != schema && Schema.Type.ARRAY == schema.type();
    Schema valueSchema = schemaful ? schema.valueSchema() : null;

    Map<Integer, Node> updates = null;
    for (int i = 0; i < list.size(); i++) {
      if (!selector.test(i)) {
        continue;
      }
      Node child = walk(valueSchema, list.get(i), path, nextIndex, transform, skipMissingOrNull, counter);
      if (!child.changed) {
        continue;
      }
      if (null == updates) {
        updates = new LinkedHashMap<>();
      }
      updates.put(i, child);
    }

    if (null == updates) {
      return new Node(schema, list, false);
    }

    List<Object> outputList = new ArrayList<>(list);
    for (Map.Entry<Integer, Node> update : updates.entrySet()) {
      outputList.set(update.getKey(), update.getValue().value);
    }

    Schema outputSchema = schema;
    if (schemaful) {
      Schema elementSchema = commonSchema(
          valueSchema, updates.values(), updates.size() < list.size(), path, "array"
      );
      if (!Objects.equals(elementSchema, valueSchema)) {
        SchemaBuilder builder = SchemaBuilder.array(elementSchema)
            .name(schema.name())
            .doc(schema.doc())
            .version(schema.version());
        if (schema.isOptional()) {
          builder.optional();
        }
        outputSchema = builder.build();
      }
    }
    return new Node(outputSchema, outputList, true);
  }

  /**
   * Kafka Connect arrays and maps declare a single element schema, so every element has to end up
   * with the same schema.
   */
  private static Schema commonSchema(
      Schema originalSchema,
      Collection<Node> updates,
      boolean hasUntouchedElements,
      FieldPath path,
      String container) {
    Schema result = hasUntouchedElements ? originalSchema : null;
    for (Node update : updates) {
      if (null == result) {
        result = update.schema;
      } else if (!Objects.equals(result, update.schema)) {
        throw new DataException(
            "Field '" + path.spec() + "' would give the elements of a schemaful " + container
                + " different schemas, which Kafka Connect does not support. Target every element "
                + "(for example 'FIELD[*].CHILD') so they all change together."
        );
      }
    }
    return null == result ? originalSchema : result;
  }

  private static final class Node {
    final Schema schema;
    final Object value;
    final boolean changed;

    Node(Schema schema, Object value, boolean changed) {
      this.schema = schema;
      this.value = value;
      this.changed = changed;
    }
  }

  private static final class Counter {
    int matches;
  }
}
