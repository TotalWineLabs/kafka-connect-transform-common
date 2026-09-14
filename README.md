# Introduction
[Documentation](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common) | [Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-transform-common)


This project contains common transformations for every day use cases with Kafka Connect.

# Installation

## Confluent Hub

The following command can be used to install the plugin directly from the Confluent Hub using the
[Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html).

```bash
confluent-hub install jcustenborder/kafka-connect-transform-common:latest
```

## Manually

The zip file that is deployed to the [Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-transform-common) is available under
`target/components/packages/`. You can manually extract this zip file which includes all dependencies. All the dependencies
that are required to deploy the plugin are under `target/kafka-connect-target` as well. Make sure that you include all the dependencies that are required
to run the plugin.

1. Create a directory under the `plugin.path` on your Connect worker.
2. Copy all of the dependencies under the newly created subdirectory.
3. Restart the Connect worker.


# Transformations
## [BytesToString](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/BytesToString.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Value
```


### Configuration

#### General


##### `charset`

The charset to use when creating the output string.

*Importance:* HIGH

*Type:* STRING

*Default Value:* UTF-8



##### `fields`

The fields to transform.

*Importance:* HIGH

*Type:* LIST




## [ChangeCase](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ChangeCase.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Value
```


### Configuration

#### General


##### `from`

The format to move from 

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``



##### `to`



*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``




## [ChangeTopicCase](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ChangeTopicCase.html)

```
com.github.jcustenborder.kafka.connect.transform.common.ChangeTopicCase
```

This transformation is used to change the case of a topic.

[✍️ Example](https://rmoff.net/2020/12/23/twelve-days-of-smt-day-12-community-transformations/#_change_the_topic_case) / [🎥 Video](https://www.youtube.com/watch?v=Z7k_6vGRrkc&t=274s)

### Tip

This transformation will convert a topic name like 'TOPIC_NAME' to `topicName`, or `topic_name`.
### Configuration

#### General


##### `from`

The format of the incoming topic name. `LOWER_CAMEL` = Java variable naming convention, e.g., "lowerCamel". `LOWER_HYPHEN` = Hyphenated variable naming convention, e.g., "lower-hyphen". `LOWER_UNDERSCORE` = C++ variable naming convention, e.g., "lower_underscore". `UPPER_CAMEL` = Java and C++ class naming convention, e.g., "UpperCamel". `UPPER_UNDERSCORE` = Java and C++ constant naming convention, e.g., "UPPER_UNDERSCORE".

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``



##### `to`

The format of the outgoing topic name. `LOWER_CAMEL` = Java variable naming convention, e.g., "lowerCamel". `LOWER_HYPHEN` = Hyphenated variable naming convention, e.g., "lower-hyphen". `LOWER_UNDERSCORE` = C++ variable naming convention, e.g., "lower_underscore". `UPPER_CAMEL` = Java and C++ class naming convention, e.g., "UpperCamel". `UPPER_UNDERSCORE` = Java and C++ constant naming convention, e.g., "UPPER_UNDERSCORE".

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``




## [ExtractNestedField](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ExtractNestedField.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Value
```


### Configuration

#### General


##### `input.inner.field.name`

The field on the child struct containing the field to be extracted. For example if you wanted the extract `address.state` you would use `state`.

*Importance:* HIGH

*Type:* STRING



##### `input.outer.field.name`

The field on the parent struct containing the child struct. For example if you wanted the extract `address.state` you would use `address`.

*Importance:* HIGH

*Type:* STRING



##### `output.field.name`

The field to place the extracted value into.

*Importance:* HIGH

*Type:* STRING




## [ExtractTimestamp](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ExtractTimestamp.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Value
```

This transformation is used to use a field from the input data to override the timestamp for the record.

[✍️ Example](https://rmoff.net/2020/12/23/twelve-days-of-smt-day-12-community-transformations/#_add_the_timestamp_of_a_field_to_the_topic_name) / [🎥 Video](https://www.youtube.com/watch?v=Z7k_6vGRrkc&t=430s)


### Configuration

#### General


##### `field.name`

The field to pull the timestamp from. This must be an int64 or a timestamp.

*Importance:* HIGH

*Type:* STRING




## [HeaderToField](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/HeaderToField.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.HeaderToField$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.HeaderToField$Value
```

### Configuration

#### General

##### `header.mappings`

The mapping of the header to the field in the message.

*Importance:* HIGH

*Type:* LIST

## [FromJSON](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/FromJSON.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.FromJSON$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.FromJSON$Value
```

This transformation extracts a JSON-encoded string from a specified field (using a JSON path or plain field name), parses it, and inserts the resulting structured object into the payload under the same or a new field (also specified as a JSON path).

### Configuration

#### General

##### `field`

The field or JSON path to extract the JSON string from. If using JSON path, must start with `$`.

*Importance:* MEDIUM

*Type:* STRING

##### `replacement.field`

The field or JSON path where the parsed object will be inserted. If using JSON path, must start with `$`. Supports nested paths for Map values.

*Importance:* LOW

*Type:* STRING

##### `field.format`

Specify field path format. Supported values: `JSON_PATH` or `PLAIN`. If set to `JSON_PATH`, the transformer will interpret the field as a JSON path. If left blank or set to `PLAIN`, the transformer will treat the field as a simple field name.

*Importance:* MEDIUM

*Type:* STRING

##### `skip.missing.or.null`

How to handle missing or null fields. If true, records with missing/null fields are passed through unchanged. If false, such records will cause an exception.

*Importance:* LOW

*Type:* BOOLEAN

### Example

Suppose you have a record value like:

```json
{
  "after": {
    "item_location_key": 34745645,
    "item_location": "{\"headers\":{\"source\":\"LMA\"},\"key\":\"4565434\",\"value\":{\"location\":{\"itemlocation\":{\"ItemLocationKey\":4565434}}}}"
  }
}
```

With the following configuration:

```json
{
  "field": "$.after.item_location",
  "replacement.field": "$.after.item_location",
  "field.format": "JSON_PATH",
  "skip.missing.or.null": true
}
```

After transformation, the `item_location` field will be a structured object (Map) instead of a JSON string.


## [TimestampConverter](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/TimestampConverter.html)

```
com.github.jcustenborder.kafka.connect.transform.common.TimestampConverter
```

This transformation converts timestamp fields using top-level or dotted nested field paths (for example, `start_date` or `after.start_date`).

### Note

For schemaful records (`Struct`), conversions that would change the field type are skipped to preserve schema compatibility. For schemaless records (`Map`), type-changing conversion is supported. String formatting/parsing is done in UTC.

### Configuration

#### General


##### `field`

The field path to convert. Supports top-level and dotted nested paths.

*Importance:* HIGH

*Type:* STRING



##### `target.type`

The target type for conversion.

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``string``, ``unix``, ``Date``, ``Time``, ``Timestamp``



##### `format`

The date format pattern used when converting to or from string values.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* 



##### `unix.precision`

The unix precision to use for unix target type conversions.

*Importance:* LOW

*Type:* STRING

*Default Value:* milliseconds

*Validator:* Matches: ``milliseconds``, ``seconds``, ``microseconds``, ``nanoseconds``




## [NormalizeSchema](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/NormalizeSchema.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.NormalizeSchema$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.NormalizeSchema$Value
```

This transformation is used to convert older schema versions to the latest schema version. This works by keying all of the schemas that are coming into the transformation by their schema name and comparing the version() of the schema. The latest version of a schema will be used. Schemas are discovered as the flow through the transformation. The latest version of a schema is what is used.
### Configuration



## [PatternFilter](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/PatternFilter.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.PatternFilter$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.PatternFilter$Value
```


### Configuration

#### General


##### `pattern`

The regex to test the message with. 

*Importance:* HIGH

*Type:* STRING

*Validator:* com.github.jcustenborder.kafka.connect.utils.config.validators.PatternValidator@4170ee0f



##### `fields`

The fields to transform.

*Importance:* HIGH

*Type:* LIST




## [PatternRename](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/PatternRename.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Value
```


### Configuration

#### General


##### `field.pattern`



*Importance:* HIGH

*Type:* STRING



##### `field.replacement`



*Importance:* HIGH

*Type:* STRING



##### `field.pattern.flags`



*Importance:* LOW

*Type:* LIST

*Default Value:* [CASE_INSENSITIVE]

*Validator:* [UNICODE_CHARACTER_CLASS, CANON_EQ, UNICODE_CASE, DOTALL, LITERAL, MULTILINE, COMMENTS, CASE_INSENSITIVE, UNIX_LINES]




## [SchemaNameToTopic](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/SchemaNameToTopic.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.SchemaNameToTopic$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.SchemaNameToTopic$Value
```

This transformation is used to take the name from the schema for the key or value and replace the topic with this value.
### Configuration



## [SetMaximumPrecision](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/SetMaximumPrecision.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.SetMaximumPrecision$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.SetMaximumPrecision$Value
```

This transformation is used to ensure that all decimal fields in a struct are below the maximum precision specified.
### Note

The Confluent AvroConverter uses a default precision of 64 which can be too large for some database systems.
### Configuration

#### General


##### `precision.max`

The maximum precision allowed.

*Importance:* HIGH

*Type:* INT

*Validator:* [1,...,64]




## [SetNull](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/SetNull.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.SetNull$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.SetNull$Value
```


### Configuration



## [TimestampNow](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/TimestampNow.html)

```
com.github.jcustenborder.kafka.connect.transform.common.TimestampNow
```

This transformation is used to override the timestamp of the incoming record to the time the record is being processed.
### Configuration



## [TimestampNowField](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/TimestampNowField.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.TimestampNowField$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.TimestampNowField$Value
```

This transformation is used to set a field with the current timestamp of the system running the transformation.

[✍️ Example](https://rmoff.net/2020/12/23/twelve-days-of-smt-day-12-community-transformations/#_add_the_current_timestamp_to_the_message_payload) / [🎥 Video](https://www.youtube.com/watch?v=Z7k_6vGRrkc&t=679s)


### Configuration

#### General


##### `fields`

The field(s) that will be inserted with the timestamp of the system.

*Importance:* HIGH

*Type:* LIST




## [ToJSON](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ToJSON.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Value
```


### Configuration

#### General


##### `output.schema.type`

The connect schema type to output the converted JSON as.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* STRING

*Validator:* [STRING, BYTES]



##### `schemas.enable`

Flag to determine if the JSON data should include the schema.

*Importance:* MEDIUM

*Type:* BOOLEAN




## [ToLong](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ToLong.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ToLong$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ToLong$Value
```


### Configuration

#### General


##### `fields`

The fields to transform.

*Importance:* HIGH

*Type:* LIST




## [TopicNameToField](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/TopicNameToField.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.TopicNameToField$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.TopicNameToField$Value
```


### Configuration

#### General


##### `field`

The field to insert the topic name.

*Importance:* HIGH

*Type:* STRING


## ExtractTopicName

Extract data from a message and use it as the topic name. You can either use the entire key/value (which should be a string), or use a field from a map or struct. 
Use the concrete transformation type designed for the record key (com.github.jcustenborder.kafka.connect.transform.common.ExtractTopicName$Key) or value (com.github.jcustenborder.kafka.connect.transform.common.ExtractTopicName$Value). You can also extract the entire value from a message header value (string) by using the concrete type (com.github.jcustenborder.kafka.connect.transform.common.ExtractTopicName$Header).

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractTopicName$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractTopicName$Value
```
*Header*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractTopicName$Header
```

### Configuration

#### General


##### `field`

Field name to use as the topic name. If left blank, the entire key or value is used (and assumed to be a string).

*Importance:* MEDIUM

*Type:* STRING

##### `field.format`

Specify field path format. Currently two formats are supported: JSON_PATH and PLAIN. If set to JSON_PATH, the transformer will interpret the field with JSON path interpreter, which supports nested field extraction. If left blank or set to PLAIN, the transformer will evaluate the field config as a non-nested field name. When using ExtractTopic$Header, only the default PLAIN format can be used, which will extract the header value as a string.

*Importance:* MEDIUM

*Type:* STRING

##### `skip.missing.or.null`

How to handle missing fields and null fields, keys, and values. By default, this transformation will throw an exception if a field defined in the field configuration is missing or null, or if no field is specified but the message’s key or value is null. If this configuration is set to true, the transformation will instead silently ignore these conditions and allow the record to pass through unaltered.

*Importance:* MEDIUM

*Type:* BOOLEAN


## Filter

Include or drop records that match the `filter.condition` predicate.

The `filter.condition` is a predicate specifying JSON Path that is applied to each record processed, and when this predicate successfully matches the record is either included (when `filter.type=include`) or excluded (when `filter.type=exclude`).

The `missing.or.null.behavior` property defines how the transform behaves when a record does not have the field(s) used in the filter condition predicate. By default the behavior is to fail. This property can also be set to include or exclude the record that is missing the predicate’s field(s).

Use the transformation type designed for the record key (com.github.jcustenborder.kafka.connect.transform.common.Filter$Key) or value (com.github.jcustenborder.kafka.connect.transform.common.Filter$Value).

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.Filter$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.Filter$Value
```

### Configuration

#### General


##### `filter.condition`

Specifies the criteria used to match records to be included or excluded by this transformation. Use JSON Path predicate notation defined in: https://github.com/json-path/JsonPath.

*Importance:* HIGH

*Type:* STRING

##### `filter.type`

Specifies the action to perform with records that match the filter.condition predicate. Use include to pass through all records that match the predicate and drop all records that do not satisfy the predicate, or use exclude to drop all records that match the predicate.

*Importance:* HIGH

*Valid Values:* [include, exclude]

*Type:* STRING

##### `missing.or.null.behavior`

Specifies the behavior when the record does not have the field(s) used in the filter.condition. Use fail to throw an exception and fail the connector task, include to pass the record through, or exclude to drop the record.

*Importance:* MEDIUM

*Valid Values:* [fail, include, exclude]

*Type:* STRING


# Field Paths

`Cast` and `RoundDecimal` target fields with a shared field path syntax that reaches into structs,
maps and arrays. Records with a schema (`Struct`) and without one (`Map`) are both supported.

| Spec | Matches |
| --- | --- |
| `ITEM.ATTRIBUTES.IS_DIGITAL_GOOD` | The `IS_DIGITAL_GOOD` field of the `ATTRIBUTES` object inside `ITEM`. |
| `RATINGS[*].VINTAGE` | The `VINTAGE` field of every element of the `RATINGS` array. |
| `RATINGS[1].VINTAGE` | The `VINTAGE` field of the second element of the `RATINGS` array only. |
| `ITEM.*` | Every field of `ITEM`. |
| `ITEM.IS_*` | Every field of `ITEM` whose name starts with `IS_`. `*` and `?` glob wildcards may appear anywhere in a name. |
| `**.VINTAGE` | Every `VINTAGE` field at any depth. |
| `$` or an empty string | The key or value itself. |

A leading `$` or `$.` is accepted so JSONPath style specs work too. Name matching is case sensitive.

### Note

Kafka Connect arrays and maps declare a single element schema, so all of their elements must share
one schema. When a record has a schema and an index specific path such as `RATINGS[1].VINTAGE` would
change that schema, the transformation fails with a `DataException`. Use `RATINGS[*].VINTAGE` so
every element changes together. Schemaless records have no such restriction.

The reusable implementation lives in `FieldPath` and `FieldPathUpdater` so future transformations
can target fields the same way.


## Cast

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.Cast$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.Cast$Value
```

This transformation casts fields, or the entire key or value, to a different type. It behaves like
the stock `org.apache.kafka.connect.transforms.Cast` but additionally targets deeply nested fields
using the [field path](#field-paths) syntax above.

### Tip

Strings are cast to booleans using the common textual representations, so `0`, `false`, `f`, `no`,
`n` and `off` all become `false` while `1`, `true`, `t`, `yes`, `y` and `on` all become `true`. Any
other numeric string is `true` when it is non zero.

### Configuration

#### General


##### `spec`

List of fields and the type to cast them to, of the form `field1:type1,field2:type2`. A bare `type`
casts the entire key or value.

*Importance:* HIGH

*Type:* LIST

*Valid Values:* Types are `int8`, `int16`, `int32`, `int64`, `float32`, `float64`, `boolean` and `string`



##### `skip.missing.or.null`

How to handle fields that are not present in the record or that are null. When true the field is
left alone, when false a `DataException` is thrown.

*Importance:* MEDIUM

*Type:* BOOLEAN

*Default Value:* true


### Example

Given the following value:

```json
{
  "PRODUCT": { "PRODUCT_KEY": "158425" },
  "RATINGS": [
    { "RATING_KEY": "6695543", "RATING_TEXT": "93", "VINTAGE": "2024" },
    { "RATING_KEY": "6695544", "RATING_TEXT": "91", "VINTAGE": "2022" }
  ],
  "ITEM": {
    "ITEM_KEY": "1223750",
    "ATTRIBUTES": { "IS_DIGITAL_GOOD": "0" }
  },
  "ITEM_HIERARCHY": { "CLASS_CODE": "541.01" }
}
```

and the configuration:

```json
{
  "transforms": "cast",
  "transforms.cast.type": "com.github.jcustenborder.kafka.connect.transform.common.Cast$Value",
  "transforms.cast.spec": "ITEM.ATTRIBUTES.IS_DIGITAL_GOOD:boolean,RATINGS[*].VINTAGE:int32,ITEM_HIERARCHY.CLASS_CODE:float64"
}
```

`IS_DIGITAL_GOOD` becomes `false`, both `VINTAGE` fields become the integers `2024` and `2022`, and
`CLASS_CODE` becomes `541.01`.


## RoundDecimal

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.RoundDecimal$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.RoundDecimal$Value
```

This transformation rounds numeric fields to a fixed number of decimal places. It targets fields
with the same [field path](#field-paths) syntax as `Cast`, so unlike `AdjustPrecisionAndScale` it
reaches fields nested inside structs, maps and arrays.

### Note

Decimal fields are rewritten with the new scale, so the schema of a schemaful record changes. Float,
double and numeric string fields keep their original type, and integer fields are left alone.

### Configuration

#### General


##### `fields`

The fields to round.

*Importance:* HIGH

*Type:* LIST



##### `scale`

The number of decimal places to round to.

*Importance:* HIGH

*Type:* INT

*Validator:* [0,...,127]



##### `rounding.mode`

The `java.math.RoundingMode` used when discarding decimal places.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* HALF_UP

*Validator:* Matches: ``UP``, ``DOWN``, ``CEILING``, ``FLOOR``, ``HALF_UP``, ``HALF_DOWN``, ``HALF_EVEN``, ``UNNECESSARY``



##### `precision`

The value written to the `connect.decimal.precision` schema parameter of rounded decimal fields.
`0`, the default, leaves the existing precision alone. This only affects the schema, never the value.

*Importance:* LOW

*Type:* INT

*Default Value:* 0

*Validator:* [0,...,127]



##### `skip.missing.or.null`

How to handle fields that are not present in the record or that are null. When true the field is
left alone, when false a `DataException` is thrown.

*Importance:* MEDIUM

*Type:* BOOLEAN

*Default Value:* true


### Example

```json
{
  "transforms": "round",
  "transforms.round.type": "com.github.jcustenborder.kafka.connect.transform.common.RoundDecimal$Value",
  "transforms.round.fields": "ITEM.PRICE,LINES[*].AMOUNT",
  "transforms.round.scale": "2",
  "transforms.round.rounding.mode": "HALF_UP"
}
```


# Development

## Building the source

```bash
mvn clean package
```

## Contributions

Contributions are always welcomed! Before you start any development please create an issue and
start a discussion. Create a pull request against your newly created issue and we're happy to see
if we can merge your pull request. First and foremost any time you're adding code to the code base
you need to include test coverage. Make sure that you run `mvn clean package` before submitting your
pull to ensure that all of the tests, checkstyle rules, and the package can be successfully built.
