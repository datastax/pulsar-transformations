# Pulsar transformations

Pulsar Transformations is a Pulsar Function that implements commonly done transformations on the data.
The intent is to provide a low-code approach, so you don't need to write code, understand Pulsar Schemas, 
or know one of the languages supported by Pulsar Functions to transform the data flowing in your Pulsar cluster.
The goal is also that the Pulsar Transformations Function is easy to use in the Pulsar cluster 
without having to install and operate other software.
Only basic transformations are available. For more complex use cases, such as aggregation, joins and lookups, 
more sophisticated tools such as SQL stream processing engines shall be used.

Currently available transformations are:
* [cast](#cast): modifies the key or value schema to a target compatible schema.
* [drop-fields](#drop-fields): drops fields from structured data.
* [merge-key-value](#merge-key-value): merges the fields of KeyValue records where both the key and value are structured data with the same schema type.
* [unwrap-key-value](#unwrap-key-value): if the record is a KeyValue, extract the KeyValue's key or value and make it the record value.
* [flatten](#flatten): flattens structured data.
* [drop](#drop): drops a record from further processing.
* [compute](#compute): computes new field values on the fly or replaces existing ones.

## Configuration

The `TransformFunction` reads its configuration as `JSON` from the Function `userConfig` parameter in the format:

```json
{
  "steps": [
    {
      "type": "drop-fields", "fields": "keyField1,keyField2", "part": "key"
    },
    {
      "type": "merge-key-value"
    },
    {
      "type": "unwrap-key-value"
    },
    {
      "type": "cast", "schema-type": "STRING"
    }
  ]
}
```

The transformations are done in the order in which they appear in the `steps` array.
Each step is defined by its `type` and uses its own arguments. Additionally, each step can be dynamically toggled on or off 
by supplying a `when` condition that evaluates to true or false. 


This example config applied on a `KeyValue<AVRO, AVRO>` input record with value `{key={keyField1: key1, keyField2: key2, keyField3: key3}, value={valueField1: value1, valueField2: value2, valueField3: value3}}` will return after each step:

```
{key={keyField1: key1, keyField2: key2, keyField3: key3}, value={valueField1: value1, valueField2: value2, valueField3: value3}}(KeyValue<AVRO, AVRO>)
           |
           | ”type": "drop-fields", "fields": "keyField1,keyField2”, "part": "key”
           |
{key={keyField3: key3}, value={valueField1: value1, valueField2: value2, valueField3: value3}} (KeyValue<AVRO, AVRO>)
           |
           | "type": "merge-key-value"
           |
{key={keyField3: key3}, value={keyField3: key3, valueField1: value1, valueField2: value2, valueField3: value3}} (KeyValue<AVRO, AVRO>)
           |
           | "type": "unwrap-key-value"
           |
{keyField3: key3, valueField1: value1, valueField2: value2, valueField3: value3} (AVRO)
           |
           | "type": "cast", "schema-type": "STRING"
           |
{"keyField3": "key3", "valueField1": "value1", "valueField2": "value2", "valueField3": "value3"} (STRING)
```

## Available steps

### Cast

Transforms the data to a target compatible schema.

Conversion rules:
* date and time related objects assume UTC time zone if it is not explicit.
* date and time related objects to/from STRING use the RFC3339 format.
* date related objects to/from LONG and DOUBLE are done using the number of milliseconds since EPOCH (1970-01-01T00:00:00Z).
* time related objects to/from LONG and DOUBLE are done using the number of milliseconds since midnight.

Step name: `cast`

Parameters:

| Name        | Description                                                                                                                                                                       |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| schema-type | the target schema type.                                                                                                                                                           |
| part        | when used with KeyValue data, defines if the transformation is done on the `key` or on the `value`. If `null` or absent the transformation applies to both the key and the value. |

#### Example:

UserConfig: `{"steps": [{"type": "cast", "schema-type": "STRING"}]}`

Input: `{field1: value1, field2: value2} (AVRO)`

Output: `{"field1": "value1", "field2": "value2"} (STRING)`

### Drop fields

Drops fields of structured data (Currently only AVRO is supported).

Step name: `drop-field`

Parameters:

| Name   | Description                                                                                                                                                                       |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fields | the list of fields to drop separated by commas `,`                                                                                                                                |
| part   | when used with KeyValue data, defines if the transformation is done on the `key` or on the `value`. If `null` or absent the transformation applies to both the key and the value. |

#### Example

UserConfig: `{"steps": [{"type": "drop-fields", "fields": "password,other"}]}`

Input: `{name: value1, password: value2} (AVRO)`

Output: `{name: value1} (AVRO)`

### Merge KeyValue

Merges the fields of KeyValue records where both the key and value are structured types of the same schema type. (Currently only AVRO is supported).

Step name: `merge-key-value`

Parameters: N/A

##### Example

UserConfig: `{"steps": [{"type": "merge-key-value"}]}`

Input: `{key={keyField: key}, value={valueField: value}} (KeyValue<AVRO, AVRO>)`

Output: `{key={keyField: key}, value={keyField: key, valueField: value}} (KeyValue<AVRO, AVRO>)`

### Unwrap KeyValue

If the record value is a KeyValue, extracts the KeyValue's key or value and make it the record value. 

Step name: `unwrap-key-value`

Parameters:

| Name      | Description                                                                                 |
|-----------|---------------------------------------------------------------------------------------------|
| unwrapKey | by default, the value is unwrapped. Set this parameter to `true` to unwrap the key instead. |

##### Example

UserConfig: `{"steps": [{"type": "unwrap-key-value"}]}`

Input: `{key={keyField: key}, value={valueField: value}} (KeyValue<AVRO, AVRO>)`

Output: `{valueField: value} (AVRO)`

### Flatten

Converts structured nested data into a new single-hierarchy-level structured data. 
The names of the new fields are built by concatenating the intermediate level field names.

Step name: `flatten`

| Name        | Description                                                                                                                                                                       |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delimiter   | the delimiter to use when concatenating the field names (default: `_`)                                                                                                            |
| part        | when used with KeyValue data, defines if the transformation is done on the `key` or on the `value`. If `null` or absent the transformation applies to both the key and the value. |

### Drop

Drops the record from further processing. Use in conjunction with `when` to selectively drop records.

Step name: `drop`

Parameters:

| Name | Description                                                                                         |
|------|-----------------------------------------------------------------------------------------------------|
| when | by default, the record is dropped. Set this parameter to selectively choose when to drop a message. |

#### Example

UserConfig: `{"steps": [{"type": "drop", "when": "value.firstName == value1"}]}`

Input: `{firstName: value1, lastName: value2} (AVRO)`

Output: N/A. Record is dropped.

##### Example

UserConfig: `{"steps": [{"type": "flatten"}]}`

Input: `{field1: {field11: value11, field12: value12}} (AVRO)`

Output: `{field1_field11: value11, field1_field12: value12} (AVRO)`

### Compute

Computes new field values based on an `expression` evaluated at runtime. If the field already exists, it will be overwritten.

Step name: `compute`

Parameters:

| Name   | Description                                                                                                                                |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------|
| fields | an array of JSON objects describing how to calculate the field values. The JSON object represents a `field` as described in the next table |

| Name (field)             | Description                                                                                                                                                                                                                                                                                                                                                                                                         |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name                     | the name of the field to be computed. Prefix with `key.` or `value.` to compute the fields in the key or value parts of the message. In addition, you can compute values on the following message headers [`destinationTopic`, `messageKey`, `properties.`]. Please note that properties is a map of key/value pairs that are referenced by the dot notation, for example `properties.key0`                         |
| expression               | supports the [Expression Language](#expression-language) syntax. It is evaluated at runtime and the result of the evaluation is assigned to the field.                                                                                                                                                                                                                                                              |
| type                     | the type of the computed field. This will translate to the schema type of the new field in the transformed message. The following types are currently supported [`STRING`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `DATE`, `TIME`, `DATETIME`]. For more details about each type, please check the next table. It is not required for the message headers [`destinationTopic`, `messageKey`, `properties.`] |
| optional (default: true) | if true, it marks the field as optional in the schema of the transformed message. This is useful when `null` is a possible value of the compute expression.                                                                                                                                                                                                                                                         |

| Name (field.type) | Description                                                                                         | Expression Examples                                                                                    |
|-------------------|-----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
 | `INT32`           | represents 32-bit integer.                                                                          | expression1: "2147483647", expression2: "1 + 1"                                                        |
 | `INT64`           | represents 64-bit integer.                                                                          | expression1: "9223372036854775807", expression2: "1 + 1"                                               |
 | `FLOAT`           | represents 32-bit floating point.                                                                   | expression1: "340282346638528859999999999999999999999.999999", expression2: "1.1 + 1.1"                |
 | `DOUBLE`          | represents 64-bit floating point.                                                                   | expression1: "1.79769313486231570e+308", expression2: "1.1 + 1.1"                                      |
 | `BOOLEAN`         | true or false                                                                                       | expression1: "true", expression2: "1 == 1", expression3: "value.stringField == 'matching string'"      |
| `DATE`            | a date without a time-zone in the [RFC3339 format](https://www.rfc-editor.org/rfc/rfc3339)          | expression1: "2021-12-03"                                                                              |
| `TIME`            | a time without a time-zone in the [RFC3339 format](https://www.rfc-editor.org/rfc/rfc3339)          | expression1: "20:15:45"                                                                                |
| `DATETIME`        | a date-time with an offset from UTC in the [RFC3339 format](https://www.rfc-editor.org/rfc/rfc3339) | expression1: "2022-10-02T01:02:03+02:00", expression2: "2019-10-02T01:02:03Z", expression3: "fn:now()" |

##### Example 1

UserConfig: `{"steps": [{"type": "compute", "fields":[
                {"name": "key.newKeyField",   "expression" : "5*3", "type": "INT32"},"
                {"name": "value.valueField",  "expression" : "fn:concat(value.valueField, '_suffix')", "type": "STRING"}]}
             ]}`

Input: `{key={keyField: key}, value={valueField: value}} (KeyValue<AVRO, AVRO>)`

Output: `{key={keyField: key, newKeyField: 15}, value={valueField: value_suffix}} (KeyValue<AVRO, AVRO>)`

##### Example 2

UserConfig: `{"steps": [{"type": "compute", "fields":[
                {"name": "destinationTopic",   "expression" : "'routed'"},
                {"name": "properties.k1", "expression" : "'overwritten'"},
                {"name": "properties.k2", "expression" : "'new'"}]}
             ]}`

Input: `{key={keyField: key}, value={valueField: value}} (KeyValue<AVRO, AVRO>), headers=destinationTopic: out1, propertes: {k1:v1}`

Output: `{key={keyField: key}, value={valueField: value}} (KeyValue<AVRO, AVRO>), headers=destinationTopic:routed, propertes: {k1:overwritten, k2:new}`

### Expression Language
In order to support [Condition Steps](#conditional-steps) and the [Compute](#compute) Transform, an expression language is required to evaluate the conditional step `when` or the compute step `expression`. The syntax is([EL](https://javaee.github.io/tutorial/jsf-el001.html#BNAHQ)) like that uses the dot notation to access field properties or map keys. It supports the following operators and functions:

#### Operators
The Expression Language supports the following operators:
* Arithmetic: +, - (binary), *, / and div, % and mod, - (unary)
* Logical: and, &&, or, ||, not, !
* Relational: ==, eq, !=, ne, <, lt, >, gt, <=, ge, >=, le.

#### Functions
Utility methods available under the `fn` namespace. For example, to calculate the current date, use 'fn:now()'. The Expression Language supports the following Functions:
* uppercase(input): , lowercase(input): Changes the capitalization of a string. If `input` is not a string, it attempts a string conversion. If the input is `null`, it returns `null`. 
* contains(input, value): Returns true if `value` exists in `input`. It attempts string conversion on both `input` and `value` if either is not a string. If `input` or `value` is `null`, ir returns false. 
* trim(input): Returns the `input` string with all leading and trailing spaces removed. If the `input` is not a string, it attempts a string conversion.
* concat(input1, input2): Returns a string concatenation of `input1` and `input2`. If either input is `null`, it is treated as an empty string.
* coalesce(value, valueIfNull): Returns `value` if it is not `null`, otherwise returns `valueIfNull`.
* replace(input, regex, replacement): Replaces each substring of `input` that matches the `regex` regular expression with `replacement`. See [Java replaceAll](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/String.html#replaceAll(java.lang.String,java.lang.String)).
* now(): Returns the current epoch millis.
* dateadd(input, delta, unit): Performs date/time arithmetic operations on the `input` date/time. 
  * `input` can be either epoch millis or an [RFC3339](https://www.rfc-editor.org/rfc/rfc3339) format like "2022-10-14T10:15:30+01:00"
  * `delta` is the amount of `unit` to add to `input`. Can be a negative value to perform subtraction.
  * `unit` the unit of time to add or subtract. Can be one of [`years`, `months`, `days`, `hours`, `minutes`, `seconds`, `millis`]

#### Conditional Steps

Each step accept an optional `when` configuration that is evaluated at step execution time against current record (i.e. the as seen by
the current step in the transformation pipeline). The `when` condition supports the [Expression Language](#expression-language) syntax. It provides access to the record attributes as follows:
* `key`: the key portion of the record in a KeyValue schema. 
* `value`: the value portion of the record in a KeyValue schema, or the message payload itself.
* `messageKey`: the optional key messages are tagged with (aka. Partition Key).
* `topicName`: the optional name of the topic which the record originated from (aka. Input Topic).
* `destinationTopic`: the name of the topic on which the transformed record will be sent (aka. Output Topic).
* `eventTime`: the optional timestamp attached to the record from its source. For example, the original timestamp attached to the pulsar message.
* `properties`: the optional user-defined properties attached to record

You can use the `.` operator to access top level or nested properties on a schema-full `key` or `value`. For example, `key.keyField1` or `value.valueFiled1.nestedValueField`. You can also use to access different keys of the user defined properties. For example, `properties.prop1`.

#### Example 1: KeyValue (KeyValue<AVRO, AVRO>)

```json
{
  "key": {
    "compound": {
      "uuid": "uuidValue",
      "timestamp": 1663616014
    },
    "value" : {
      "first" : "f1",
      "last" : "l1",
      "rank" : 1,
      "address" : {
        "zipcode" : "abc-def"
      }
    }
  }}
```

| when                                                        | Evaluates to |
|-------------------------------------------------------------|--------------|
| `"key.compund.uuid == 'uudValue'"`                          | True         |
| `"key.compund.timestamp <= 10"`                             | False        |
| `"value.first == 'f1' && value.last.toUpperCase() == 'L1'`  | True         |
| `"value.rank <= 1 && value.address.substring(0, 3) == 'abc'` | True         |

#### Example 2: (Primitive string schema with metadata)

* Partition Key: `key1`
* Source topic: `topic1`
* User defined k/v: `{"prop1": "p1", "prop2": "p2"}`
* Payload (String): `Hello world!`

| when                                               | Evaluates to |
|----------------------------------------------------|--------------|
| `"messageKey == 'key1' or topicName == 'topic1' "` | True         | 
| `"value == 'Hello world!'"`                        | True         |
| `"properties.prop1 == 'p2'"`                       | False        |

## Deployment

See [the Pulsar docs](https://pulsar.apache.org/fr/docs/functions-deploy) for more details on how to deploy a Function.

### Deploy as a non built-in Function

* Create a Transformation Function providing the path to the Pulsar Transformations NAR.
```shell
pulsar-admin functions create \
--jar pulsar-transformations-2.0.0.nar \
--name my-function \
--inputs my-input-topic \
--output my-output-topic \
--user-config '{"steps": [{"type": "drop-fields", "fields": "password"}, {"type": "merge-key-value"}, {"type": "unwrap-key-value"}, {"type": "cast", "schema-type": "STRING"}]}'
```

### Deploy as a built-in Function

* Put the Pulsar Transformations NAR in the `functions` directory of the Pulsar Function worker (or broker).
```shell
cp pulsar-transformations-2.0.0.nar $PULSAR_HOME/functions/pulsar-transformations-2.0.0.nar
```
* Restart the function worker (or broker) instance or reload all the built-in functions:
```shell
pulsar-admin functions reload
```
* Create a Transformation Function with the admin CLI. The built-in function type is `transforms`.
```shell
pulsar-admin functions create \
--function-type transforms \
--name my-function \
--inputs my-input-topic \
--output my-output-topic \
--user-config '{"steps": [{"type": "drop-fields", "fields": "password"}, {"type": "merge-key-value"}, {"type": "unwrap-key-value"}, {"type": "cast", "schema-type": "STRING"}]}'
```

### Deploy the Transformation Function coupled with a Pulsar Sink

*This requires Datastax Luna Streaming 2.10.1.6+.*

Thanks to [PIP-193](https://github.com/apache/pulsar/issues/16739) it's possible to execute a function inside a sink process, removing the need of temporary topics.

* Create a Pulsar Sink instance with `transform-function` Transformation Function with the admin CLI.
```shell
pulsar-admin sinks create \
--sink-type <sink_type> \
--inputs my-input-topic \
--tenant public \
--namespace default \
--name my-sink \
--transform-function "builtin://transforms" \
--transform-function-config '{"steps": [{"type": "drop-fields", "fields": "password"}, {"type": "merge-key-value"}, {"type": "unwrap-key-value"}, {"type": "cast", "schema-type": "STRING"}]}'
```



