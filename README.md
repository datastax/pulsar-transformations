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
Each step is defined by its `type` and uses its own arguments.


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

Step name: `cast`

Parameters:

| Name        | Description |
| ----------- | ----------- |
| schema-type | the target schema type. Only `STRING` is available. |
| part | when used with KeyValue data, defines if the transformation is done on the `key` or on the `value`. If `null` or absent the transformation applies to both the key and the value. |

#### Example:

UserConfig: `{"steps": [{"type": "cast", "schema-type": "STRING"}]}`

Input: `{field1: value1, field2: value2} (AVRO)`

Output: `{"field1": "value1", "field2": "value2"} (STRING)`

### Drop fields

Drops fields of structured data (Currently only AVRO is supported).

Step name: `drop-field`

Parameters:

| Name        | Description |
| ----------- | ----------- |
| fields | the list of fields to drop separated by commas `,` |
| part | when used with KeyValue data, defines if the transformation is done on the `key` or on the `value`. If `null` or absent the transformation applies to both the key and the value. |

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

| Name        | Description |
| ----------- | ----------- |
| unwrapKey | by default, the value is unwrapped. Set this parameter to `true` to unwrap the key instead |

##### Example

UserConfig: `{"steps": [{"type": "unwrap-key-value"}]}`

Input: `{key={keyField: key}, value={valueField: value}} (KeyValue<AVRO, AVRO>)`

Output: `{valueField: value} (AVRO)`

### Flatten

Converts structured nested data into a new single-hierarchy-level structured data. 
The names of the new fields are built by concatenating the intermediate level field names.

Step name: `flatten`

| Name        | Description |
| ----------- | ----------- |
| delimiter | the delimiter to use when concatenating the field names (default: `_`) |
| part | when used with KeyValue data, defines if the transformation is done on the `key` or on the `value`. If `null` or absent the transformation applies to both the key and the value. |


##### Example

UserConfig: `{"steps": [{"type": "flatten"}]}`

Input: `{field1: {field11: value11, field12: value12}} (AVRO)`

Output: `{field1_field11: value11, field1_field12: value12} (AVRO)`

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



