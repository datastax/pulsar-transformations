# Pulsar transformations

Currently, when users want to modify the data in Pulsar, they need to write a Function. It would be handy for them to be able to use a ready-made, built-in Function that implements the most common basic transformations like the ones available in Kafka Connect’s SMTs.
This removes the burden of writing the Function themselves, having to understanding the complexities of Pulsar Schemas, and coding in a language that they may not master (probably Java if they want to do advanced stuff). Users benefit from battle-tested, well-maintained, performance-optimised code.

This repo provides a `TransformFunction` that executes a sequence of basic transformations on the data. 
The `TransformFunction` is easy to configure and launchable as a built-in NAR.
The `TransformFunction` is able to apply a sequence of common transformations in-memory, so we don’t need to execute the `TransformFunction` multiple times and read/write to a topic each time.

## Implementation

When it processes a record, `TransformFunction` will:

1. Call in sequence the process method of a series of `TransformStep` implementations.
2. Each `TransformStep` will modify the output message and topic as needed.
3. Send the transformed message to the output topic.

The `TransformFunction` reads its configuration as `JSON` from `userConfig` in the format:

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

`TransformFunction` will be built as a NAR including a `pulsar-io.yaml` service file so it can be registered as a built-in function with name transform.