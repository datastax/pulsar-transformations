/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.pulsar.functions.transforms.tests.util;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class CqlLogicalTypes {
    // CQL DECIMAL
    private static final String CQL_DECIMAL = "cql_decimal";
    private static final String CQL_DECIMAL_BIGINT = "bigint";
    private static final String CQL_DECIMAL_SCALE = "scale";
    public static final CqlDecimalLogicalType CQL_DECIMAL_LOGICAL_TYPE = new CqlDecimalLogicalType();
    public static final org.apache.avro.Schema decimalType  = CQL_DECIMAL_LOGICAL_TYPE.addToSchema(
            SchemaBuilder.record(CQL_DECIMAL)
                    .fields()
                    .name(CQL_DECIMAL_BIGINT).type().bytesType().noDefault()
                    .name(CQL_DECIMAL_SCALE).type().intType().noDefault()
                    .endRecord());
    public static org.apache.avro.Schema.Field createDecimalField(String name, boolean optional) {
        org.apache.avro.Schema.Field decimalField = new org.apache.avro.Schema.Field(name, decimalType);
        if (optional) {
            decimalField =
                new org.apache.avro.Schema.Field(
                    name,
                    SchemaBuilder.unionOf().nullType().and().type(decimalField.schema()).endUnion(),
                    null,
                    org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE);
        }

        return decimalField;
    }

    public static org.apache.avro.generic.GenericRecord createDecimalRecord(BigDecimal decimal) {
        org.apache.avro.generic.GenericRecord decimalRecord = new GenericData.Record(CqlLogicalTypes.decimalType);
        decimalRecord.put(CqlLogicalTypes.CQL_DECIMAL_BIGINT, ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
        decimalRecord.put(CqlLogicalTypes.CQL_DECIMAL_SCALE, decimal.scale());
        return decimalRecord;
    }

    public static class CqlDecimalLogicalType extends LogicalType {
        public CqlDecimalLogicalType() {
            super(CQL_DECIMAL);
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            // validate the type
            if (schema.getType() != Schema.Type.RECORD) {
                throw new IllegalArgumentException("Logical type cql_decimal must be backed by a record");
            }
        }
    }
}
