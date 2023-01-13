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
package com.datastax.oss.pulsar.functions.transforms.jstl;

import com.datastax.oss.pulsar.functions.transforms.ComputeStep;
import com.datastax.oss.pulsar.functions.transforms.Utils;
import com.datastax.oss.pulsar.functions.transforms.model.ComputeField;
import com.datastax.oss.pulsar.functions.transforms.model.ComputeFieldType;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class JstlEvaluatorBenchmark {
  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }

  public Record<GenericObject> nestedRecord;
  public ComputeStep computeStep;

  @Setup(Level.Iteration)
  public void setUp() {
    nestedRecord = Utils.createTestAvroKeyValueRecord();
    computeStep = ComputeStep.builder().fields(buildComputeFields(("value"))).build();
  }

  @Benchmark
  @Fork(value = 1, warmups = 1)
  @BenchmarkMode(Mode.All)
  public void doCompute() throws Exception {
    Utils.process(this.nestedRecord, this.computeStep);
  }

  private List<ComputeField> buildComputeFields(String scope) {
    List<ComputeField> fields = new ArrayList<>();
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newStringField")
            .expression("'Hotaru'")
            .type(ComputeFieldType.STRING)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newInt8Field")
            .expression("127")
            .type(ComputeFieldType.INT8)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newInt16Field")
            .expression("32767")
            .type(ComputeFieldType.INT16)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newInt32Field")
            .expression("2147483647")
            .type(ComputeFieldType.INT32)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newInt64Field")
            .expression("9223372036854775807")
            .type(ComputeFieldType.INT64)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newFloatField")
            .expression("340282346638528859999999999999999999999.999999")
            .type(ComputeFieldType.FLOAT)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newDoubleField")
            .expression("1.79769313486231570e+308")
            .type(ComputeFieldType.DOUBLE)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newBooleanField")
            .expression("1 == 1")
            .type(ComputeFieldType.BOOLEAN)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newDateField")
            .expression("'2007-12-03T00:00:00Z'")
            .type(ComputeFieldType.DATE)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newLocalDateField")
            .expression("'2007-12-03'")
            .type(ComputeFieldType.LOCAL_DATE)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newTimeField")
            .expression("'10:15:30'")
            .type(ComputeFieldType.TIME)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newLocalTimeField")
            .expression("'10:15:30'")
            .type(ComputeFieldType.LOCAL_TIME)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newTimestampField")
            .expression("'2007-12-03T10:15:30+00:00'")
            .type(ComputeFieldType.INSTANT)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newInstantField")
            .expression("'2007-12-03T10:15:30+00:00'")
            .type(ComputeFieldType.TIMESTAMP)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newLocalDateTimeField")
            .expression("'2007-12-03T10:15:30'")
            .type(ComputeFieldType.LOCAL_DATE_TIME)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newDateTimeField")
            .expression("'2007-12-03T10:15:30+00:00'")
            .type(ComputeFieldType.DATETIME)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newBytesField")
            .expression("'Hotaru'.bytes")
            .type(ComputeFieldType.BYTES)
            .build());

    return fields;
  }
}
