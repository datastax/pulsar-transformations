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
package com.datastax.oss.pulsar.functions.transforms.jmh;

import com.datastax.oss.pulsar.functions.transforms.FlattenStep;
import com.datastax.oss.pulsar.functions.transforms.Utils;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class FlattenStepBenchmark {

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }

  public Record<GenericObject> nestedRecord;

  public FlattenStep flattenStep;

  @Param({"10", "100", "1000"})
  public int nestedLevels;

  @Setup(Level.Trial)
  public void setUp() {
    nestedRecord = Utils.createNestedAvroKeyValueRecord(nestedLevels);
    flattenStep = FlattenStep.builder().build();
  }

  @Benchmark
  @Fork(value = 1, warmups = 1)
  @BenchmarkMode(Mode.AverageTime)
  public void doFlatten() throws Exception {
    Utils.process(this.nestedRecord, this.flattenStep);
  }
}
