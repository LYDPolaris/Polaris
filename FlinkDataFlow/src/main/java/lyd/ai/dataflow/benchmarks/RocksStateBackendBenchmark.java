/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lyd.ai.dataflow.benchmarks;

import lyd.ai.dataflow.benchmarks.functions.IntLongApplications;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

import static org.openjdk.jmh.annotations.Scope.Thread;

@OperationsPerInvocation(value = RocksStateBackendBenchmark.RECORDS_PER_INVOCATION)
public class RocksStateBackendBenchmark extends StateBackendBenchmarkBase {
	public static final int RECORDS_PER_INVOCATION = 2_000_000;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + RocksStateBackendBenchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public void stateBackends(RocksStateBackendContext context) throws Exception {
		IntLongApplications.reduceWithWindow(context.source, TumblingEventTimeWindows.of(Time.seconds(10_000)));
		context.execute();
	}

	@State(Thread)
	public static class RocksStateBackendContext extends StateBackendContext {
		@Param({"ROCKS", "ROCKS_INC"})
		public StateBackend stateBackend = StateBackend.MEMORY;

		@Setup
		public void setUp() throws IOException {
			super.setUp(stateBackend, RECORDS_PER_INVOCATION);
		}

		@TearDown
		public void tearDown() throws IOException {
			super.tearDown();
		}
	}
}