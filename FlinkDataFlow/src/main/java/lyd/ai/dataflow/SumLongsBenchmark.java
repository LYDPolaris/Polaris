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

package lyd.ai.dataflow;

import lyd.ai.dataflow.functions.LongSource;
import lyd.ai.dataflow.functions.MultiplyByTwo;
import lyd.ai.dataflow.functions.SumReduce;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;


public class SumLongsBenchmark {

	public static final int RECORDS_PER_INVOCATION = 7_000_000;

	public static void main(String[] args){

	}

	public void benchmarkCount(FlinkEnvironmentContext context) throws Exception {

		StreamExecutionEnvironment env = context.env;
		DataStreamSource<Long> source = env.addSource(new LongSource(RECORDS_PER_INVOCATION));

		source
				.map(new MultiplyByTwo())
				.windowAll(GlobalWindows.create())
				.reduce(new SumReduce())
				.print();

		env.execute();
	}
}
