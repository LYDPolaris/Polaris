/*
 * Copyright 2017 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lyd.ai.dataflow.exercises.datastream.process;

import lyd.ai.dataflow.exercises.datastream.datatypes.Customer;
import lyd.ai.dataflow.exercises.datastream.datatypes.EnrichedTrade;
import lyd.ai.dataflow.exercises.datastream.datatypes.Trade;
import lyd.ai.dataflow.exercises.datastream.sources.FinSources;
import lyd.ai.dataflow.exercises.datastream.sources.FinSourcesJ;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LowLatencyEventTimeJoinJ {
	public static void main(String[] args) throws Exception {
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Simulated trade stream
		DataStream<Trade> tradeStream = FinSourcesJ.tradeSource(env);

		// simulated customer stream
		DataStream<Customer> customerStream = FinSourcesJ.customerSource(env);

		DataStream<EnrichedTrade> joinedStream = tradeStream
				.keyBy("customerId")
				.connect(customerStream.keyBy("customerId"))
				.process(new EventTimeJoinFunctionJ());

		joinedStream.print();

		env.execute("Low-latency event-time join");
	}
}
