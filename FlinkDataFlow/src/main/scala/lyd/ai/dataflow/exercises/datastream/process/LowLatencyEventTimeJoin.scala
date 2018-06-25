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

package lyd.ai.dataflow.exercises.datastream.process

import lyd.ai.dataflow.exercises.datastream.sources.FinSources
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object LowLatencyEventTimeJoin {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Simulated trade stream
    val tradeStream = FinSources.tradeSource(env)

    // simulated customer stream
    val customerStream = FinSources.customerSource(env)
    import org.apache.flink.api.scala._
    val joinedStream = tradeStream
      .keyBy(_.customerId)
      .connect(customerStream.keyBy(_.customerId))
      .process(new EventTimeJoinFunctionJ)

    joinedStream.print()

    env.execute
  }
}
