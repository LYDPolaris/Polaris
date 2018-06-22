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

package lyd.ai.dataflow.cep;

import lyd.ai.dataflow.cep.events.MeasureEvent;
import lyd.ai.dataflow.cep.events.MeasureEventType;
import lyd.ai.dataflow.cep.events.WarningEvent;
import lyd.ai.dataflow.cep.sources.ExampleSource;
import lyd.ai.dataflow.cep.events.MeasureEventType;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.nfa.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * Simple CEP program with 3 types of events
 * 
 * l = LowPriorityEvent l{3} -> warning
 * n = NormalPriorityEvent n{2} -> warning
 * h = HighPriorityEvent h{1} -> warning
 */
public class MeasureCEP {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		env.setParallelism(1);
		
		DataStream<MeasureEvent> ds = env.addSource(new ExampleSource(), "Source 1")
		.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<MeasureEvent>() {
			private static final long serialVersionUID = 3409048222597677035L;

			@Override
			public long extractTimestamp(MeasureEvent event, long previousElementTimestamp) {
				return event.getEventTime();
			}
			
			@Override
			public Watermark checkAndGetNextWatermark(MeasureEvent lastElement, long extractedTimestamp) {
				return new Watermark(extractedTimestamp);
			}
		});
		
		// divide input-stream into three streams [low, normal, high]-priority
		DataStream<MeasureEvent> lowPriorityStream = ds.filter(new FilterFunction<MeasureEvent>() {
			private static final long serialVersionUID = 2003297503669062954L;

			@Override
			public boolean filter(MeasureEvent event) throws Exception {
				return event.getEventType().equals(MeasureEventType.LOW);
			}
		});
		
		DataStream<MeasureEvent> normalPriorityStream = ds.filter(new FilterFunction<MeasureEvent>() {

			@Override
			public boolean filter(MeasureEvent event) throws Exception {
				return event.getEventType().equals(MeasureEventType.NORMAL);
			}
		});
		
		DataStream<MeasureEvent> highPriorityStream = ds.filter(new FilterFunction<MeasureEvent>() {

			@Override
			public boolean filter(MeasureEvent event) throws Exception {
				return event.getEventType().equals(MeasureEventType.HIGH);
			}
		});
		
		ds.print();
		
		// match every high-priority-event with data >= 50
		Pattern<MeasureEvent, ?> highPattern = Pattern.<MeasureEvent>begin("high")
				.where(new SimpleCondition<MeasureEvent>() {
					private static final long serialVersionUID = 4833978306621774597L;

					@Override
					public boolean filter(MeasureEvent event) throws Exception {
						return event.getData() >= 50;
					}
				});
		
		DataStream<WarningEvent> dsResultHigh = CEP.pattern(highPriorityStream.keyBy("id"), highPattern).select(new PatternSelectFunction<MeasureEvent, WarningEvent>() {

			@Override
			public WarningEvent select(Map<String, List<MeasureEvent>> events) throws Exception {
				System.out.println("----");
				events.forEach((k, v) -> {
					System.out.println("MATCH: " + k + " " + v);
				});
				System.out.println("----");
				MeasureEvent matchedEvent = events.get("high").get(0);
				return new WarningEvent(matchedEvent.getId(), matchedEvent.getData(), matchedEvent.getEventType());
			}
		});
		
		// match two consecutive normal-priority-events with data >= 55 where the
		// data of the second event is higher than the first ones
		Pattern<MeasureEvent, ?> normalPattern = Pattern.<MeasureEvent>begin("normal 1", AfterMatchSkipStrategy.skipPastLastEvent())
				.where(new SimpleCondition<MeasureEvent>() {
					private static final long serialVersionUID = 8679799839985001154L;

					@Override
					public boolean filter(MeasureEvent event) throws Exception {
						return event.getData() >= 55;
					}
				})
				.next("normal 2")
				.where(new IterativeCondition<MeasureEvent>() {
					
					@Override
					public boolean filter(MeasureEvent event, Context<MeasureEvent> ctx) throws Exception {
						MeasureEvent ev = new MeasureEvent();
						
						// there is just one previous event..
						for(MeasureEvent m: ctx.getEventsForPattern("normal 1")) {
							//System.out.println("e: " + m);
							ev = m;
						};
						return event.getData() > ev.getData();
					}
				}).within(Time.seconds(10));


		DataStream<WarningEvent> dsResultNormal = CEP.pattern(normalPriorityStream.keyBy("id"), normalPattern).select(new PatternSelectFunction<MeasureEvent, WarningEvent>() {
			private static final long serialVersionUID = 7298709929613070464L;

			@Override
			public WarningEvent select(Map<String, List<MeasureEvent>> events) throws Exception {
				System.out.println("----");
				events.forEach((k, v) -> {
					System.out.println("MATCH: " + k + " " + v);
				});
				System.out.println("----");
				MeasureEvent matchedEvent = events.get("normal 2").get(0);
				return new WarningEvent(matchedEvent.getId(), matchedEvent.getData(), matchedEvent.getEventType());
			}
		});
		
		// match three consecutive low-priority-events with data >= 45 where the
		// data of the second and third event is increased continuously
		Pattern<MeasureEvent, ?> lowPattern = Pattern.<MeasureEvent>begin("low 1", AfterMatchSkipStrategy.skipPastLastEvent())
				.where(new SimpleCondition<MeasureEvent>() {
					private static final long serialVersionUID = 8679799839985001154L;

					@Override
					public boolean filter(MeasureEvent event) throws Exception {
						return event.getData() >= 45;
					}
				})
				.next("low 2")
				.where(new IterativeCondition<MeasureEvent>() {
					
					@Override
					public boolean filter(MeasureEvent event, Context<MeasureEvent> ctx) throws Exception {
						MeasureEvent ev = new MeasureEvent();
						
						// there is just one previous event..
						for(MeasureEvent m: ctx.getEventsForPattern("low 1")) {
							//System.out.println("e: " + m);
							ev = m;
						};
						return event.getData() > ev.getData();
					}
				})
				.next("low 3")
				.where(new IterativeCondition<MeasureEvent>() {
					
					@Override
					public boolean filter(MeasureEvent event, Context<MeasureEvent> ctx) throws Exception {
						MeasureEvent ev = new MeasureEvent();
						
						// there is just one previous event..
						for(MeasureEvent m: ctx.getEventsForPattern("low 2")) {
							//System.out.println("e: " + m);
							ev = m;
						};
						return event.getData() > ev.getData();
					}
				})
				.within(Time.seconds(10));

		DataStream<WarningEvent> dsResultLow = CEP.pattern(lowPriorityStream.keyBy("id"), lowPattern).select(new PatternSelectFunction<MeasureEvent, WarningEvent>() {
			private static final long serialVersionUID = 7298709929613070464L;

			@Override
			public WarningEvent select(Map<String, List<MeasureEvent>> events) throws Exception {
				System.out.println("----");
				events.forEach((k, v) -> {
					System.out.println("MATCH: " + k + " " + v);
				});
				System.out.println("----");
				MeasureEvent matchedEvent = events.get("low 3").get(0);
				return new WarningEvent(matchedEvent.getId(), matchedEvent.getData(), matchedEvent.getEventType());
			}
		});

		DataStream<WarningEvent> dsWarnings = dsResultHigh.union(dsResultLow, dsResultNormal);
		dsWarnings.print();
		// add to other sinks...
		
		// execute program
		env.execute("Measure data");
	}
}
