/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 */
public class WikipediaAnalysis {

	private static class CustomSource extends RichSourceFunction<String> implements
		CheckpointedFunction {

		@Override public void snapshotState(FunctionSnapshotContext context)
			throws Exception {
			LOGGER.info("do snapshotState.");
		}

		@Override public void initializeState(FunctionInitializationContext context)
			throws Exception {
			LOGGER.info("initializeState.");
		}

		@Override public void run(SourceContext ctx) throws Exception {
			while (true) {
				for (int i = 0; i < WordCountData.WORDS.length; i++) {
					String data = WordCountData.WORDS[i];
					ctx.collect(data);
					try {
						Thread.sleep(1000);
					} catch (Exception e) {}
				}
			}
		}

		@Override public void cancel() {

		}
	}

	private static class CustomSimpleSink extends RichSinkFunction<String> implements CheckpointedFunction {

		@Override public void snapshotState(FunctionSnapshotContext context)
			throws Exception {
			LOGGER.info("sink do snapshotState.");
		}

		@Override public void initializeState(FunctionInitializationContext context)
			throws Exception {

		}



		@Override public void invoke(String value, Context context)
			throws Exception {
			LOGGER.info("invoke value is {}", value);
		}
	}
	private static class CustomSink extends RichSinkFunction<Tuple2<String, Integer>> implements CheckpointedFunction {

		@Override public void snapshotState(FunctionSnapshotContext context)
			throws Exception {
			LOGGER.info("sink do snapshotState.");
		}

		@Override public void initializeState(FunctionInitializationContext context)
			throws Exception {
			LOGGER.info("sink do initializeState");
		}

		@Override public void invoke(Tuple2<String, Integer> value, Context context)
			throws Exception {
			LOGGER.info("invoke value is {}", value);
		}
	}

	public static class WikipediaEditEvent {
		private String user;
		private long byteDiff;
		private long createTime;


		public void setUser(String user) {
			this.user = user;
		}

		public void setByteDiff(long byteDiff) {
			this.byteDiff = byteDiff;
		}

		public String getUser() {
			return user;
		}

		public long getByteDiff() {
			return byteDiff;
		}

		public void setCreateTime(long createTime) {
			this.createTime = createTime;
		}

		public long getCreateTime() {
			return createTime;
		}




	}

	public static class WikipediaEditsSource extends RichSourceFunction<WikipediaEditEvent> implements
		ParallelSourceFunction<WikipediaEditEvent>, CheckpointedFunction {

		@Override public void snapshotState(FunctionSnapshotContext context)
			throws Exception {
			LOGGER.info("do snapshotState.");
		}

		@Override public void initializeState(FunctionInitializationContext context)
			throws Exception {
			LOGGER.info("initializeState.");
		}

		@Override public void run(SourceContext ctx) throws Exception {
			while (true) {
				WikipediaEditEvent editEvent = new WikipediaEditEvent();
				editEvent.setByteDiff(1);
				editEvent.setUser("leesf");
				editEvent.setCreateTime(System.currentTimeMillis());
				ctx.collect(editEvent);
				try {
					Thread.sleep(1000);
				} catch (Exception e) {}
			}
		}

		@Override public void cancel() {

		}
	}

	// *************************************************************************
	// PROGRAM
	// *************************************************************************
	final static Logger LOGGER = LoggerFactory.getLogger(WikipediaAnalysis.class);
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource()).setParallelism(2);
		KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
			.keyBy(new KeySelector<WikipediaEditEvent, String>() {
				@Override
				public String getKey(WikipediaEditEvent event) {
					return event.getUser();
				}
			});

		/*DataStream<Tuple2<String, Long>> result = keyedEdits
			.window(TumblingEventTimeWindows.of(Time.seconds(5)))
			.fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
				@Override
				public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
					acc.f0 = event.getUser();
					acc.f1 += event.getByteDiff();
					return acc;
				}
			});*/

		DataStream<Tuple2<String, Long>> result1 = keyedEdits
			.window(TumblingEventTimeWindows.of(Time.minutes(5)))
			.aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {


				@Override
				public Tuple2<String, Long> createAccumulator() {
					return new Tuple2<>("", 0L);
				}

				@Override
				public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
					 accumulator.f0 = value.getUser();
					 accumulator.f1 += value.getByteDiff();

					 return accumulator;
				}

				@Override
				public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
					return accumulator;
				}

				@Override
				public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
					System.out.println("a is " + a + ", b is " + b);
					return a;
				}
			});


		//result.print();
		result1.print().setParallelism(3);

		JobGraph jobGraph = see.getStreamGraph().getJobGraph();

		see.execute();
	}
	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}
