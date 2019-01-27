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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

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
public class WordCount {

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

	// *************************************************************************
	// PROGRAM
	// *************************************************************************
	final static Logger LOGGER = LoggerFactory.getLogger(WordCount.class);
	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			String test = params.get("test");

			// get default test text data
			//text = env.fromElements(WordCountData.WORDS);
			text = env.addSource(new CustomSource()).setParallelism(1);

		}

		DataStream<Tuple2<String, Integer>> counts =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			text.flatMap(new Tokenizer()).setParallelism(256)
			// group by the tuple field "0" and sum up tuple field "1"
			.keyBy(0).sum(1).setParallelism(3);

		// emit result
		System.out.println("Printing result to stdout. Use --output to specify output path.");
		counts.addSink(new CustomSink()).setParallelism(4);
		//text.addSink(new CustomSimpleSink()).setParallelism(2);

		// execute program

		System.out.println(env.getStreamGraph().getStreamingPlanAsJSON());

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());



		env.execute("Streaming WordCount");
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
