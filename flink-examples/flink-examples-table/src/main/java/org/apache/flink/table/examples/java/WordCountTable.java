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

package org.apache.flink.table.examples.java;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.GroupedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Simple example for demonstrating the use of the Table API for a Word Count in Java.
 *
 * <p>This example shows how to:
 *  - Convert DataSets to Tables
 *  - Apply group, aggregate, select, and filter operations
 */
public class WordCountTable {
	final static Logger LOGGER = LoggerFactory.getLogger(WordCountTable.class);
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************



	private static class CustomProducer extends RichSinkFunction<Tuple2<Boolean, Row>> {
		@Override
		public void open(Configuration configuration) throws Exception {
			LOGGER.info("open");
		}

		@Override
		public void invoke(Tuple2<Boolean, Row> in, Context context) {
			System.out.println(in.f0 + ", " + in.f1);
		}
	}

	private static class CustomRetractTableSink implements TableSink<Tuple2<Boolean, Row>>,
		RetractStreamTableSink<Row> {
		private CustomProducer customProducer;
		private TypeInformation<Row> typeInfo;
		private String[] fieldNames;
		private TypeInformation<?>[] fieldTypes;

		public CustomRetractTableSink(CustomProducer customProducer, TypeInformation<?>[] fieldTypes, String[] fieldNames) {
			this.customProducer = customProducer;
			this.fieldTypes = fieldTypes;
			this.fieldNames = fieldNames;
			this.typeInfo = new RowTypeInfo(fieldTypes, fieldNames);
		}

		@Override public TypeInformation<Row> getRecordType() {
			return typeInfo;
		}

		@Override public void emitDataStream(
			org.apache.flink.streaming.api.datastream.DataStream<Tuple2<Boolean, Row>> dataStream) {
			dataStream.addSink(customProducer);
		}

		@Override public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
			return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
		}

		@Override public String[] getFieldNames() {
			return fieldNames;
		}

		@Override public TypeInformation<?>[] getFieldTypes() {
			return fieldTypes;
		}

		@Override public TableSink<Tuple2<Boolean, Row>> configure(
			String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			CustomRetractTableSink copy = new CustomRetractTableSink(customProducer, fieldTypes, fieldNames);
			return copy;
		}
	}

	private static class CustomTableSink implements
		StreamTableSink<Tuple2<Boolean, Row>>,
		UpsertStreamTableSink<Row> {
		private CustomProducer customProducer;
		private TypeInformation<Row> typeInfo;
		private String[] fieldNames;
		private TypeInformation<?>[] fieldTypes;

		public CustomTableSink(CustomProducer customProducer, TypeInformation<?>[] fieldTypes, String[] fieldNames) {
			this.customProducer = customProducer;
			this.fieldTypes = fieldTypes;
			this.fieldNames = fieldNames;
			this.typeInfo = new RowTypeInfo(fieldTypes, fieldNames);
		}

		@Override public void setKeyFields(String[] keys) {
			System.out.println("setKeys is " + (keys == null ? null : Arrays.asList(keys)));

		}

		@Override public void setIsAppendOnly(Boolean isAppendOnly) {
			System.out.println("isAppendOnly " + isAppendOnly);
		}

		@Override public TypeInformation<Row> getRecordType() {
			return typeInfo;
		}

		@Override public void emitDataStream(
			org.apache.flink.streaming.api.datastream.DataStream<Tuple2<Boolean, Row>> dataStream) {
			dataStream.addSink(customProducer);
		}

		@Override public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
			return new TupleTypeInfo<>(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
		}

		@Override public String[] getFieldNames() {
			return fieldNames;
		}

		@Override public TypeInformation<?>[] getFieldTypes() {
			return fieldTypes;
		}

		@Override public TableSink<Tuple2<Boolean, Row>> configure(
			String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			CustomTableSink copy = new CustomTableSink(customProducer, fieldTypes, fieldNames);
			return copy;
		}
	}
	private static class CustomSource implements SourceFunction<WC> {

		@Override public void run(SourceContext<WC> ctx) throws Exception {
			String[] datas = new String[] {
				"leesf#jingzhou#1992#25", "dyd#wuhan#1992#25", "zhangsan#beijing#1993#25", "lisi#shagnhai#1995#23"
			};

			while (true) {
				for (int i = 0; i < datas.length; i++) {
					String data = datas[i];
					String[] res = data.split("#");
					ctx.collect(new WC(res[0], res[1], res[2],Integer.parseInt(res[3])));
					try {
						Thread.sleep(1000);
					} catch (Exception e) {}
				}
			}
		}

		@Override public void cancel() {

		}
	}

	public static void main(String[] args) throws Exception {


		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(streamExecutionEnvironment);


		Table table = tableEnvironment.fromDataStream(streamExecutionEnvironment.addSource(new CustomSource()), "name, address, birth, age, a.proctime");

		tableEnvironment.registerTable("source", table);
		Table table1 = tableEnvironment.scan("source");
		//Table selectTable1 = table1.select("name, address, birth, age").groupBy("name, address, birth").select("name, address, birth, sum(age)");
		Table selectTable = table1/*.groupBy("name, address, birth")*/.select("name, address, birth, age, a");

		String[] fieldNames = new String[] {"name", "address", "birth", "age"};
		List<TypeInformation<?>> fieldTypes = new ArrayList<>();
		fieldTypes.add(Types.STRING());
		fieldTypes.add(Types.STRING());
		fieldTypes.add(Types.STRING());
		fieldTypes.add(Types.LONG());
		TypeInformation<?>[] result = new TypeInformation<?>[fieldTypes.size()];
		fieldTypes.toArray(result);
		CustomProducer customProducer = new CustomProducer();

		TableSink sinktable = new CustomTableSink(customProducer, result, fieldNames);
		//TableSink sinktable = new CustomRetractTableSink(customProducer, result, fieldNames);


		selectTable.printSchema();
		System.out.println(Arrays.asList(selectTable.getSchema().getFieldNames()));
		System.out.println(Arrays.asList(selectTable.getSchema().getFieldTypes()));
		selectTable.writeToSink(sinktable);
		//tableEnvironment.registerTableSink("sink", sinktable);
		//selectTable.insertInto("sink");
		//tableEnvironment.toRetractStream(tableEnvironment.scan("sink"), WC.class).print();

		streamExecutionEnvironment.execute();


	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO containing a word and its respective count.
	 */
	public static class WC {
		public String name;
		public String address;
		public String birth;
		public long age;


		// public constructor to make it a Flink POJO
		public WC() {}

		public WC(String name, String address, String birth, int age) {
			this.name = name;
			this.address = address;
			this.birth = birth;
			this.age = age;

		}


	}
}
