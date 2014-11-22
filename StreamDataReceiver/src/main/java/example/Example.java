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

package example;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public final class Example {
	private static final String CHECKPOINTS_DIR = "/home/2012/m.kopec/checkpoints";
	private static final Pattern SPACE = Pattern.compile("[\n ]", Pattern.MULTILINE);

	public static void main(String[] args) {
		if (args.length < 4) {
			System.err
					.println("Usage: JavaNetworkWordCount <hostname> <port> <batch_time_milis> <word_sleep_time_milis>");
			System.exit(1);
		}

		int batchSizeMilis = Integer.parseInt(args[2]);
		final int wordSleepMilis = Integer.parseInt(args[3]);

		SparkConf sparkConf = new SparkConf().setAppName("NumberMonitor");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				new Duration(batchSizeMilis));
		ssc.checkpoint(CHECKPOINTS_DIR);

		JavaReceiverInputDStream<String> lines = ssc
				.receiverStream(new JavaCustomReceiver(args[0], Integer
						.parseInt(args[1])));

		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterable<String> call(String x) {
						return Lists.newArrayList(SPACE.split(x));
					}
				});

		JavaDStream<Long> numbers = words.filter(
				new Function<String, Boolean>() {
					@Override
					public Boolean call(String x) throws Exception {
						return !StringUtils.isBlank(x);
					}
				}).map(new Function<String, Long>() {
			@Override
			public Long call(String x) throws Exception {
				return Long.valueOf(transform(x, wordSleepMilis));
			}
		});

		JavaPairDStream<String, Long> maxStream = numbers.mapToPair(
				new PairFunction<Long, String, Long>() {
					@Override
					public Tuple2<String, Long> call(Long n) {
						return new Tuple2<String, Long>("max", n);
					}
				}).updateStateByKey(
				new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
					@Override
					public Optional<Long> call(List<Long> values,
							Optional<Long> state) {
						Long max = state.or(Long.MIN_VALUE);
						for (Long x : values)
							if (x > max)
								max = x;
						return Optional.of(max);
					}
				});

		maxStream.checkpoint(new Duration(1000 * 60 * 60 * 24));

		maxStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				if (rdd.count() != 1) {
					System.out.println("######### Empty RDD ###########");
					return null;
				}
				List<Tuple2<String, Long>> collect = rdd.collect();
				Long max = collect.get(0)._2;
				System.out.println("######### Max: " + max + " ###########");
				return null;
			}
		});

		ssc.start();
		ssc.awaitTermination();
	}

	private static String transform(String s, int wordSleepMilis) {
		try {
			Thread.sleep(wordSleepMilis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return s;
	}
}