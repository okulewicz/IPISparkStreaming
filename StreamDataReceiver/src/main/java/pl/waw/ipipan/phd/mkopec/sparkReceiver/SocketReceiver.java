package pl.waw.ipipan.phd.mkopec.sparkReceiver;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
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

public final class SocketReceiver {
	private static final Duration CHECKPOINT_INTERVAL = new Duration(1000 * 60 * 60 * 24);
	private static final String CHECKPOINTS_DIR = "checkpoints";
	private static final Pattern SEPARATOR = Pattern.compile("[\n ]+", Pattern.MULTILINE);

	private static final Logger LOG = Logger.getLogger(SocketReceiver.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		if (args.length < 4) {
			LOG.error("Usage: " + SocketReceiver.class.getSimpleName()
					+ " <hostname> <port> <batch_time_milis> <per_number_sleep_time_milis>");
			return;
		}

		try {
			Integer.parseInt(args[1]);
			Integer.parseInt(args[2]);
			Integer.parseInt(args[3]);
		} catch (NumberFormatException ex) {
			LOG.error("Error parsing program arguments: " + ex);
			return;
		}

		final String host = args[0];
		final int port = Integer.parseInt(args[1]);
		final int batchSizeMilis = Integer.parseInt(args[2]);
		final int wordSleepMilis = Integer.parseInt(args[3]);

		SparkConf sparkConf = new SparkConf().setAppName(SocketReceiver.class.getSimpleName());
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(batchSizeMilis));
		ssc.checkpoint(CHECKPOINTS_DIR); // needed for maintaining state

		JavaReceiverInputDStream<String> lines = ssc.receiverStream(new CustomSocketReceiver(host, port));

		// create a stream of words
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x) {
				return Lists.newArrayList(SEPARATOR.split(x));
			}
		});

		// filter empty and not long words and parse others into Long
		JavaDStream<Long> numbers = words.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String x) throws Exception {
				try {
					Long.valueOf(x);
				} catch (NumberFormatException ex) {
					LOG.error("Unable to parse number: " + ex);
					return false;
				}
				return true;
			}
		}).map(new Function<String, Long>() {
			@Override
			public Long call(String x) throws Exception {
				sleep(wordSleepMilis);
				return Long.valueOf(x);
			}
		});

		// map each long X to pair: "distinct": set(X)
		JavaPairDStream<String, Set<Long>> setsStream = numbers.mapToPair(new PairFunction<Long, String, Set<Long>>() {
			@Override
			public Tuple2<String, Set<Long>> call(Long n) {
				Set<Long> set = new HashSet<>();
				set.add(n);
				return new Tuple2<String, Set<Long>>("distinct", set);
			}
		});

		// create a stream of states, each containing a set of distinct longs
		// seen up to date
		JavaPairDStream<String, Set<Long>> mergedSets = setsStream
				.updateStateByKey(new Function2<List<Set<Long>>, Optional<Set<Long>>, Optional<Set<Long>>>() {
					@Override
					public Optional<Set<Long>> call(List<Set<Long>> values, Optional<Set<Long>> state) {
						Set<Long> result = state.or(new HashSet<Long>());
						for (Set<Long> set : values)
							result.addAll(set);
						return Optional.of(result);
					}
				});

		// print state for each rdd
		mergedSets.foreachRDD(new Function<JavaPairRDD<String, Set<Long>>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, Set<Long>> rdd) throws Exception {
				if (rdd.count() != 1) {
					System.out.println("\n############## Empty RDD");
					return null;
				}
				List<Tuple2<String, Set<Long>>> collect = rdd.collect();
				Set<Long> set = collect.get(0)._2;
				System.out.println("\n############## Distinct numbers up to date: " + set.size());
				System.out.println("############## Numbers up to date: " + new TreeSet<Long>(set));
				return null;
			}
		});

		// we need to manually set long time for checkpoint, as it won't work
		words.checkpoint(CHECKPOINT_INTERVAL);
		numbers.checkpoint(CHECKPOINT_INTERVAL);
		setsStream.checkpoint(CHECKPOINT_INTERVAL);
		mergedSets.checkpoint(CHECKPOINT_INTERVAL);

		ssc.start();
		ssc.awaitTermination();
	}

	private static void sleep(int wordSleepMilis) {
		try {
			Thread.sleep(wordSleepMilis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}