package pl.waw.ipipan.phd.mkopec.sparkReceiver;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

import com.google.common.base.Optional;

public final class TwitterReceiver {

	private static final Duration CHECKPOINT_INTERVAL = new Duration(1000 * 60 * 60 * 24);
	private static final String CHECKPOINTS_DIR = "checkpoints";
	private static final Pattern SEPARATOR = Pattern.compile("[\n ]+", Pattern.MULTILINE);

	private static final Logger LOG = Logger.getLogger(TwitterReceiver.class);

	protected static final int TOP_COUNT = 5;

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		if (args.length < 2) {
			LOG.error("Usage: " + TwitterReceiver.class.getSimpleName()
					+ " <batch_time_milis> <per_word_sleep_time_milis> <comma separated keywords>");
			return;
		}

		try {
			Integer.parseInt(args[0]);
			Integer.parseInt(args[1]);
		} catch (NumberFormatException ex) {
			LOG.error("Error parsing program arguments: " + ex);
			return;
		}

		final int batchSizeMilis = Integer.parseInt(args[0]);
		final int wordSleepMilis = Integer.parseInt(args[1]);

		String[] keywords = new String[] {};
		if (args.length > 2)
			keywords = args[2].split(",");

		SparkConf sparkConf = new SparkConf().setAppName(TwitterReceiver.class.getSimpleName());
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(batchSizeMilis));
		ssc.checkpoint(CHECKPOINTS_DIR);

		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(ssc, keywords,
				StorageLevels.MEMORY_ONLY);

		JavaPairDStream<String, Long> counts = twitterStream
				.flatMap(tweet -> Arrays.asList(SEPARATOR.split(tweet.getText()))).filter(word -> {
					sleep(wordSleepMilis);
					return !StringUtils.isBlank(word);
				}).mapToPair(word -> new Tuple2<String, Long>(word, 1L)).reduceByKey((x, y) -> x + y)
				.updateStateByKey((values, state) -> {
					Long total = state.or(0L);
					for (Long v : values)
						total += v;
					return Optional.of(total);
				});

		// print state for each rdd
		counts.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				if (rdd.count() == 0) {
					System.out.println("##############  Empty RDD");
					return null;
				}
				List<Tuple2<String, Long>> list = rdd.collect();
				Collections.sort(list, new Comparator<Tuple2<String, Long>>() {
					@Override
					public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
						return -o1._2.compareTo(o2._2);
					}
				});
				System.out.println("\n############## Top " + TOP_COUNT + " words");
				for (Tuple2<String, Long> tuple : list.subList(0, Math.min(list.size(), TOP_COUNT)))
					System.out.println(tuple._1 + "\t" + tuple._2);
				System.out.println("############## ");
				return null;
			}
		});

		// we need to manually set long time for checkpoint, as it won't work
		counts.checkpoint(CHECKPOINT_INTERVAL);
		
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