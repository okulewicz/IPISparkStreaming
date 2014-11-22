package pl.waw.ipipan.phd.mkopec.sparkReceiver;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public final class TwitterReceiver {

	private static final String CHECKPOINTS_DIR = "../checkpoints";
	private static final String[] KEYWORDS = new String[] {};
	private static final int BATCH_SIZE_MILIS = 5000;
	private static final int SLEEP_PER_HASHTAG_MILIS = 1;

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("TwitterExample");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				new Duration(BATCH_SIZE_MILIS));
		ssc.checkpoint(CHECKPOINTS_DIR);

		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils
				.createStream(ssc, KEYWORDS, StorageLevels.MEMORY_ONLY);

//		JavaDStream<String> hashtags = twitterStream.flatMap(
//				tweet -> Lists.newArrayList(tweet.getHashtagEntities())).map(
//				he -> transform(he.getText()));
//
//		JavaPairDStream<String, Integer> hashtagCounts = hashtags.mapToPair(
//				s -> new Tuple2<String, Integer>(s, 1)).reduceByKey(
//				(x, y) -> x + y);
//
//		JavaPairDStream<String, Integer> totalCounts = hashtagCounts
//				.updateStateByKey((values, state) -> {
//					Integer total = state.or(0)
//							+ values.stream().reduce(0, (a, b) -> a + b);
//					return Optional.of(total);
//				});
//
//		totalCounts
//				.foreachRDD(rdd -> {
//					Stream<Tuple2<String, Integer>> sorted = rdd.collect()
//							.stream()
//							.sorted((o1, o2) -> -o1._2.compareTo(o2._2));
//					Stream<Tuple2<String, Integer>> top5 = sorted.limit(5);
//					System.out
//							.println("############## Top 5 hashtags ###############");
//					top5.forEach(t -> System.out.println("\t" + t._1 + "\t"
//							+ t._2));
//					System.out
//							.println("#############################################");
//					return null;
//				});

		ssc.start();
		ssc.awaitTermination();
	}

	private static String transform(String text) {
		try {
			Thread.sleep(SLEEP_PER_HASHTAG_MILIS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return text;
	}
}