package org.apache.spark.streaming.examples.demo

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.util.IntParam
import StreamingContext._
import TwitterDemoHelper._

object TwitterDemo {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: TwitterDemo <master> <# streams> <checkpoint HDFS path>")
      System.exit(1)
    }

    val Array(master, IntParam(numStreams), checkpointPath) = args

    // Create the StreamingContext
    val ssc = new StreamingContext(master, "TwitterDemo", Seconds(1))
    ssc.checkpoint(checkpointPath)

    // Create the streams of tweets
    val tweets = ssc.union(
      authorizations(numStreams).map(oauth => ssc.twitterStream(Some(oauth)))
    )

    // Count the tags over a 1 minute window
    val tagCounts = tweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
                          .countByValueAndWindow(Minutes(1), Seconds(1))

    // Sort the tags by counts
    val sortedTags = tagCounts.map { case (tag, count) => (count, tag) }
                              .transform(_.sortByKey(false))

    // Print top 10 tags
    sortedTags.foreach(showTopTags(20) _)

    ssc.start()
  }
}

