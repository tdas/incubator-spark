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

package org.apache.spark.streaming.examples

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.SparkContext

/**
 * Clusters tweets by language using MLlib's K-Means algorithm and a bigram-based feature model.
 *
 * We want to cluster these tweets by language and be able to recognize other
 * tweets in the same language later. Twitter actually provides a “client language”
 * field on each tweet, but it’s often “English” because peoples’ browsers are
 * English even if they tweet in a different one.
 *
 * You can run this program from the root of the Spark directory with the following command
 * (create and use your own twitter credentials from https://dev.twitter.com/apps)
 * <code>
 * ./run-example \
 * -Dtwitter4j.oauth.consumerKey=bsFgduP6xlqpX8MeDw \
 * -Dtwitter4j.oauth.consumerSecret=ASCy5FVCoZwbPj89QXOfNQJL3R3hq42QGVPwC0 \
 * -Dtwitter4j.oauth.accessToken=9184252-aX2L5jGyBFi28HpwKsLZoGOpTxy6hVR5TjdBfliDls \
 * -Dtwitter4j.oauth.accessTokenSecret=U0gQ4otrlZmhNf1ec1zXbtAnI2pAiBvAIrHDBxsLI \
 * org.apache.spark.streaming.examples.TwitterClusteringLanguages \
 * local[4]
 * </code>
 *
 */
object TwitterClusteringLanguages {
  def main(args: Array[String]) {


    //Process program arguments and set properties
    if (args.length < 1) {
      System.err.println("Usage: " + this.getClass.getCanonicalName + " <master>")
      System.exit(1)
    }
    val (master, filters) = (args.head, args.tail)
    System.setProperty("spark.cleaner.ttl","3600")

    //Initialize Spark and SparkStreaming contexts
    val sc = new SparkContext(
      master,
      "Streaming Classification",
      System.getenv("SPARK_HOME"),
      Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    val ssc = new StreamingContext(sc, Seconds(5))

    //Grab Tweets from file and process them
    //TODO: make tweets available publicly
    val file = sc.textFile("tweets")
    val texts = file.map(line => line.split('\t')(5))

    //Featurize the existing tweets then build the model using MLlib
    val vectors = texts.map(featurize)
    //TODO: make the modeling parameters program arguments
    //TODO: break out model training to a separate process
    val model = KMeans.train(vectors, 10, 10)

    //Create the stream
    val stream = ssc.twitterStream(None, filters)

    //Cluster incoming tweets using the previously built model,
    // then only output the tweets in the 9th cluster
    stream.filter(
      status => model.predict(featurize(status.getText)) == 9
    ).map(_.getText).print()
    ssc.start()
  }

  /**
   * Create feature vectors by turning each tweet into bigrams of characters (an n-gram model)
   * and then hashing those to a length-1000 feature vector that we can pass to MLlib.
   * This is a common way to decrease the number of features in a model while still
   * getting excellent accuracy (otherwise every pair of Unicode characters would
   * potentially be a feature).
   */

  def featurize(s: String): Array[Double] = {
    val lower = s.toLowerCase.filter(c => c.isLetter || c.isSpaceChar)
    val result = new Array[Double](1000)
    val bigrams = lower.sliding(2).toArray
    for (h <- bigrams.map(_.hashCode % 1000)) {
      result(h) += 1.0 / bigrams.length
    }
    result
  }

}
