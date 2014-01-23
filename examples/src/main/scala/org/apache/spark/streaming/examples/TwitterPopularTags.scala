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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.Logging
import org.apache.spark.ui.JettyUtils
import org.apache.spark.ui.JettyUtils._
import scala.collection.mutable.ArrayBuffer

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 */
object TwitterPopularTags {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: TwitterPopularTags <master>" +
        " [filter1] [filter2] ... [filter n]")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val (master, filters) = (args.head, args.tail)

    val ssc = new StreamingContext(master, "TwitterPopularTags", Seconds(2),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val ui = new TwitterPopularTagsUI()

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(20)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
      ui.updateData(topList.map(_.swap))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}


class TwitterPopularTagsUI(port: Int = 6060) extends Logging {
  val handler = (request: HttpServletRequest) => this.render(request)
  val tagFreqData = ArrayBuffer[(String, Int)]()
  JettyUtils.startJettyServer("0.0.0.0", port, Seq( ("/", handler) ))

  def updateData(newData: Seq[(String, Int)]) {
    tagFreqData.clear()
    tagFreqData ++= newData
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    <html>
      <head>
        <meta http-equiv="refresh" content="1"/>
      </head>
      <body>
        <table class="table table-bordered table-striped table-condensed sortable table-fixed">
          <thead><th width="80%" align="left">Tag</th><th width="20%">Frequency</th></thead>
          <tbody>
            {tagFreqData.map(r => makeRow(r))}
          </tbody>
        </table>
      </body>
    </html>
  }

  def makeRow(tagFreq: (String, Int)): Seq[Node] = {
    <tr>
      <td>
        {tagFreq._1}
      </td>
      <td>
        {tagFreq._2}
      </td>
    </tr>
  }
}


