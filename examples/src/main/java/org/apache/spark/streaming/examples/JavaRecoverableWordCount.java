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

package org.apache.spark.streaming.examples;

import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.nio.charset.Charset;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

/**
 * Counts words in text encoded with UTF8 received from the network every second.
 *
 * Usage: JavaRecoverableWordCount <master> <hostname> <port> <checkpoint-directory> <output-file>
 *   <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
 *   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *   <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data
 *   <output-file> file to which the word counts will be appended
 *
 * In local mode, <master> should be 'local[n]' with n > 1
 * <checkpoint-directory> and <output-file> must be absolute paths
 *
 * To run this on your local machine, you need to first run a Netcat server
 *
 *      `$ nc -lk 9999`
 *
 * and run the example as
 *
 *      `$ ./bin/run-example org.apache.spark.streaming.examples.JavaRecoverableWordCount \
 *              local[2] localhost 9999 ~/checkpoint/ ~/out`
 *
 * If the directory ~/checkpoint/ does not exist (e.g. running for the first time), it will create
 * a new JavaStreamingContext (will print "Creating new context" to the console). Otherwise, if
 * checkpoint data exists in ~/checkpoint/, then it will create JavaStreamingContext from
 * the checkpoint data.
 *
 * The output of the computation will be appended to the output file `~/out`.
 *
 * To run this example in a local standalone cluster with the above Netcat server and automatic driver recovery
 *
 *      `$ ./bin/spark-class org.apache.spark.deploy.Client -s launch <cluster-url> <path-to-examples-jar> \
 *              org.apache.spark.streaming.examples.JavaRecoverableWordCount <cluster-url> \
 *              localhost 9999 ~/checkpoint ~/out`
 *
 * <cluster-url> is your local standalone cluster's URL
 * <path-to-examples-jar> would typically be file://<spark-dir>/examples/target/scala-XX/spark-examples....jar
 *
 * Refer to the online documentation for more details on launching standalone cluster with supervise mode
 * for automatic driver recovery.
 */

public class JavaRecoverableWordCount {

  // Factory class for generating JavaStreamingContext
  static public class WordCountContextFactory implements JavaStreamingContextFactory,  Serializable {
    String master;
    String ip;
    int port;
    String checkpointDir;
    String outputPath;

    public WordCountContextFactory(String master_ , String ip_ , int port_ , String checkpointDir_, String outputPath_ ) {
      master = master_;
      ip = ip_;
      port = port_;
      checkpointDir = checkpointDir_;
      outputPath = outputPath_;
    }

    public JavaStreamingContext create() {

      // If you do not see this printed, that means the StreamingContext has been loaded
      // from the new checkpoint
      System.out.println("Creating new context");
      final File outputFile = new File(outputPath);
      if (outputFile.exists()) outputFile.delete();

      // Create the context with a 1 second batch size
      JavaStreamingContext jssc = new JavaStreamingContext(master, "JavaRecoverableWordCount",
          new Duration(1000), System.getenv("SPARK_HOME"),
          JavaStreamingContext.jarOfClass(JavaNetworkWordCount.class)
      );

      // Create a socket text stream on target ip:port and count the
      // words in the input stream of \n delimited text (eg. generated by 'nc')
      JavaDStream<String> lines = jssc.socketTextStream(ip, port);
      JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
        @Override
        public Iterable<String> call(String x) {
          return Lists.newArrayList(x.split(" "));
        }
      });
      JavaPairDStream<String, Integer> wordCounts = words.map(
          new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
              return new Tuple2<String, Integer>(s, 1);
          }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });

      wordCounts.foreachRDD(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {
        @Override public Void call(JavaPairRDD<String, Integer> pairRDD, Time time) throws Exception {
          String counts = "Counts at time " + time + " [" + Joiner.on(", ").join(pairRDD.collect())  + "]";
          System.out.println(counts);
          System.out.println("Appending to " + outputFile.getAbsolutePath());
          Files.append(counts + "\n", outputFile, Charset.defaultCharset());
          return null;
        }
      });
      jssc.checkpoint(checkpointDir);
      return jssc;
    }
  }

  public static void main(String[] args) {
    if (args.length != 5) {
      System.err.println("You arguments were [" + Joiner.on(", ").join(args) + "]");
      System.err.println(
        "Usage: JavaRecoverableWordCount <master> <hostname> <port> <checkpoint-directory> <output-file>\n" +
        "     <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.\n" +
        "     <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.\n" +
        "     <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data\n" +
        "     <output-file> file to which the word counts will be appended\n\n" +
        "In local mode, <master> should be 'local[n]' with n > 1\n" +
        "Both <checkpoint-directory> and <output-file> must be absolute paths\n"
      );
      System.exit(1);
    }

    String master = args[0];
    String ip = args[1];
    int port = Integer.parseInt(args[2]);
    String checkpointDirectory = args[3];
    String outputPath = args[4];

    JavaStreamingContextFactory factory = new WordCountContextFactory(master, ip, port, checkpointDirectory, outputPath);
    JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
    jssc.start();
    jssc.awaitTermination();
  }
}
