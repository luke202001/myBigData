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

package com.myBigData.livy;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

class PiJob implements Job<Double>, Function<Integer, Integer>,
        Function2<Integer, Integer, Integer> {

  private final int samples;

  public PiJob(int samples) {
    this.samples = samples;
  }

  @Override
  public Double call(JobContext ctx) throws Exception {
    List<Integer> sampleList = new ArrayList<Integer>();
    for (int i = 0; i < samples; i++) {
      sampleList.add(i + 1);
    }

    return 4.0d * ctx.sc().parallelize(sampleList).map(this).reduce(this) / samples;
  }

  @Override
  public Integer call(Integer v1) {
    double x = Math.random();
    double y = Math.random();
    return (x*x + y*y < 1) ? 1 : 0;
  }

  @Override
  public Integer call(Integer v1, Integer v2) {
    return v1 + v2;
  }

}


/**
 * Example execution:
 * java -cp /pathTo/spark-core_2.10-*version*.jar:/pathTo/livy-api-*version*.jar:
 * /pathTo/livy-client-http-*version*.jar:/pathTo/livy-examples-*version*.jar
 * org.apache.livy.examples.PiApp http://livy-host:8998 2
 */
public class PiApp {
  public static void main(String[] args) throws Exception {
    String livyUrl = "http://master21:8998";
    Integer  samples = 2;
    String  piJar = "D:\\mySpark\\target\\original-mySpark-1.0-SNAPSHOT.jar";
    LivyClient client = new LivyClientBuilder()
            .setURI(new URI(livyUrl))
            .build();

    try {
      System.err.printf("Uploading %s to the Spark context...\n", piJar);
      client.uploadJar(new File(piJar)).get();

      System.err.printf("Running PiJob with %d samples...\n", samples);
      double pi = client.submit(new PiJob(samples)).get();

      System.out.println("Pi is roughly: " + pi);
    } finally {
      client.stop(true);
    }
  }
}

