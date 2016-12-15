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

// scalastyle:off println
package com.ano.adam

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.rdd.ADAMContext._


/** Count kmer in the specified format */
object AnnoSp {
  def main(args: Array[String]) {
    println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn")));
    val spark = SparkSession.builder
      .config("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.submit.deployMode", "client")
      .config("spark.executor.memory", "2000t")
      .config("spark.driver.memory", "4000t")
      //.config("spark.yarn.jars", "")
      .master("local[*]")
      //.config("spark.master", "yarn")
      //.master("yarn")
      .config("spark.app.name", "Anno BDG")
      .appName("Anno BDG")
      .getOrCreate()

    val sc = spark.sparkContext
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "20000000g")
    spark.conf.set("spark.driver.memory", "400000000g")
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //val reads = sc.loadAlignments("/data/sample.rmdup.bam")
    val reads = sc.loadAlignments("/data/sample.rmdup.adam")
    val kmers = sc.textFile("/data/win_100k.use_50mer", 5).map(line => {
        // get the range from the rdd2.kmer file
        val columns = line.split("\\s+") // spaces delimited
        val contig = columns(0)
        val start = columns(4).toLong
        val end = columns.last.toLong
        (contig, start, end)
      })

    kmers.toDF("name","min","max").registerTempTable("t1")
    //kmers.take(50).foreach(println)
    // join against the other RDD keyed by contig name
    println("number of partitions for adam:" + reads.rdd.getNumPartitions)
    println("number of partitions for adam:" + kmers.getNumPartitions)
    val samples = reads.rdd.flatMap(read => {
      // check whether the read is mapped, lest we get a null pointer exception
      if (read.getReadMapped) {
        Some((read.getContigName, read.getStart))
      } else {
        None
      }
    })
    samples.toDF("name", "value").registerTempTable("t2")
    //samples.take(50).foreach(println)
    spark.sql(
      """SELECT A.name, A.min, A.max, SUM(CASE WHEN A.name = B.name AND
        |(B.value BETWEEN A.min AND A.max) THEN 1 ELSE 0 END) AS counter
        |FROM t1 A INNER JOIN t2 B ON A.name = B.name
        |GROUP BY A.name, A.min, A.max""".stripMargin).show
    println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn")));
    spark.stop()
  }
}
// scalastyle:on println
