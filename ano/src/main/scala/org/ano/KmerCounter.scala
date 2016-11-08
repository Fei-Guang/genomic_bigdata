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
package org.ano

import scala.math.random

import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.serializer.KryoSerializer
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator


/** Kmer counter */
object KmerCounter {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .config("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .appName("Anno BDG")
      .getOrCreate()

    //set new runtime options
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")
    //spark.conf.set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    //spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = spark.sparkContext
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //get all settings
    //sc.conf.set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    val configMap:Map[String, String] = spark.conf.getAll
    //configMap.foreach(println)

    val reads = sc.loadAlignments("/data/sample.rmdup.bam")
    val kmers = sc.textFile("/data/win_100k.use_50mer")
      .map(line => {
        // get the range from the rdd2.kmer file
        val columns = line.split("\\s+") // i assume this is tab delimited?
        val contig = columns(0)
        val start = columns(4).toLong
        val end = columns.last.toLong
        (contig, start, end)
      })

    kmers.toDF("name","min","max").registerTempTable("t1")
    //kmers.take(50).foreach(println)
    // join against the other RDD keyed by contig name
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
    spark.sql("""SELECT A.name, A.min, A.max, SUM(CASE WHEN A.name = B.name AND (B.value BETWEEN A.min AND A.max) THEN 1 ELSE 0 END) AS countofoccourcetable2value FROM t1 A INNER JOIN t2 B ON A.name = B.name GROUP BY A.name, A.min, A.max""").show
    //countsByChromosome.foreach(println)
    //countsByChromosome.saveAsTextFile("/data/result.txt")
    spark.stop()
  }
}
// scalastyle:on println
