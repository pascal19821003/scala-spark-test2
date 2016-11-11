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
package org.apache.spark.examples

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random
case class Person(name:String,col1:String,col2:String)
/**
  * Transitive closure on a graph.
  */
object SparkReadHdfs {
  val numEdges = 5
  val numVertices = 5
  val rand = new Random(42)

  def generateGraph: Seq[(Int, Int)] = {
    val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
    while (edges.size < numEdges) {
      val from = rand.nextInt(numVertices)
      val to = rand.nextInt(numVertices)
      if (from != to) edges.+=((from, to))
    }
    edges.toSeq
  }


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkTC").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val tableName = "tb_avato"
    val csvFilePath = "C:\\Users\\Pascal\\Desktop\\kafka-0.10.0.1-src\\scala-spark-test2\\src\\main\\scala\\org\\apache\\spark\\examples\\avato.csv"
    val lines = sc.textFile("file:\\" + csvFilePath)

    val firstLine = lines.first()
    println("first line:"+firstLine)

    val dataRdd: RDD[String] = lines.subtract(sc.parallelize(List(firstLine)))

    dataRdd.foreach(line => println("line:"+line))

    val colArr: Array[String] = firstLine.split(",")

    val hiveContext = new HiveContext(sc)

    hiveContext.sql("use wuxibank")

    import hiveContext.implicits._

    //val data = sc.textFile("path").map(x=>x.split("\\s+")).map(x=>Person(x(0),x(1).toInt,x(2)))
     val personRdd: RDD[Person] = dataRdd.map(x=>x.split(",")).map(x=>Person(x(0),x(1), x(2)))



    val df: DataFrame = personRdd.toDF()

    df.show()

    //df.registerTempTable(tableName)





    //    val pairs = lines.map(line=>(line,1))
    //    val counts = pairs.reduceByKey((a,b) => a+b)
    //    println
    //    counts.foreach(tuple=>{
    //      println(tuple._1 + " > "+ tuple._2)
    //    })


//    val slices = if (args.length > 0) args(0).toInt else 2
//    var tc = spark.parallelize(generateGraph, slices).cache()
//
//    // Linear transitive closure: each round grows paths by one edge,
//    // by joining the graph's edges with the already-discovered paths.
//    // e.g. join the path (y, z) from the TC with the edge (x, y) from
//    // the graph to obtain the path (x, z).
//
//    // Because join() joins on keys, the edges are stored in reversed order.
//    val edges = tc.map(x => (x._2, x._1))
//
//    println("tc:")
//    tc.foreach(xy=>println(xy._1 + " " + xy._2))
//    //////////////
//    println("edges:")
//    edges.foreach((x )=>println(x._1 +  " " +x._2))
//
//    ///////////////
//    println("join:")
//    tc.join(edges).foreach((x)=>println(x._1 + " ("+x._2._1+ " , " + x._2._2+")"))
//
//    // This join is iterated until a fixed point is reached.
//    var oldCount = 0L
//    var nextCount = tc.count()
////    do {
//      //      oldCount = nextCount
//      //      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
//      //      // then project the result to obtain the new (x, z) paths.
//     tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct().cache()
//      //      println("=============")
//      //      tc.foreach(t=>println(t._1 + " " + t._2))
//      //      nextCount = tc.count()
//      //    } while (nextCount != oldCount)
//
//    println("TC has " + tc.count() + " edges.")
    sc.stop()
  }
}
// scalastyle:on println