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
package com.holdenkarau.spark.testing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

import scala.collection.immutable.Queue
import scala.collection.mutable

class SampleScalaCheckTest extends FunSuite with SharedSparkContext with Checkers {
  // tag::propertySample[]
  // A trivial property that the map doesn't change the number of elements
  test("map should not change number of elements") {
    val property =
      forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) {
        rdd => rdd.map(_.length).count() == rdd.count()
      }

    check(property)
  }
  // end::propertySample[]
  // A slightly more complex property check using RDDComparisions
  // tag::propertySample2[]
  test("assert that two methods on the RDD have the same results") {
    val property =
      forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) {
        rdd => RDDComparisons.compare(filterOne(rdd), filterOther(rdd)).isEmpty
      }

    check(property)
  }
  // end::propertySample2[]

  test("test custom RDD generator") {

    val humanGen: Gen[RDD[Human]] =
      RDDGenerator.genRDD[Human](sc) {
        val generator: Gen[Human] = for {
          name <- Arbitrary.arbitrary[String]
          age <- Arbitrary.arbitrary[Int]
        } yield (Human(name, age))

        generator
      }

    val property =
      forAll(humanGen) {
        rdd => rdd.map(_.age).count() == rdd.count()
      }

    check(property)
  }

  test("assert rows' types like schema type") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val rowGen: Gen[Row] = DataframeGenerator.getRowGenerator(schema)
    val property =
      forAll(rowGen) {
        row => row.get(0).isInstanceOf[String] && row.get(1).isInstanceOf[Int]
      }

    check(property)
  }

  test("test generating Dataframes") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    val dataframeGen = DataframeGenerator.genDataFrame(sqlContext, schema)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema && dataframe.count >= 0
      }

    check(property)
  }

  test("test custom columns generators") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    val ageGenerator = new ColumnGenerator("age", Gen.choose(10, 100))
    val dataframeGen = DataframeGenerator.genDataFrameWithCustomFields(sqlContext, schema)(ageGenerator)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema && dataframe.filter("age > 100 OR age < 10").count() == 0
      }

    check(property)
  }

  test("test multiple columns generators") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    val nameGenerator = new ColumnGenerator("name", Gen.oneOf("Holden", "Hanafy")) // name should be on of those
    val ageGenerator = new ColumnGenerator("age", Gen.choose(10, 100))
    val dataframeGen = DataframeGenerator.genDataFrameWithCustomFields(sqlContext, schema)(nameGenerator, ageGenerator)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema &&
          dataframe.filter("(name != 'Holden' AND name != 'Hanafy') OR (age > 100 OR age < 10)").count() == 0
      }

    check(property)
  }

  test("generate rdd of specific size") {
    implicit val generatorDrivenConfig =
      PropertyCheckConfig(minSize = 10, maxSize = 20)
    val prop = forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) {
      rdd => rdd.count() <= 20
    }
    check(prop)
  }

  test("test Generating InputDStreams") {
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))
    val generator: Gen[InputDStream[Int]] = DStreamGenerator.genInputDStream[Int](ssc)(Arbitrary.arbitrary[Int])

    val property =
      forAll(generator) {
        stream => {
          println("hello")
          stream.saveAsTextFiles("/tmp/hanafy/")
//          stream.print()
          ssc.start()
          ssc.awaitTerminationOrTimeout(10000)
          ssc.stop(stopSparkContext = false)

          true
        }
      }


    check(property)

    //    val rdd1 = sc.parallelize(List(1, 2, 3))
//    val rdd2 = sc.parallelize(List(4, 5, 6))
//    val rdd3 = sc.parallelize(List(7, 8, 9))
//    val rddQueue: mutable.Queue[RDD[Int]] = mutable.Queue(rdd1, rdd2, rdd3)
//    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, true)


//    inputStream.count().foreachRDD(x => x.foreach(println))
//    inputStream.foreachRDD(rdd => rdd.foreach(println))
    //    inputStream.start()
    //    ssc.start()
    //    ssc.awaitTerminationOrTimeout(10000)
    //
    //
    //    //    println("Here: " + inputStream.count())
    //    println("Hereooooooooooooooooooo")
    //    ssc.stop()
    //    val x = forAll(listGen.arbitrary) {
    //        lst =>
    //      println("List Size: " + lst.size)
    //      lst.foreach(rdd => {
    //        println("Count: " + rdd.count)
    //        println(rdd.collect().mkString(", "))
    //      })
    //
    //      println("==================================")
    //      lst.reverse.reverse == lst
    //    }
    //
    //    val x = forAll((lst: List[List[Int]]) => {
    //      println("List Size: " + lst.size)
    //      lst.foreach(x => {println(x.size)} )
    //      println("=========================")
    //      lst.reverse.reverse == lst
    //    }
    //    )
    //
    //    check(x)
  }

  private def filterOne(rdd: RDD[String]): RDD[Int] = {
    rdd.filter(_.length > 2).map(_.length)
  }

  private def filterOther(rdd: RDD[String]): RDD[Int] = {
    rdd.map(_.length).filter(_ > 2)
  }
}

case class Human(name: String, age: Int)
