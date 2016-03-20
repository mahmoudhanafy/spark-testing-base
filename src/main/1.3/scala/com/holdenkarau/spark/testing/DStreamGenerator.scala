package com.holdenkarau.spark.testing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalacheck.{Arbitrary, Gen}

import scala.reflect.ClassTag

object DStreamGenerator {
  /**
   * Creates an InputDStream generator.
   *
   * @param ssc          Streaming Context.
   * @param oneAtTime    Create RDD per duration or create them all at once.
   * @param getGenerator used to create the generator. This function will be used to create the generator as
   *                     many times as required.
   * @tparam T The required type for the DStream
   * @return Gen of InputDStream[T]
   */
  def genInputDStream[T: ClassTag](ssc: StreamingContext, oneAtTime: Boolean = false)
      (getGenerator: => Gen[T]): Gen[InputDStream[T]] = {

    implicit val gen = Arbitrary(RDDGenerator.genRDD[T](ssc.sparkContext)(getGenerator))
    val queueGen = Arbitrary(Arbitrary.arbitrary[scala.collection.mutable.Queue[RDD[T]]])
    val inputGen: Gen[InputDStream[T]] = queueGen.arbitrary.map(queue => ssc.queueStream(queue, oneAtTime, null))

    inputGen
  }
}
