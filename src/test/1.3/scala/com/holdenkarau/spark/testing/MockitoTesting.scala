package com.holdenkarau.spark.testing

import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class MockitoTesting extends FunSuite with BeforeAndAfter with MockitoSugar {

  test("mocking setup clock") {
    val commonSuite = mock[StreamingSuiteCommon]
    when(commonSuite.setupClock()).thenReturn(new SparkConf().set("name", "hanafy"))

    val clock = commonSuite.setupClock()
    assert(clock.get("name") == "hanafy")
  }

  test("mocking ") {
    val commonSuite = mock[StreamingSuiteCommon]
//    commonSuite.setupStreams()
  }

}
