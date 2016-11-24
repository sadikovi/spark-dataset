/*
 * Copyright 2016 sadikovi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sadikovi.spark

import scala.reflect.runtime.universe._
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.github.sadikovi.spark.implicits._

class TestSuite extends FunSuite with Matchers {
  test("run") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    val ds = spark.range(10)
    val res = ds.gfilter((s: java.lang.Long) => s > 4)
    println(s"Result = $res")
    res.explain(true)
    res.show()
  }
}
