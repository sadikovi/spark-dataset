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

import scala.language.experimental.macros
import scala.reflect.macros.Context

class Elem[T] {
  override def toString(): String = "Elem"
}

object Elem {
  def filter[T](p: T => Boolean): T => Boolean = macro impl[T]

  def impl[T: c.WeakTypeTag](c: Context)(p: c.Expr[T => Boolean]): c.Expr[T => Boolean] = {
    import c.universe._
    val t = show(p.tree)
    println(s"Class=${t.getClass}, t=$t")
    p
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    println("Hello!")
  }
}
