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

import java.io.File

import scala.language.experimental.macros
import scala.reflect.macros.Context

import org.apache.spark.sql.Dataset

object DatasetOperations {
  /** Return list of directories where generated data can be stored */
  private def getClassPaths(c: Context): Seq[File] = {
    c.classPath.map { url => new File(url.toURI) }.filter { _.isDirectory }
  }

  /** Get final class path as optional, might not exist */
  private def getClassPath(c: Context): Option[File] = {
    val list = getClassPaths(c).filter { _.getName != "test-classes" }
    // discard 'test-classes' path
    list.headOption
  }

  /** Generate unique name for the object hash */
  def generateName(name: Any): String = {
    val suffix = "tree.gen"
    val prefix = "func"
    s"$prefix.$name.$suffix"
  }

  def doFilterGenerate[T](ds: Dataset[T])(func: T => Boolean): Dataset[T] =
    macro macro_doFilterGenerate[T]

  def macro_doFilterGenerate[T: c.WeakTypeTag](c: Context)(ds: c.Expr[Dataset[T]])(func: c.Expr[T => Boolean]): c.Expr[Dataset[T]] = {
    import c.universe._
    val t = show(func.tree)
    // reconstruct function call
    val funcCall = Select(ds.tree, newTermName("filter"))
    val funcArg = List(func.tree)
    val returnExpr = c.Expr[Dataset[T]](Apply(funcCall, funcArg))

    val path = getClassPath(c).map { file =>
      file.toString + File.separator + generateName(func) }
    println(s"Class=${returnExpr.getClass}, func=$func, path=$path")
    if (path.isEmpty) {
      println("Path is not defined, ignore writing to disk")
    } else {
      println(s"Writing to ${path.get}")
      // Utils.writeResource(path.get, t.getBytes)
    }
    returnExpr
  }
}
