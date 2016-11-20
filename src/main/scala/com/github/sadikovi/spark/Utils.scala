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

import java.io.{File, FileOutputStream, IOException, InputStream}

object Utils {
  /** Get default class loader that was used to load this class */
  def getDefaultClassLoader: ClassLoader = {
    getClass.getClassLoader
  }

  /** Get current thread class loader or fall back to default one */
  def getAvailableClassLoader: ClassLoader = {
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getDefaultClassLoader)
  }

  /** Read resource for a given name/path */
  def readResource(name: String): InputStream = {
    getAvailableClassLoader.getResourceAsStream(name)
  }

  def writeResource(path: String, arr: Array[Byte]): Unit = {
    writeResource(new File(path), arr)
  }

  def writeResource(file: File, arr: Array[Byte]): Unit = {
    if (file.exists()) {
      throw new IOException(s"File $file already exists")
    }

    val out: FileOutputStream = new FileOutputStream(file)
    try {
      out.write(arr)
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }
}
