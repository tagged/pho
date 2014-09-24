/*
 * Copyright 2014 Tagged
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

package com.tagged.pho

import org.apache.hadoop.hbase.util.Bytes

/**
 * Base class used to express HBase column families and qualifiers.
 * By encapsulating the raw bytes of the qualifier,
 * the number of conversions to/from strings should be reduced
 * particularly in large scan results.
 */
class Identifier(private val bytes: Array[Byte]) {

  def this(name: String) = this(Identifier.bytesFromString(name))

  /**
   * Deep hash value based on the actual bytes in the array.
   * Required for object equality test.
   */
  override lazy val hashCode: Int = Bytes.hashCode(bytes)

  /**
   * Deep equality test based on the bytes in the array.
   * Required for object equality test.
   */
  override def equals(that: Any): Boolean = {
    eq(that.asInstanceOf[Object]) ||
      (that match {
        case that: Identifier => this.bytes.deep == that.bytes.deep
        case _ => false
      })
  }

  /**
   * The identifier converted into a String.
   */
  lazy val name = Identifier.stringFromBytes(bytes)

  /**
   * Human readable output of the identifier.
   * Subclasses may use this method in place of the auto-generated case class toString method,
   * which would output the raw byte values.
   */
  override def toString: String = {
    val className = this.getClass.getSimpleName
    s"$className($name)"
  }

}

object Identifier {

  def bytesFromString(name: String) = Bytes.toBytes(name)

  def stringFromBytes(bytes: Array[Byte]) = Bytes.toString(bytes)

}
