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

package com.tagged.pho.phoenix

import com.tagged.pho.converter.{ConverterToken, PhoConverter}
import org.apache.hadoop.hbase.util.Bytes

object PhoenixConverters {

  case object BooleanConverter extends PhoConverter[Boolean] {

    val sizeOf = 1

    def toBytes(value: Boolean): Array[Byte] = value match {
      case false => Array[Byte](0)
      case true => Array[Byte](1)
    }

    def getToken(bytes: Array[Byte]): ConverterToken[Boolean] = {
      val token = bytes.slice(0, sizeOf)
      val value = token.length match {
        case 0 => false
        case _ => token(0) != 0
      }
      ConverterToken(token, value)
    }

  }

  case object ByteConverter extends PhoConverter[Byte] {

    val sizeOf = 1

    def toBytes(value: Byte): Array[Byte] = Array((value ^ Byte.MinValue).toByte)

    def getToken(bytes: Array[Byte]): ConverterToken[Byte] = {
      val token = bytes.slice(0, sizeOf)
      ConverterToken(token, (token(0) ^ Byte.MinValue).toByte)
    }

  }

  case object ShortConverter extends PhoConverter[Short] {

    val sizeOf = 2

    def toBytes(value: Short): Array[Byte] = Bytes.toBytes((value ^ Short.MinValue).toShort)

    def getToken(bytes: Array[Byte]): ConverterToken[Short] = {
      val token = bytes.slice(0, sizeOf)
      ConverterToken(token, (Bytes.toShort(token) ^ Short.MinValue).toShort)
    }

  }

  case object IntConverter extends PhoConverter[Int] {

    val sizeOf = 4

    def toBytes(value: Int): Array[Byte] = Bytes.toBytes(value ^ Int.MinValue)

    def getToken(bytes: Array[Byte]): ConverterToken[Int] = {
      val token = bytes.slice(0, sizeOf)
      ConverterToken(token, Bytes.toInt(token) ^ Int.MinValue)
    }

  }

  case object LongConverter extends PhoConverter[Long] {

    val sizeOf = 8

    def toBytes(value: Long): Array[Byte] = Bytes.toBytes(value ^ Long.MinValue)

    def getToken(bytes: Array[Byte]): ConverterToken[Long] = {
      val token = bytes.slice(0, sizeOf)
      ConverterToken(token, Bytes.toLong(token) ^ Long.MinValue)
    }

  }

  case object StringConverter extends PhoConverter[String] {

    def toBytes(value: String): Array[Byte] = Bytes.toBytes(value)

    def getToken(bytes: Array[Byte]): ConverterToken[String] = {
      ConverterToken(bytes, Bytes.toString(bytes))
    }

  }

}
