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

package com.tagged.pho.converter

import com.tagged.pho.phoenix.PhoenixConverters._
import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification

import scala.runtime.BoxedUnit

class PhoConverterSpec extends Specification with DataTables {

  "EmptyConverter" should {

    "convert from Unit to empty array" in {
      EmptyConverter.getValue(Array[Byte]()) must beAnInstanceOf[BoxedUnit]
      EmptyConverter.getValue(Array[Byte](23, 0, 12)) must beAnInstanceOf[BoxedUnit]
      EmptyConverter.toBytes() must beEqualTo(Array[Byte]())
    }

  }

  "IdentityConverter" should {

    "leave all input untouched" in {
      "bytes" |>
        Array[Byte](23) |
        Array[Byte](-3, 4, 3) | { (bytes: Array[Byte]) =>
        val toBytesResult = IdentityConverter.toBytes(bytes)
        val getValueResult = IdentityConverter.getValue(bytes)
        toBytesResult must beEqualTo(bytes)
        getValueResult must beEqualTo(bytes)
      }
    }

  }

  "InvertConverter" should {

    "symmetrically convert" in {
      "bytes" | "expectedInversion" |>
        Array[Byte](23) ! Array(~23) |
        Array[Byte](-3, 4, 3) ! Array(~(-3), ~4, ~3) | { (bytes, expectedInversion) =>
        val converter = InverseConverter(IdentityConverter)
        val toBytesResult = converter.toBytes(bytes)
        val getValueResult = converter.getValue(toBytesResult)
        toBytesResult must beEqualTo(expectedInversion)
        getValueResult must beEqualTo(bytes)
      }
    }

  }

  "Tuple2Converter" should {

    "symmetrically convert" in {
      val input = (3L, -3L)
      val expectedBytes = Array[Byte](-128, 0, 0, 0, 0, 0, 0, 3, 127, -1, -1, -1, -1, -1, -1, -3)
      val converter = Tuple2Converter(LongConverter, LongConverter)
      val bytes = converter.toBytes(input)
      val result = converter.getValue(bytes)
      bytes must beEqualTo(expectedBytes)
      result must beEqualTo(input)
    }

  }

}
