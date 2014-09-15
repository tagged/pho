package com.tagged.morice

import com.tagged.morice.MoConverter._
import com.tagged.morice.PhoenixConversions.LongConverter
import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification

class MoConverterSpec extends Specification with DataTables {

  "IdentityConverter" should {

    "leave all input untouched" in {
      "bytes"               |>
      Array[Byte](23)       |
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
      "bytes"               | "expectedInversion"  |>
      Array[Byte](23)       ! Array(~23)           |
      Array[Byte](-3, 4, 3) ! Array(~(-3), ~4, ~3) | { (bytes, expectedInversion) =>
        val converter = InvertConverter(IdentityConverter)
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
