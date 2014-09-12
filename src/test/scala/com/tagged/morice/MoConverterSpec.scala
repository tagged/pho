package com.tagged.morice

import com.tagged.morice.MoConverter.{InvertConverter, IdentityConverter}
import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification

class MoConverterSpec extends Specification with DataTables {

  "IdentityConverter" should {

    "leave all input untouched" in {
      "bytes"               |>
      Array[Byte](23)       |
      Array[Byte](-3, 4, 3) | { (bytes: Array[Byte]) =>
        val getBytesResult = IdentityConverter.getBytes(bytes)
        val getValueResult = IdentityConverter.getValue(bytes)
        getBytesResult must beEqualTo(bytes)
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
        val getBytesResult = converter.getBytes(bytes)
        val getValueResult = converter.getValue(getBytesResult)
        getBytesResult must beEqualTo(expectedInversion)
        getValueResult must beEqualTo(bytes)
      }
    }

  }

}
