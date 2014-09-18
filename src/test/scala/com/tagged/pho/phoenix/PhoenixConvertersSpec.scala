package com.tagged.pho.phoenix

import com.tagged.pho.phoenix.PhoenixConverters._
import org.apache.hadoop.hbase.util.Bytes
import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification

class PhoenixConvertersSpec extends Specification with DataTables {

  implicit object LexicographicOrdering extends Ordering[Array[Byte]] {
    def compare(a:Array[Byte], b:Array[Byte]) = Bytes.compareTo(a, b)
  }

  "BooleanConverter" should {

    "convert symmetrically" in {
      "value" | "expectedBytes" |>
      false   ! Array(0)        |
      true    ! Array(1)        | { (value, expectedBytes ) =>
        val bytes = BooleanConverter.toBytes(value)
        val result = BooleanConverter.getValue(bytes)
        bytes must beEqualTo(expectedBytes)
        result must beEqualTo(value)
      }
    }

  }

  "ByteConverter" should {

    "convert symmetrically" in {
      "value"          | "expectedBytes" |>
      0.toByte         ! Array(-128)     |
      Byte.MaxValue    ! Array(  -1)     |
      Byte.MinValue    ! Array(   0)     |
      -3.toByte        ! Array( 125)     |
      3.toByte         ! Array(-125)     | { (value, expectedBytes) =>
        val bytes = ByteConverter.toBytes(value)
        val result = ByteConverter.getValue(bytes)
        bytes must beEqualTo(expectedBytes)
        result must beEqualTo(value)
      }
    }

    "have a lexicographic ordering matching the normal numeric ordering" in {
      val numericOrdering = Seq[Byte](Byte.MinValue, -3, 0, 3, Byte.MaxValue)
      val bytes = numericOrdering.map(ByteConverter.toBytes)
      bytes must beSorted
    }

  }

  "ShortConverter" should {

    "convert symmetrically" in {
      "value"        | "expectedBytes" |>
      0.toShort      ! Array(-128,  0) |
      Short.MaxValue ! Array(  -1, -1) |
      Short.MinValue ! Array(   0,  0) |
      -3.toShort     ! Array( 127, -3) |
      3.toShort      ! Array(-128,  3) | { (value, expectedBytes) =>
        val bytes = ShortConverter.toBytes(value)
        val result = ShortConverter.getValue(bytes)
        bytes must beEqualTo(expectedBytes)
        result must beEqualTo(value)
      }
    }

    "have a lexicographic ordering matching the normal numeric ordering" in {
      val numericOrdering = Seq[Short](Short.MinValue, -3, 0, 3, Short.MaxValue)
      val bytes = numericOrdering.map(ShortConverter.toBytes)
      bytes must beSorted
    }

  }

  "IntConverter" should {

    "convert symmetrically" in {
      "value"        | "expectedBytes"                         |>
        0            ! Array(-128,  0,  0,  0) |
        Int.MaxValue ! Array(  -1, -1, -1, -1) |
        Int.MinValue ! Array(   0,  0,  0,  0) |
        -3           ! Array( 127, -1, -1, -3) |
        3            ! Array(-128,  0,  0,  3) | { (value, expectedBytes) =>
        val bytes = IntConverter.toBytes(value)
        val result = IntConverter.getValue(bytes)
        bytes must beEqualTo(expectedBytes)
        result must beEqualTo(value)
      }
    }

    "have a lexicographic ordering matching the normal numeric ordering" in {
      val numericOrdering = Seq(Int.MinValue, Int.MinValue / 2, -3, 0, 3, Int.MaxValue / 2, Int.MaxValue)
      val bytes = numericOrdering.map(IntConverter.toBytes)
      bytes must beSorted
    }

  }

  "LongConverter" should {

    "convert symmetrically" in {
      "value"       | "expectedBytes"                         |>
      0L            ! Array(-128,  0,  0,  0,  0,  0,  0,  0) |
      Long.MaxValue ! Array(  -1, -1, -1, -1, -1, -1, -1, -1) |
      Long.MinValue ! Array(   0,  0,  0,  0,  0,  0,  0,  0) |
      -3L           ! Array( 127, -1, -1, -1, -1, -1, -1, -3) |
      3L            ! Array(-128,  0,  0,  0,  0,  0,  0,  3) | { (value, expectedBytes) =>
        val bytes = LongConverter.toBytes(value)
        val result = LongConverter.getValue(bytes)
        bytes must beEqualTo(expectedBytes)
        result must beEqualTo(value)
      }
    }

    "have a lexicographic ordering matching the normal numeric ordering" in {
      val numericOrdering = Seq[Long](Long.MinValue, Long.MinValue / 2, -3L, 0L, 3L, Long.MaxValue / 2, Long.MaxValue)
      val bytes = numericOrdering.map(LongConverter.toBytes)
      bytes must beSorted
    }

  }

  "StringConverter" should {

    "convert symmetrically" in {
      "value"         | "expectedBytes"                       |>
      "Hello"        !! Array(72, 101, 108, 108, 111)         |
      "中國"           !! Array(-28, -72, -83, -27, -100, -117) |
      "\uD83D\uDCA9" !! Array(-16, -97, -110, -87)            | { (value, expectedBytes) =>
        val bytes = StringConverter.toBytes(value)
        val result = StringConverter.getValue(bytes)
        bytes must beEqualTo(expectedBytes)
        result must beEqualTo(value)
      }
    }

  }

}
