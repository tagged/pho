package com.tagged.morice

import org.apache.hadoop.hbase.util.Bytes
import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification
import PhoenixConversions._

class PhoenixConversionsSpec extends Specification with DataTables {

  implicit object LexicographicOrdering extends Ordering[Array[Byte]] {
    def compare(a:Array[Byte], b:Array[Byte]) = Bytes.compareTo(a, b)
  }

  "ConvertByte" should {

    "convert symmetrically" in {
      "value"          | "expectedBytes"                         |>
        0.toByte      ! Array(-128) |
        Byte.MaxValue ! Array(  -1) |
        Byte.MinValue ! Array(   0) |
        -3.toByte     ! Array( 125) |
        3.toByte      ! Array(-125) | { (value, expectedBytes ) =>
        val bytes = ConvertByte.getBytes(value)
        val result = ConvertByte.getValue(bytes)
        bytes must beEqualTo(expectedBytes)
        result must beEqualTo(value)
      }
    }

    "have a lexicographic ordering matching the normal numeric ordering" in {
      val numericOrdering = Seq[Byte](Byte.MinValue, -3, 0, 3, Byte.MaxValue)
      val bytes = numericOrdering.map(ConvertByte.getBytes)
      bytes must beSorted
    }

  }

  "ConvertShort" should {

    "convert symmetrically" in {
      "value"          | "expectedBytes"                         |>
        0.toShort      ! Array(-128,  0) |
        Short.MaxValue ! Array(  -1, -1) |
        Short.MinValue ! Array(   0,  0) |
        -3.toShort     ! Array( 127, -3) |
        3.toShort      ! Array(-128,  3) | { (value, expectedBytes ) =>
        val bytes = ConvertShort.getBytes(value)
        val result = ConvertShort.getValue(bytes)
        bytes must beEqualTo(expectedBytes)
        result must beEqualTo(value)
      }
    }

    "have a lexicographic ordering matching the normal numeric ordering" in {
      val numericOrdering = Seq[Short](Short.MinValue, -3, 0, 3, Short.MaxValue)
      val bytes = numericOrdering.map(ConvertShort.getBytes)
      bytes must beSorted
    }

  }

  "ConvertLong" should {

    "convert symmetrically" in {
      "value"       | "expectedBytes"                         |>
      0L            ! Array(-128,  0,  0,  0,  0,  0,  0,  0) |
      Long.MaxValue ! Array(  -1, -1, -1, -1, -1, -1, -1, -1) |
      Long.MinValue ! Array(   0,  0,  0,  0,  0,  0,  0,  0) |
      -3L           ! Array( 127, -1, -1, -1, -1, -1, -1, -3) |
      3L            ! Array(-128,  0,  0,  0,  0,  0,  0,  3) | { (value, expectedBytes ) =>
        val bytes = ConvertLong.getBytes(value)
        val result = ConvertLong.getValue(bytes)
        bytes must beEqualTo(expectedBytes)
        result must beEqualTo(value)
      }
    }

    "have a lexicographic ordering matching the normal numeric ordering" in {
      val numericOrdering = Seq[Long](Long.MinValue, Long.MinValue / 2, -3L, 0L, 3L, Long.MaxValue / 2, Long.MaxValue)
      val bytes = numericOrdering.map(ConvertLong.getBytes)
      bytes must beSorted
    }

  }

}
