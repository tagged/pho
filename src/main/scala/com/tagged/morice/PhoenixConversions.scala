package com.tagged.morice

import org.apache.hadoop.hbase.util.Bytes

object PhoenixConversions {

  object BooleanConverter extends MoConverter[Boolean] {

    def getBytes(value: Boolean): Array[Byte] = value match {
      case false => Array[Byte](0)
      case true  => Array[Byte](1)
    }

    def getValue(bytes: Array[Byte]): Boolean = Bytes.toBoolean(bytes)

  }

  object ByteConverter extends MoConverter[Byte] {

    def getBytes(value: Byte): Array[Byte] = Array((value ^ Byte.MinValue).toByte)

    def getValue(bytes: Array[Byte]): Byte = (bytes(0) ^ Byte.MinValue).toByte

  }

  object ShortConverter extends MoConverter[Short] {

    def getBytes(value: Short): Array[Byte] = Bytes.toBytes((value ^ Short.MinValue).toShort)

    def getValue(bytes: Array[Byte]): Short = (Bytes.toShort(bytes) ^ Short.MinValue).toShort

  }

  object LongConverter extends MoConverter[Long] {

    def getBytes(value: Long): Array[Byte] = Bytes.toBytes(value ^ Long.MinValue)

    def getValue(bytes: Array[Byte]): Long = Bytes.toLong(bytes) ^ Long.MinValue

  }

  object StringConverter extends MoConverter[String] {

    def getBytes(value: String): Array[Byte] = Bytes.toBytes(value)

    def getValue(bytes: Array[Byte]): String = Bytes.toString(bytes)

  }

}
