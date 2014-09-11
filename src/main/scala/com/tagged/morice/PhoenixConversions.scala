package com.tagged.morice

import org.apache.hadoop.hbase.util.Bytes

object PhoenixConversions {

  object ConvertBoolean {

    def getBytes(value: Boolean): Array[Byte] = value match {
      case false => Array[Byte](0)
      case true  => Array[Byte](1)
    }

    def getValue(bytes: Array[Byte]): Boolean = Bytes.toBoolean(bytes)

  }

  object ConvertByte {

    def getBytes(value: Byte): Array[Byte] = Array((value ^ Byte.MinValue).toByte)

    def getValue(bytes: Array[Byte]): Byte = (bytes(0) ^ Byte.MinValue).toByte

  }

  object ConvertShort {

    def getBytes(value: Short): Array[Byte] = Bytes.toBytes((value ^ Short.MinValue).toShort)

    def getValue(bytes: Array[Byte]): Short = (Bytes.toShort(bytes) ^ Short.MinValue).toShort

  }

  object ConvertLong {

    def getBytes(value: Long): Array[Byte] = Bytes.toBytes(value ^ Long.MinValue)

    def getValue(bytes: Array[Byte]): Long = Bytes.toLong(bytes) ^ Long.MinValue

  }

}
