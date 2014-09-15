package com.tagged.morice

import com.tagged.morice.MoConverter.ConverterToken
import org.apache.hadoop.hbase.util.Bytes

object PhoenixConversions {

  case object BooleanConverter extends MoConverter[Boolean] {

    val sizeOf = 1

    def toBytes(value: Boolean): Array[Byte] = value match {
      case false => Array[Byte](0)
      case true  => Array[Byte](1)
    }

    def getToken(bytes: Array[Byte]): ConverterToken[Boolean] = {
      val token = bytes.slice(0, sizeOf)
      ConverterToken(token, token(0) != 0.toByte)
    }

  }

  case object ByteConverter extends MoConverter[Byte] {

    val sizeOf = 1

    def toBytes(value: Byte): Array[Byte] = Array((value ^ Byte.MinValue).toByte)

    def getToken(bytes: Array[Byte]): ConverterToken[Byte] = {
      val token = bytes.slice(0, sizeOf)
      ConverterToken(token, (token(0) ^ Byte.MinValue).toByte)
    }

  }

  case object ShortConverter extends MoConverter[Short] {

    val sizeOf = 2

    def toBytes(value: Short): Array[Byte] = Bytes.toBytes((value ^ Short.MinValue).toShort)

    def getToken(bytes: Array[Byte]): ConverterToken[Short] = {
      val token = bytes.slice(0, sizeOf)
      ConverterToken(token, (Bytes.toShort(token) ^ Short.MinValue).toShort)
    }

  }

  case object LongConverter extends MoConverter[Long] {

    val sizeOf = 8

    def toBytes(value: Long): Array[Byte] = Bytes.toBytes(value ^ Long.MinValue)

    def getToken(bytes: Array[Byte]): ConverterToken[Long] = {
      val token = bytes.slice(0, sizeOf)
      ConverterToken(token, Bytes.toLong(token) ^ Long.MinValue)
    }

  }

  case object StringConverter extends MoConverter[String] {

    def toBytes(value: String): Array[Byte] = Bytes.toBytes(value)

    def getToken(bytes: Array[Byte]): ConverterToken[String] = {
      ConverterToken(bytes, Bytes.toString(bytes))
    }

  }

}
