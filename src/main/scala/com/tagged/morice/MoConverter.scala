package com.tagged.morice

import com.tagged.morice.MoConverter.ConverterToken

trait MoConverter[T] {

  def getBytes(value: T): Array[Byte]

  def getToken(bytes: Array[Byte]): ConverterToken[T]

  def getValue(bytes: Array[Byte]): T = getToken(bytes).value

}

object MoConverter {

  case class ConverterToken[T](bytes: Array[Byte], value: T)

  object IdentityConverter extends MoConverter[Array[Byte]] {

    def getBytes(value: Array[Byte]): Array[Byte] = value

    def getToken(bytes: Array[Byte]): ConverterToken[Array[Byte]] = ConverterToken(bytes, bytes)

  }

  case class InvertConverter[T](inner: MoConverter[T]) extends MoConverter[T] {

    def getBytes(value: T): Array[Byte] = inner.getBytes(value).map({ b => (~b).toByte })

    def getToken(bytes: Array[Byte]): ConverterToken[T] = {
      val inverted = bytes.map({ b => (~b).toByte })
      inner.getToken(inverted)
    }

  }

  case class Tuple2Converter[T1,T2](_1: MoConverter[T1], _2: MoConverter[T2]) extends MoConverter[Tuple2[T1,T2]] {

    def getBytes(value: Tuple2[T1,T2]) = _1.getBytes(value._1) ++ _2.getBytes(value._2)

    def getToken(bytes: Array[Byte]): ConverterToken[Tuple2[T1,T2]] = {
      val token1 = _1.getToken(bytes)
      val remainder = bytes.drop(token1.bytes.length)
      val token2 = _2.getToken(remainder)
      ConverterToken(token1.bytes ++ token2.bytes, (token1.value, token2.value))
    }

  }

}
