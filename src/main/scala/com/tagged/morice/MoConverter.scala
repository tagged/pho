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

  case class Tuple2Converter[A,B](a: MoConverter[A], b: MoConverter[B]) extends MoConverter[Tuple2[A,B]] {

    def getBytes(value: Tuple2[A,B]) = a.getBytes(value._1) ++ b.getBytes(value._2)

    def getToken(bytes: Array[Byte]): ConverterToken[Tuple2[A,B]] = {
      val token1 = a.getToken(bytes)
      val after1 = bytes.drop(token1.bytes.length)
      val token2 = b.getToken(after1)
      val token = token1.bytes ++ token2.bytes
      val value = (token1.value, token2.value)
      ConverterToken(token, value)
    }

  }

  case class Tuple3Converter[A,B,C](a: MoConverter[A], b: MoConverter[B], c: MoConverter[C]) extends MoConverter[Tuple3[A,B,C]] {

    def getBytes(value: Tuple3[A,B,C]) = a.getBytes(value._1) ++ b.getBytes(value._2) ++ c.getBytes(value._3)

    def getToken(bytes: Array[Byte]): ConverterToken[Tuple3[A,B,C]] = {
      val token1 = a.getToken(bytes)
      val after1 = bytes.drop(token1.bytes.length)
      val token2 = b.getToken(after1)
      val after2 = after1.drop(token2.bytes.length)
      val token3 = c.getToken(after2)
      val token = token1.bytes ++ token2.bytes ++ token3.bytes
      val value = (token1.value, token2.value, token3.value)
      ConverterToken(token, value)
    }

  }

  case class Tuple4Converter[A,B,C,D](a: MoConverter[A], b: MoConverter[B], c: MoConverter[C], d: MoConverter[D]) extends MoConverter[Tuple4[A,B,C,D]] {
    def getBytes(value: Tuple4[A,B,C,D]) = a.getBytes(value._1) ++ b.getBytes(value._2) ++ c.getBytes(value._3) ++ d.getBytes(value._4)

    def getToken(bytes: Array[Byte]): ConverterToken[Tuple4[A,B,C,D]] = {
      val token1 = a.getToken(bytes)
      val after1 = bytes.drop(token1.bytes.length)
      val token2 = b.getToken(after1)
      val after2 = after1.drop(token2.bytes.length)
      val token3 = c.getToken(after2)
      val after3 = after2.drop(token2.bytes.length)
      val token4 = d.getToken(after3)
      val token = token1.bytes ++ token2.bytes ++ token3.bytes ++ token4.bytes
      val value = (token1.value, token2.value, token3.value, token4.value)
      ConverterToken(token, value)
    }

  }

}
