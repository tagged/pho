package com.tagged.morice

import com.tagged.morice.MoConverter.ConverterToken

trait MoConverter[T] {

  def toBytes(value: T): Array[Byte]

  def getToken(bytes: Array[Byte]): ConverterToken[T]

  def getValue(bytes: Array[Byte]): T = getToken(bytes).value

}

object MoConverter {

  case class ConverterToken[T](bytes: Array[Byte], value: T)

  case object IdentityConverter extends MoConverter[Array[Byte]] {

    def toBytes(value: Array[Byte]): Array[Byte] = value

    def getToken(bytes: Array[Byte]): ConverterToken[Array[Byte]] = ConverterToken(bytes, bytes)

  }

  case class InvertConverter[T](inner: MoConverter[T]) extends MoConverter[T] {

    def toBytes(value: T): Array[Byte] = inner.toBytes(value).map({ b => (~b).toByte })

    def getToken(bytes: Array[Byte]): ConverterToken[T] = {
      val inverted = bytes.map({ b => (~b).toByte })
      inner.getToken(inverted)
    }

  }

  case class Tuple2Converter[A,B](a: MoConverter[A], b: MoConverter[B]) extends MoConverter[(A,B)] {

    def toBytes(value: (A,B)) = Array.concat(a.toBytes(value._1), b.toBytes(value._2))

    def getToken(bytes: Array[Byte]): ConverterToken[(A,B)] = {
      val token1 = a.getToken(bytes)
      val after1 = bytes.drop(token1.bytes.length)
      val token2 = b.getToken(after1)
      val token = Array.concat(token1.bytes, token2.bytes)
      val value = (token1.value, token2.value)
      ConverterToken(token, value)
    }

  }

  case class Tuple3Converter[A,B,C](a: MoConverter[A], b: MoConverter[B], c: MoConverter[C]) extends MoConverter[(A,B,C)] {

    def toBytes(value: (A,B,C)) = Array.concat(a.toBytes(value._1), b.toBytes(value._2), c.toBytes(value._3))

    def getToken(bytes: Array[Byte]): ConverterToken[(A,B,C)] = {
      val token1 = a.getToken(bytes)
      val after1 = bytes.drop(token1.bytes.length)
      val token2 = b.getToken(after1)
      val after2 = after1.drop(token2.bytes.length)
      val token3 = c.getToken(after2)
      val token = Array.concat(token1.bytes, token2.bytes, token3.bytes)
      val value = (token1.value, token2.value, token3.value)
      ConverterToken(token, value)
    }

  }

  case class Tuple4Converter[A,B,C,D](a: MoConverter[A], b: MoConverter[B], c: MoConverter[C], d: MoConverter[D]) extends MoConverter[(A,B,C,D)] {
    def toBytes(value: (A,B,C,D)) = Array.concat(a.toBytes(value._1), b.toBytes(value._2), c.toBytes(value._3), d.toBytes(value._4))

    def getToken(bytes: Array[Byte]): ConverterToken[(A,B,C,D)] = {
      val token1 = a.getToken(bytes)
      val after1 = bytes.drop(token1.bytes.length)
      val token2 = b.getToken(after1)
      val after2 = after1.drop(token2.bytes.length)
      val token3 = c.getToken(after2)
      val after3 = after2.drop(token2.bytes.length)
      val token4 = d.getToken(after3)
      val token = Array.concat(token1.bytes, token2.bytes, token3.bytes, token4.bytes)
      val value = (token1.value, token2.value, token3.value, token4.value)
      ConverterToken(token, value)
    }

  }

}
