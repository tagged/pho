package com.tagged.pho.converter

case class Tuple2Converter[A,B](a: PhoConverter[A], b: PhoConverter[B]) extends PhoConverter[(A,B)] {

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
