package com.tagged.pho.converter

case class Tuple4Converter[A,B,C,D](a: PhoConverter[A], b: PhoConverter[B], c: PhoConverter[C], d: PhoConverter[D]) extends PhoConverter[(A,B,C,D)] {
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
