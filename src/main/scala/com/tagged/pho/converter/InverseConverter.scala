package com.tagged.pho.converter

case class InverseConverter[T](inner: PhoConverter[T]) extends PhoConverter[T] {

  def toBytes(value: T): Array[Byte] = inner.toBytes(value).map({ b => (~b).toByte })

  def getToken(bytes: Array[Byte]): ConverterToken[T] = {
    val inverted = bytes.map({ b => (~b).toByte })
    inner.getToken(inverted)
  }

}
