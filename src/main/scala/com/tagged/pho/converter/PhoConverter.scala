package com.tagged.pho.converter

trait PhoConverter[T] {

  def toBytes(value: T): Array[Byte]

  def getToken(bytes: Array[Byte]): ConverterToken[T]

  def getValue(bytes: Array[Byte]): T = getToken(bytes).value

}
