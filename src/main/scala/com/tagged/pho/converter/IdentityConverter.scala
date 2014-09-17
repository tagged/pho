package com.tagged.pho.converter

case object IdentityConverter extends PhoConverter[Array[Byte]] {

  def toBytes(value: Array[Byte]): Array[Byte] = value

  def getToken(bytes: Array[Byte]): ConverterToken[Array[Byte]] = ConverterToken(bytes, bytes)

}
