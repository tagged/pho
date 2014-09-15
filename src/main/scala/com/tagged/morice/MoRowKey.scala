package com.tagged.morice

case class MoRowKey[T](value: T, converter: MoConverter[T]) {

  def getBytes: Array[Byte] = converter.getBytes(value)

}
