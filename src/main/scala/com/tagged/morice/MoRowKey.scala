package com.tagged.morice

case class MoRowKey[T](value: T, converter: MoConverter[T]) {

  def toBytes: Array[Byte] = converter.toBytes(value)

}
