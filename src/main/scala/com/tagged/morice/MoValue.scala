package com.tagged.morice

case class MoValue[T](column: MoColumn[T], value: T) {

  def getBytes: Array[Byte] = column.converter.getBytes(value)

}
