package com.tagged.morice

case class MoCell[T](column: MoColumn[T], value: T) {

  def toBytes: Array[Byte] = column.converter.toBytes(value)

}
