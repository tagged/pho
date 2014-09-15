package com.tagged.morice

case class MoCell[T](column: MoColumn[T], value: Option[T]) {

  def this(column: MoColumn[T], value: T) = this(column, Option(value))

  def toBytes: Array[Byte] = column.converter.toBytes(value.get)

}
