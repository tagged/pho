package com.tagged.morice

case class MoColumn[T](family: MoColumnFamily, name: String, converter: MoConverter[T]) {

  lazy val getBytes: Array[Byte] = name.getBytes

  def getValue(bytes: Array[Byte]): MoValue[T] = new MoValue(this, converter.getValue(bytes))

}
