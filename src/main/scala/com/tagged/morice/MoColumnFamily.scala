package com.tagged.morice

case class MoColumnFamily(name: String) {

  lazy val getBytes = name.getBytes

  def column[T](name: String, converter: MoConverter[T]) = new MoColumn(this, name, converter)

}
