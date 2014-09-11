package com.tagged.morice

case class MoColumnFamily(name: String) {

  lazy val getBytes = name.getBytes

  def column(name: String) = new MoColumn(this, name)

}