package com.tagged.morice

case class MoColumn(family: MoColumnFamily, name: String) {

  lazy val getBytes = name.getBytes

}
