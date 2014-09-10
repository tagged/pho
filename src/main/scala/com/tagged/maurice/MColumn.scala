package com.tagged.maurice

case class MColumn(family: String, name: String) {

  def this(rawFamily: Array[Byte], rawName: Array[Byte]) = this(new String(rawFamily), new String(rawName))

  lazy val rawFamily = family.getBytes

  lazy val rawName = name.getBytes

}
