package com.tagged.maurice

case class MColumn(rawFamily: Array[Byte], rawName: Array[Byte]) {

  def this(columnFamily: String, columnName: String) = this(columnFamily.getBytes, columnName.getBytes)

  lazy val family = new String(rawFamily)

  lazy val name = new String(rawName)

}
