package com.tagged.morice

trait MoConverter[T] {

  def getBytes(value: T): Array[Byte]

  def getValue(bytes: Array[Byte]): T

}
