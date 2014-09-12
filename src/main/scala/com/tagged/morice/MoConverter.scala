package com.tagged.morice

trait MoConverter[T] {

  def getBytes(value: T): Array[Byte]

  def getValue(bytes: Array[Byte]): T

}

object MoConverter {

  object IdentityConverter extends MoConverter[Array[Byte]] {

    def getBytes(value: Array[Byte]): Array[Byte] = value

    def getValue(bytes: Array[Byte]): Array[Byte] = bytes

  }

  case class InvertConverter[T](inner: MoConverter[T]) extends MoConverter[T] {

    def getBytes(value: T): Array[Byte] = inner.getBytes(value).map({ b => (~b).toByte })

    def getValue(bytes: Array[Byte]): T = inner.getValue(bytes.map({ b => (~b).toByte }))

  }

}
