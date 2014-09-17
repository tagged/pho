package com.tagged.pho.converter

case class ConverterToken[T](bytes: Array[Byte], value: T)
