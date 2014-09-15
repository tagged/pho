package com.tagged.morice

case class MoDocument[A,B](key: MoRowKey[A], values: Iterable[MoCell[B]])
