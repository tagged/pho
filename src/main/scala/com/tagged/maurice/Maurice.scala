package com.tagged.maurice

object Maurice {

  class ColumnFamily(val getBytes: Array[Byte]) {

    def this(name: String) = this(name.getBytes)

    lazy val name = new String(getBytes)

    def column(name: String) = new Column(this, name)

    def column(bytes: Array[Byte]) = new Column(this, bytes)

    lazy override val toString = s"ColumnFamily($name)"

  }

  class Column(val family: ColumnFamily, val getBytes: Array[Byte]) {

    def this(family: ColumnFamily, name: String) = this(family, name.getBytes)

    lazy val name = new String(getBytes)

    lazy override val toString = s"Column($family,$name)"

  }

}
