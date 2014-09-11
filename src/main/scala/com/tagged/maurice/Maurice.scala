package com.tagged.maurice

object Maurice {

  case class ColumnFamily(name: String) {

    lazy val getBytes = name.getBytes

    def column(name: String) = new Column(this, name)

  }

  case class Column(family: ColumnFamily, name: String) {

    lazy val getBytes = name.getBytes

  }

}
