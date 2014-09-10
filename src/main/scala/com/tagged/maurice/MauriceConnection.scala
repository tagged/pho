package com.tagged.maurice

import org.apache.hadoop.hbase.client.{Get, Put, HTableInterface, HConnection}

class MauriceConnection(connection: HConnection) {

  def withTable[A](name: String)(block: HTableInterface => A) = {
    val table = connection.getTable(name)
    try {
      block(table)
    } finally {
      table.close()
    }
  }

  def writeBytes(tableName: String, rowKey: Array[Byte], values: Map[MColumn, Array[Byte]]) = {
    withTable(tableName) { table =>
      val put = new Put(rowKey)
      for (value <- values) {
        put.add(value._1.rawFamily, value._1.rawName, value._2)
      }
      table.put(put)
      table.flushCommits()
    }
  }

  def readBytes(tableName: String, rowKey: Array[Byte], columns: Iterable[MColumn]): Map[MColumn, Array[Byte]] = {
    withTable(tableName) { table =>
      val get = new Get(rowKey)
      for (column <- columns) {
        get.addColumn(column.rawFamily, column.rawName)
      }
      val result = table.get(get)
      columns.map({ column =>
        column -> result.getValue(column.rawFamily, column.rawName)
      }).toMap
    }
  }

}
