package com.tagged.morice

import org.apache.hadoop.hbase.client.{Get, Put, HTableInterface, HConnection}

class MoConnection(connection: HConnection) {

  def withTable[A](name: String)(block: HTableInterface => A) = {
    val table = connection.getTable(name)
    try {
      block(table)
    } finally {
      table.close()
    }
  }

  def writeBytes(tableName: String, rowKey: Array[Byte], values: Map[MoColumn, Array[Byte]]) = {
    withTable(tableName) { table =>
      val put = new Put(rowKey)
      for (value <- values) {
        put.add(value._1.family.getBytes, value._1.getBytes, value._2)
      }
      table.put(put)
      table.flushCommits()
    }
  }

  def readBytes(tableName: String, rowKey: Array[Byte], columns: Iterable[MoColumn]): Map[MoColumn, Array[Byte]] = {
    withTable(tableName) { table =>
      val get = new Get(rowKey)
      for (column <- columns) {
        get.addColumn(column.family.getBytes, column.getBytes)
      }
      val result = table.get(get)
      columns.map({ column =>
        column -> result.getValue(column.family.getBytes, column.getBytes)
      }).toMap
    }
  }

}
