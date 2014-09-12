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

  def write[T](tableName: String, rowKey: Array[Byte], values: Iterable[MoValue[T]]) = {
    withTable(tableName) { table =>
      val put = new Put(rowKey)
      for (value <- values) {
        put.add(value.column.family.getBytes, value.column.getBytes, value.getBytes)
      }
      table.put(put)
      table.flushCommits()
    }
  }

  def read[T](tableName: String, rowKey: Array[Byte], columns: Iterable[MoColumn[T]]): Iterable[MoValue[T]] = {
    withTable(tableName) { table =>
      val get = new Get(rowKey)
      for (column <- columns) {
        get.addColumn(column.family.getBytes, column.getBytes)
      }
      val result = table.get(get)
      for (column <- columns) yield  {
        column.getValue(result.getValue(column.family.getBytes, column.getBytes))
      }
    }
  }

}
