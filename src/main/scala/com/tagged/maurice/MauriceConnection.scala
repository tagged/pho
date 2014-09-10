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

  def writeBytes(tableName: String, rowKey: Array[Byte], column: MColumn, value: Array[Byte]) = {
    withTable(tableName) { table =>
      val put = new Put(rowKey)
      put.add(column.rawFamily, column.rawName, value)
      table.put(put)
      table.flushCommits()
    }
  }

  def readBytes(tableName: String, rowKey: Array[Byte], column: MColumn): Array[Byte] = {
    withTable(tableName) { table =>
      val get = new Get(rowKey)
      get.addColumn(column.rawFamily, column.rawName)
      val result = table.get(get)
      result.getValue(column.rawFamily, column.rawName)
    }
  }

}
