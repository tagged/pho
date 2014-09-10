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

  def writeBytes(tableName: String, rowKey: Array[Byte], columnFamily: String, columnName: String, value: Array[Byte]) = {
    withTable(tableName) { table =>
      val put = new Put(rowKey)
      put.add(columnFamily.getBytes, columnName.getBytes, value)
      table.put(put)
      table.flushCommits()
    }
  }

  def readBytes(tableName: String, rowKey: Array[Byte], columnFamily: String, columnName: String): Array[Byte] = {
    withTable(tableName) { table =>
      val rawColumnFamily = columnFamily.getBytes
      val rawColumnName = columnName.getBytes
      val get = new Get(rowKey)
      get.addColumn(rawColumnFamily, rawColumnName)
      val result = table.get(get)
      result.getValue(columnFamily.getBytes, columnName.getBytes)
    }
  }

}
