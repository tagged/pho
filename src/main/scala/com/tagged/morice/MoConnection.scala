package com.tagged.morice

import org.apache.hadoop.hbase.client._
import scala.collection.JavaConverters._

class MoConnection(connection: HConnection) {

  def withTable[A](name: String)(block: HTableInterface => A) = {
    val table = connection.getTable(name)
    try {
      block(table)
    } finally {
      table.close()
    }
  }

  def withScanner[A](name: String, scan: Scan)(block: Iterable[Result] => A) = {
    withTable(name) { table =>
      val scanner = table.getScanner(scan)
      try {
        block(scanner.asScala)
      } finally {
        scanner.close()
      }
    }
  }

  def write[A,B](tableName: String, rowKey: MoRowKey[A], values: Iterable[MoCell[B]]) = {
    withTable(tableName) { table =>
      val put = new Put(rowKey.getBytes)
      for (value <- values) {
        put.add(value.column.family.getBytes, value.column.getBytes, value.getBytes)
      }
      table.put(put)
      table.flushCommits()
    }
  }

  def read[A,B](tableName: String, rowKey: MoRowKey[A], columns: Iterable[MoColumn[B]]): Iterable[MoCell[B]] = {
    withTable(tableName) { table =>
      val get = new Get(rowKey.getBytes)
      for (column <- columns) {
        get.addColumn(column.family.getBytes, column.getBytes)
      }
      val result = table.get(get)
      for (column <- columns) yield  {
        column.getCell(result)
      }
    }
  }

}
