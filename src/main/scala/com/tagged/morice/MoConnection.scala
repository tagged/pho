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

  def write[T](tableName: String, rowKey: Array[Byte], values: Iterable[MoCell[T]]) = {
    withTable(tableName) { table =>
      val put = new Put(rowKey)
      for (value <- values) {
        put.add(value.column.family.getBytes, value.column.getBytes, value.getBytes)
      }
      table.put(put)
      table.flushCommits()
    }
  }

  def read[T](tableName: String, rowKey: Array[Byte], columns: Iterable[MoColumn[T]]): Iterable[MoCell[T]] = {
    withTable(tableName) { table =>
      val get = new Get(rowKey)
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
