package com.tagged.pho

import org.apache.hadoop.hbase.client._
import scala.collection.JavaConverters._

class PhoConnection(connection: HConnection) {

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

  def write(tableName: String, doc: Document[_]) = {
    withTable(tableName) { table =>
      table.put(doc.getPut)
    }
  }

  def read(tableName: String, rowKey: RowKey[_], columns: Iterable[Column[_]]): Iterable[Cell[_]] = {
    withTable(tableName) { table =>
      val get = new Get(rowKey.toBytes)
      for (column <- columns) {
        get.addColumn(column.family.toBytes, column.toBytes)
      }
      val result = table.get(get)
      columns.map(_.getCell(result).getOrElse(null)).filter(_ != null)
    }
  }

  def read[A](tableName: String, query: Query[A]): Iterable[Document[A]] = {
    withScanner(tableName, query.getScan) { scanner =>
      scanner.map({ result =>
        val key = query.startRow.getRowKey(result)
        val cells = query.columns.map(_.getCell(result).getOrElse(null)).filter(_ != null)
        Document(key, cells)
      })
    }
  }

}
