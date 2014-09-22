package com.tagged.pho

import org.apache.hadoop.hbase.client._
import scala.collection.JavaConverters._

class PhoTable(connection: HConnection, tableName: String) {

  def withTable[A](block: HTableInterface => A) = {
    val table = connection.getTable(tableName)
    try {
      block(table)
    } finally {
      table.close()
    }
  }

  def withScanner[A](scan: Scan)(block: Seq[Result] => A) = {
    withTable { table =>
      val scanner = table.getScanner(scan)
      try {
        block(scanner.asScala.toSeq)
      } finally {
        scanner.close()
      }
    }
  }

  def write(doc: Document[_]) = {
    withTable { table =>
      table.put(doc.getPut)
    }
  }

  def read(rowKey: RowKey[_], columns: Seq[Column[_]]): Seq[Cell[_]] = {
    withTable { table =>
      val get = new Get(rowKey.toBytes)
      for (column <- columns) {
        get.addColumn(column.family.bytes, column.qualifierBytes)
      }
      val result = table.get(get)
      columns.map(_.getCell(result).orNull).filter(_ != null)
    }
  }

  def read[A](query: Query[A]): Seq[Document[A]] = {
    withScanner(query.getScan) { scanner =>
      scanner.map({ result =>
        val key = query.startRow.getRowKey(result)
        val cells = query.columns.map(_.getCell(result).orNull).filter(_ != null)
        Document(key, cells)
      })
    }
  }

}
