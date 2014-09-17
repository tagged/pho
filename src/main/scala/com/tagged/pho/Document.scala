package com.tagged.pho

import org.apache.hadoop.hbase.client.Put

case class Document[A](key: RowKey[A], values: Iterable[Cell[_]], version: Option[Version] = None) {

  def getValue[X](column: Column[X]): Option[X] = {
    values.find(_.column == column) match {
      case Some(cell) => Option(cell.value.asInstanceOf[X])
      case None => None
    }
  }

  def getPut: Put = {
    val put = version match {
      case Some(v) => new Put(key.toBytes, v.timestamp)
      case None    => new Put(key.toBytes)
    }
    for (value <- values) {
      put.add(value.column.family.toBytes, value.column.qualifierBytes, value.toBytes)
    }
    put
  }

}
