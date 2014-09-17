package com.tagged.morice

import org.apache.hadoop.hbase.client.Put

case class MoDocument[A](key: MoRowKey[A], values: Iterable[MoCell[_]], timestamp: Option[Long] = None) {

  def getValue[X](column: MoColumn[X]): Option[X] = {
    values.find(_.column == column) match {
      case Some(cell) => Option(cell.value.asInstanceOf[X])
      case None => None
    }
  }

  def getPut: Put = {
    val put = timestamp match {
      case Some(ts) => new Put(key.toBytes, ts)
      case None     => new Put(key.toBytes)
    }
    for (value <- values) {
      put.add(value.column.family.toBytes, value.column.toBytes, value.toBytes)
    }
    put
  }

}
