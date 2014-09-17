package com.tagged.pho

import org.apache.hadoop.hbase.client.Put

/**
 * Identified by column location, value and version timestamp,
 * a cell represents the basic unit of storage.
 *
 * @param column location of data
 * @param value  high-level piece of data
 * @tparam A     type of data
 */
case class Cell[A](column: Column[A], value: A, version: Option[Version] = None) {

  def this(column: Column[A], value: A, version: Version) = this(column, value, Option(version))

  def valueBytes: Array[Byte] = column.converter.toBytes(value)

  def addToPut(put: Put) = version match {
    case Some(v) => put.add(column.family.bytes, column.qualifierBytes, v.timestamp, valueBytes)
    case None    => put.add(column.family.bytes, column.qualifierBytes, valueBytes)
  }

}
