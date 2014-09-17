package com.tagged.pho

import org.apache.hadoop.hbase.util.Bytes

/**
 * A grouping of columns,
 * contained within a shared unit of physical storage.
 *
 * @param name of the family
 */
case class ColumnFamily(name: String) {

  lazy val bytes = Bytes.toBytes(name)

}
