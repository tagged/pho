package com.tagged.pho

import org.apache.hadoop.hbase.util.Bytes

case class ColumnFamily(name: String) {

  lazy val toBytes = Bytes.toBytes(name)

}
