package com.tagged.morice

import org.apache.hadoop.hbase.util.Bytes

case class MoColumnFamily(name: String) {

  lazy val toBytes = Bytes.toBytes(name)

}
