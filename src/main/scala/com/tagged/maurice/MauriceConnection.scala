package com.tagged.maurice

import org.apache.hadoop.hbase.client.{HTableInterface, HConnection}

class MauriceConnection(connection: HConnection) {

  def withTable[A](name: String)(block: HTableInterface => A) = {
    val table = connection.getTable(name)
    try {
      block(table)
    } finally {
      table.close()
    }
  }

}
