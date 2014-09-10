package com.tagged.maurice

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, HConnectionManager}
import org.specs2.mutable.Specification

class MauriceConnectionSpec extends Specification {

  val configuration = HBaseConfiguration.create()
  scala.util.Properties.propOrNone("hbase.zookeeper.quorum") match {
    case Some(quorum) => configuration.set("hbase.zookeeper.quorum", quorum)
    case None => Unit
  }

  val connection = HConnectionManager.createConnection(configuration)
  val maurice = new MauriceConnection(connection)

  "with an HBase connection we" should {

    "insert rows" in {
      maurice.withTable("myTable") { table =>
        val key = "row1".getBytes
        val put = new Put(key)
        put.add("family1".getBytes, "qualifier1".getBytes, "value1".getBytes)
        table.put(put)
      }

      true must beTrue
    }

  }

}
