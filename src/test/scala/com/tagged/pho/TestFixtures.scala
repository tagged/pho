package com.tagged.pho

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HConnectionManager

/**
 * These tests require that a test table be pre-created on the HBase cluster.
 * Run the following in the hbase shell.
 *
 *     create 'PhoIntegrationTests', 'family1', 'family2'
 */
object TestFixtures {

  val testTableName = "PhoIntegrationTests"
  val family1 = ColumnFamily("family1")
  val family2 = ColumnFamily("family2")

  val configuration = HBaseConfiguration.create()
  scala.util.Properties.propOrNone("hbase.zookeeper.quorum") match {
    case Some(quorum) => configuration.set("hbase.zookeeper.quorum", quorum)
    case None => Unit
  }

  val connection = HConnectionManager.createConnection(configuration)
  val pho = new PhoConnection(connection)

}
