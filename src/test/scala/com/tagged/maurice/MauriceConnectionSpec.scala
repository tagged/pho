package com.tagged.maurice

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, Put, HConnectionManager}
import org.specs2.mutable.Specification

/**
 * This test requires that a test table be pre-created on the HBase cluster:
 *
 *     create 'MauriceConnectionSpec', 'family1', 'family2'
 */
class MauriceConnectionSpec extends Specification {

  val tableName = "MauriceConnectionSpec"
  val family1 = "family1"
  val family2 = "family2"

  val configuration = HBaseConfiguration.create()
  scala.util.Properties.propOrNone("hbase.zookeeper.quorum") match {
    case Some(quorum) => configuration.set("hbase.zookeeper.quorum", quorum)
    case None => Unit
  }

  val connection = HConnectionManager.createConnection(configuration)
  val maurice = new MauriceConnection(connection)

  "withTable" should {

    "let us write and read using HTableInterface primitives" in {
      val rowkey = System.nanoTime().toString.getBytes
      val qualifier = "primitiveReadWriteTest".getBytes
      val writeValue = System.nanoTime().toString.getBytes

      val readResult = maurice.withTable(tableName) { table =>
        val put = new Put(rowkey)
        put.add(family1.getBytes, qualifier, writeValue)
        table.put(put)
        table.flushCommits()

        val get = new Get(rowkey)
        get.addColumn(family1.getBytes, qualifier)
        val result = table.get(get)
        result.getValue(family1.getBytes, qualifier)
      }

      readResult must beEqualTo(writeValue)
    }

    "let us write and read byte arrays" in {
      val rowKey = System.nanoTime().toString.getBytes
      val columnName = "byteArrayReadWriteTest"
      val writeValue = System.nanoTime().toString.getBytes

      maurice.writeBytes(tableName, rowKey, new MColumn(family1, columnName), writeValue)
      val readResult = maurice.readBytes(tableName, rowKey, new MColumn(family1, columnName))

      readResult must beEqualTo(writeValue)
    }

  }

}
