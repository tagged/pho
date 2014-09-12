package com.tagged.morice

import com.tagged.morice.PhoenixConversions.StringConverter
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, Put, HConnectionManager}
import org.specs2.mutable.Specification

/**
 * This test requires that a test table be pre-created on the HBase cluster:
 *
 *     create 'MoriceConnectionSpec', 'family1', 'family2'
 */
class MoConnectionSpec extends Specification {

  val tableName = "MoriceConnectionSpec"
  val family1 = "family1"
  val family2 = "family2"

  val configuration = HBaseConfiguration.create()
  scala.util.Properties.propOrNone("hbase.zookeeper.quorum") match {
    case Some(quorum) => configuration.set("hbase.zookeeper.quorum", quorum)
    case None => Unit
  }

  val connection = HConnectionManager.createConnection(configuration)
  val morice = new MoConnection(connection)

  "withTable" should {

    "let us put and get using HTableInterface primitives" in {
      val rowKey = System.nanoTime().toString.getBytes
      val qualifier = "primitiveReadWriteTest".getBytes
      val writeValue = System.nanoTime().toString.getBytes

      val readResult = morice.withTable(tableName) { table =>
        val put = new Put(rowKey)
        put.add(family1.getBytes, qualifier, writeValue)
        table.put(put)
        table.flushCommits()

        val get = new Get(rowKey)
        get.addColumn(family1.getBytes, qualifier)
        val result = table.get(get)
        result.getValue(family1.getBytes, qualifier)
      }

      readResult must beEqualTo(writeValue)
    }

  }

  "write/read single row key" should {

    "let us write and read specific column values" in {
      val rowKey = System.nanoTime().toString.getBytes
      val column1 = MoColumnFamily(family1).column("columnReadWriteTest1", StringConverter)
      val column2 = MoColumnFamily(family2).column("columnReadWriteTest2", StringConverter)
      val value1 = new MoValue(column1, System.nanoTime().toString)
      val value2 = new MoValue(column2, System.nanoTime().toString)

      val values = Array(value1, value2)
      morice.write(tableName, rowKey, values)
      val readResult = morice.read(tableName, rowKey, Array(column1, column2))

      readResult.toArray must beEqualTo(values)
    }

    "read empty column values as None" in {
      val rowKey = ("emptyReadTest" + System.nanoTime()).getBytes
      val column = MoColumnFamily(family1).column("emptyReadTest", StringConverter)

      val readResult = morice.read(tableName, rowKey, Array(column))

      readResult.toArray must beEqualTo(Array(MoValue(column, None)))
    }

  }

}
