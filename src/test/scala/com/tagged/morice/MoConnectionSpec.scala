package com.tagged.morice

import com.tagged.morice.PhoenixConversions.StringConverter
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, Get, Put, HConnectionManager}
import org.apache.hadoop.hbase.util.Bytes
import org.specs2.mutable.Specification

/**
 * This test requires that a test table be pre-created on the HBase cluster:
 *
 *     create 'MoriceConnectionSpec', 'family1', 'family2'
 */
class MoConnectionSpec extends Specification {

  val tableName = "MoriceConnectionSpec"
  val family1 = MoColumnFamily("family1")
  val family2 = MoColumnFamily("family2")

  val configuration = HBaseConfiguration.create()
  scala.util.Properties.propOrNone("hbase.zookeeper.quorum") match {
    case Some(quorum) => configuration.set("hbase.zookeeper.quorum", quorum)
    case None => Unit
  }

  val connection = HConnectionManager.createConnection(configuration)
  val morice = new MoConnection(connection)

  "withTable" should {

    "let us put and get using HTableInterface primitives" in {
      val rowKey = Bytes.toBytes(System.nanoTime().toString)
      val qualifier = Bytes.toBytes("primitiveReadWriteTest")
      val writeValue = Bytes.toBytes(System.nanoTime().toString)

      val readResult = morice.withTable(tableName) { table =>
        val put = new Put(rowKey)
        put.add(family1.toBytes, qualifier, writeValue)
        table.put(put)
        table.flushCommits()

        val get = new Get(rowKey)
        get.addColumn(family1.toBytes, qualifier)
        val result = table.get(get)
        result.getValue(family1.toBytes, qualifier)
      }

      readResult must beEqualTo(writeValue)
    }

  }

  "withScanner" should {

    "let us scan using primitives" in {
      val now = System.nanoTime()
      val column = MoColumn(family1, "resultSetTest", StringConverter)

      // write some rows
      for ((i, word) <- Map(1->"one", 2->"two", 3->"three", 4->"four", 5->"five", 6->"six", 7->"seven", 8->"eight", 9->"nine")) {
        val doc = new MoDocument(
          MoRowKey("readSet." + now + "." + i, StringConverter),
          Seq(MoCell(column, word))
        )
        morice.write(tableName, doc)
      }

      val startRow = Bytes.toBytes("readSet." + now)
      val endRow = Bytes.toBytes("readSet." + (now + 1))
      val scan = new Scan(startRow, endRow)
      val readResult = morice.withScanner(tableName, scan) { results =>
        results.map(column.getCell).flatten.map(_.value)
      }

      readResult must beEqualTo(List("one", "two", "three", "four", "five", "six", "seven", "eight", "nine"))
    }
  }

  "write/read single row key" should {

    "let us write and read specific column values" in {
      val column1 = MoColumn(family1, "columnReadWriteTest1", StringConverter)
      val column2 = MoColumn(family2, "columnReadWriteTest2", StringConverter)

      val doc = MoDocument(
        MoRowKey(System.nanoTime().toString, StringConverter),
        Seq(
          MoCell(column1, System.nanoTime().toString),
          MoCell(column2, System.nanoTime().toString)
        )
      )

      morice.write(tableName, doc)
      val readResult = morice.read(tableName, doc.key, doc.values.map(_.column))

      readResult must beEqualTo(doc.values)
    }

    "read empty column values as None" in {
      val rowKey = MoRowKey("emptyReadTest" + System.nanoTime(), StringConverter)
      val column = MoColumn(family1, "emptyReadTest", StringConverter)

      val readResult = morice.read(tableName, rowKey, Seq(column))

      readResult must beEmpty
    }

  }

}
