package com.tagged.morice

import com.tagged.morice.PhoenixConversions.StringConverter
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, Get, Put, HConnectionManager}
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

  "withScanner" should {

    "let us scan using primitives" in {
      val now = System.nanoTime()
      val column = MoColumnFamily(family1).column("resultSetTest", StringConverter)

      // write some rows
      for ((i, word) <- Map(1->"one", 2->"two", 3->"three", 4->"four", 5->"five", 6->"six", 7->"seven", 8->"eight", 9->"nine")) {
        val rowKey = MoRowKey("readSet." + now + "." + i, StringConverter)
        val cell = new MoCell(column, word)
        morice.write(tableName, rowKey, Array(cell))
      }

      val startRow = ("readSet." + now).getBytes
      val endRow = ("readSet." + (now + 1)).getBytes
      val scan = new Scan(startRow, endRow)
      val readResult = morice.withScanner(tableName, scan) { results =>
        for (result <- results) yield {
          val cell = column.getCell(result)
          cell.value.get
        }
      }

      readResult must beEqualTo(List("one", "two", "three", "four", "five", "six", "seven", "eight", "nine"))
    }
  }

  "write/read single row key" should {

    "let us write and read specific column values" in {
      val rowKey = MoRowKey(System.nanoTime().toString, StringConverter)
      val column1 = MoColumnFamily(family1).column("columnReadWriteTest1", StringConverter)
      val column2 = MoColumnFamily(family2).column("columnReadWriteTest2", StringConverter)
      val cell1 = new MoCell(column1, System.nanoTime().toString)
      val cell2 = new MoCell(column2, System.nanoTime().toString)

      val cells = Array(cell1, cell2)
      morice.write(tableName, rowKey, cells)
      val readResult = morice.read(tableName, rowKey, Array(column1, column2))

      readResult.toArray must beEqualTo(cells)
    }

    "read empty column values as None" in {
      val rowKey = MoRowKey("emptyReadTest" + System.nanoTime(), StringConverter)
      val column = MoColumnFamily(family1).column("emptyReadTest", StringConverter)

      val readResult = morice.read(tableName, rowKey, Array(column))

      readResult.toArray must beEqualTo(Array(MoCell(column, None)))
    }

  }

}
