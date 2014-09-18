package com.tagged.pho

import com.tagged.pho.converter.PhoenixConverters
import PhoenixConverters.StringConverter
import com.tagged.pho.filter._
import org.apache.hadoop.hbase.client.{Scan, Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.specs2.mutable.Specification

class PhoConnectionSpec extends Specification {

  import TestFixtures._

  "withTable" should {

    "let us put and get using HTableInterface primitives" in {
      val rowKey = Bytes.toBytes(System.nanoTime().toString)
      val qualifier = Bytes.toBytes("primitiveReadWriteTest")
      val writeValue = Bytes.toBytes(System.nanoTime().toString)

      val readResult = pho.withTable(testTableName) { table =>
        val put = new Put(rowKey)
        put.add(family1.bytes, qualifier, writeValue)
        table.put(put)
        table.flushCommits()

        val get = new Get(rowKey)
        get.addColumn(family1.bytes, qualifier)
        val result = table.get(get)
        result.getValue(family1.bytes, qualifier)
      }

      readResult must beEqualTo(writeValue)
    }

  }

  "withScanner" should {

    "let us scan using primitives" in {
      val now = System.nanoTime()
      val column = Column(family1, "resultSetTest", StringConverter)

      // write some rows
      for ((i, word) <- Map(1->"one", 2->"two", 3->"three", 4->"four", 5->"five", 6->"six", 7->"seven", 8->"eight", 9->"nine")) {
        val doc = new Document(
          RowKey("readSet." + now + "." + i, StringConverter),
          Seq(Cell(column, word))
        )
        pho.write(testTableName, doc)
      }

      val startRow = Bytes.toBytes("readSet." + now)
      val endRow = Bytes.toBytes("readSet." + (now + 1))
      val scan = new Scan(startRow, endRow)
      val readResult = pho.withScanner(testTableName, scan) { results =>
        results.map(column.getCell).flatten.map(_.value)
      }

      readResult must beEqualTo(List("one", "two", "three", "four", "five", "six", "seven", "eight", "nine"))
    }
  }

  "write/read single row key" should {

    "let us write and read specific column values" in {
      val column1 = Column(family1, "columnReadWriteTest1", StringConverter)
      val column2 = Column(family2, "columnReadWriteTest2", StringConverter)

      val doc = Document(
        RowKey(System.nanoTime().toString, StringConverter),
        Seq(
          Cell(column1, System.nanoTime().toString),
          Cell(column2, System.nanoTime().toString)
        )
      )

      pho.write(testTableName, doc)
      val readResult = pho.read(testTableName, doc.key, doc.cells.map(_.column))

      readResult must beEqualTo(doc.cells)
    }

    "read empty column values as None" in {
      val rowKey = RowKey("emptyReadTest" + System.nanoTime(), StringConverter)
      val column = Column(family1, "emptyReadTest", StringConverter)

      val readResult = pho.read(testTableName, rowKey, Seq(column))

      readResult must beEmpty
    }

  }

}
