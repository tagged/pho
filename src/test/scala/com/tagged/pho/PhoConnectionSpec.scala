package com.tagged.pho

import com.tagged.pho.PhoenixConversions.StringConverter
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, Get, Put, HConnectionManager}
import org.apache.hadoop.hbase.util.Bytes
import org.specs2.mutable.Specification

/**
 * This test requires that a test table be pre-created on the HBase cluster:
 *
 *     create 'PhoIntegrationTests', 'family1', 'family2'
 */
class PhoConnectionSpec extends Specification {

  val tableName = "PhoIntegrationTests"
  val family1 = MoColumnFamily("family1")
  val family2 = MoColumnFamily("family2")

  val configuration = HBaseConfiguration.create()
  scala.util.Properties.propOrNone("hbase.zookeeper.quorum") match {
    case Some(quorum) => configuration.set("hbase.zookeeper.quorum", quorum)
    case None => Unit
  }

  val connection = HConnectionManager.createConnection(configuration)
  val pho = new PhoConnection(connection)

  "withTable" should {

    "let us put and get using HTableInterface primitives" in {
      val rowKey = Bytes.toBytes(System.nanoTime().toString)
      val qualifier = Bytes.toBytes("primitiveReadWriteTest")
      val writeValue = Bytes.toBytes(System.nanoTime().toString)

      val readResult = pho.withTable(tableName) { table =>
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
        pho.write(tableName, doc)
      }

      val startRow = Bytes.toBytes("readSet." + now)
      val endRow = Bytes.toBytes("readSet." + (now + 1))
      val scan = new Scan(startRow, endRow)
      val readResult = pho.withScanner(tableName, scan) { results =>
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

      pho.write(tableName, doc)
      val readResult = pho.read(tableName, doc.key, doc.values.map(_.column))

      readResult must beEqualTo(doc.values)
    }

    "read empty column values as None" in {
      val rowKey = MoRowKey("emptyReadTest" + System.nanoTime(), StringConverter)
      val column = MoColumn(family1, "emptyReadTest", StringConverter)

      val readResult = pho.read(tableName, rowKey, Seq(column))

      readResult must beEmpty
    }

  }

  "queries" should {
    val now = System.nanoTime()
    val column = MoColumn(family1, "queryResultSetTest", StringConverter)

    // write some rows
    val docs = for ((i, word) <- List(1->"one", 2->"two", 3->"three", 4->"four", 5->"five", 6->"six", 7->"seven", 8->"eight", 9->"nine")) yield {
      val doc = new MoDocument(
        MoRowKey("querySet." + now + "." + i, StringConverter),
        Seq(MoCell(column, word))
      )
      pho.write(tableName, doc)
      doc
    }

    "let us read multiple documents at once" in {
      val query = MoQuery(
        MoRowKey("querySet." + now + ".", StringConverter),
        MoRowKey("querySet." + now + "z", StringConverter),
        Seq(column)
      )
      val result = pho.read(tableName, query)

      result must beEqualTo(docs)
    }

    "let us limit the length of the result set read" in {
      val query = MoQuery(
        MoRowKey("querySet." + now + ".", StringConverter),
        MoRowKey("querySet." + now + "z", StringConverter),
        Seq(column),
        Seq(MoFilter.LimitFilter(5))
      )
      val result = pho.read(tableName, query)

      result must beEqualTo(docs.slice(0, 5))
    }

    "let us find elements using an equality filter" in {
      val searchCell = docs.slice(4,5).head.values.head
      val query = MoQuery(
        MoRowKey("querySet." + now + ".", StringConverter),
        MoRowKey("querySet." + now + "z", StringConverter),
        Seq(column),
        Seq(MoFilter.CellEqualsFilter(searchCell), MoFilter.LimitFilter(3))
      )
      val result = pho.read(tableName, query)

      result must beEqualTo(docs.slice(4, 5))
    }

    "let us find elements using an multiple equality filters ORed together" in {
      val searchCell1 = docs.slice(1,2).head.values.head
      val searchCell2 = docs.slice(4,5).head.values.head
      val query = MoQuery(
        MoRowKey("querySet." + now + ".", StringConverter),
        MoRowKey("querySet." + now + "z", StringConverter),
        Seq(column),
        Seq(
          MoFilter.OrFilter(
            MoFilter.CellEqualsFilter(searchCell1),
            MoFilter.CellEqualsFilter(searchCell2)
          )
        )
      )
      val result = pho.read(tableName, query)

      result must beEqualTo(docs.slice(1,2) ++ docs.slice(4, 5))
    }

  }

}
