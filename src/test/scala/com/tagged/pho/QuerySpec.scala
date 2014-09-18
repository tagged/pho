package com.tagged.pho

import com.tagged.pho.phoenix.PhoenixConverters
import PhoenixConverters.StringConverter
import com.tagged.pho.filter._
import org.specs2.mutable.Specification

class QuerySpec extends Specification {

  import TestFixtures._

  val TestPrefix = "QuerySpec"
  val Number = Column(family1, s"$TestPrefix.Number", StringConverter)

  // write some rows
  val now = System.nanoTime()
  val docs = for ((i, word) <- List(1->"one", 2->"two", 3->"three", 4->"four", 5->"five", 6->"six", 7->"seven", 8->"eight", 9->"nine")) yield {
    val doc = new Document(
      RowKey(s"$TestPrefix.$now.$i", StringConverter),
      Seq(Cell(Number, word))
    )
    pho.write(testTableName, doc)
    doc
  }

  "Query" should {

    "retreive all documents in the original set" in {
      val query = Query(
        docs.head.key,
        RowKey(s"$TestPrefix.$now.Z", StringConverter),
        Seq(Number)
      )
      val result = pho.read(testTableName, query)

      result must beEqualTo(docs)
    }

    "use an inclusive start key, but exclusive end key" in {
      val query = Query(
        docs.head.key,
        docs.last.key,
        Seq(Number)
      )
      val result = pho.read(testTableName, query)

      result must beEqualTo(docs.take(docs.length - 1))
    }

  }

  "LimitFilter" should {

    "let us limit the length of the result set read" in {
      val query = Query(
        docs.head.key,
        docs.last.key,
        Seq(Number),
        Seq(LimitFilter(5))
      )
      val result = pho.read(testTableName, query)

      result must beEqualTo(docs.take(5))
    }

  }

  "EqualsFilter" should {

    "let us find elements with matching cell values" in {
      val searchCell = docs.slice(4, 5).head.cells.head
      val query = Query(
        docs.head.key,
        docs.last.key,
        Seq(Number),
        Seq(
          EqualsFilter(searchCell),
          LimitFilter(3)
        )
      )
      val result = pho.read(testTableName, query)

      result must beEqualTo(docs.slice(4, 5))
    }

  }

  "OrFilter" should {

    "let us combine filters" in {
      val searchCell1 = docs.slice(1, 2).head.cells.head
      val searchCell2 = docs.slice(4, 5).head.cells.head
      val query = Query(
        docs.head.key,
        docs.last.key,
        Seq(Number),
        Seq(
          OrFilter(
            EqualsFilter(searchCell1),
            EqualsFilter(searchCell2)
          )
        )
      )
      val result = pho.read(testTableName, query)

      result must beEqualTo(docs.slice(1, 2) ++ docs.slice(4, 5))
    }

  }

  "WhileFilter" should {

    "continue until a particular result is found" in {
      val searchCell = docs.slice(4,5).head.cells.head
      val query = Query(
        docs.head.key,
        docs.last.key,
        Seq(Number),
        Seq(
          WhileFilter(NotEqualsFilter(searchCell))
        )
      )
      val result = pho.read(testTableName, query)

      result must beEqualTo(docs.slice(0, 4))
    }

  }

}
