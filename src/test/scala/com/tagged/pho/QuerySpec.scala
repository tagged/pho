/*
 * Copyright 2014 Tagged
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tagged.pho

import com.tagged.pho.phoenix.PhoenixConverters
import PhoenixConverters._
import com.tagged.pho.filter._
import org.specs2.mutable.Specification

class QuerySpec extends Specification {

  import TestFixtures._

  val TestPrefix = "QuerySpec"
  val Number = Column(family1, s"$TestPrefix.Number", IntConverter)
  val Text   = Column(family1, s"$TestPrefix.Text", StringConverter)
  val Even   = Column(family1, s"$TestPrefix.Even", BooleanConverter)

  val testData = Seq(
    Seq(
      Cell(Number, 1),
      Cell(Text, "one")
    ),
    Seq(
      Cell(Even, true),
      Cell(Number, 2),
      Cell(Text, "two")
    ),
    Seq(
      Cell(Number, 3),
      Cell(Text, "three")
    ),
    Seq(
      Cell(Even, true),
      Cell(Number, 4),
      Cell(Text, "four")
    ),
    Seq(
      Cell(Number, 5),
      Cell(Text, "five")
    ),
    Seq(
      Cell(Even, true),
      Cell(Number, 6),
      Cell(Text, "six")
    ),
    Seq(
      Cell(Number, 7),
      Cell(Text, "seven")
    ),
    Seq(
      Cell(Even, true),
      Cell(Number, 8),
      Cell(Text, "eight")
    ),
    Seq(
      Cell(Number, 9),
      Cell(Text, "nine")
    )
  )
  
  // write some rows
  val now = System.nanoTime()
  var i = 0
  val docs = for (cells <- testData) yield {
    i = i + 1
    val doc = new Document(
      RowKey(StringConverter, s"$TestPrefix.$now.$i"),
      cells
    )
    testTable.write(doc)
    doc
  }

  val endKey = RowKey(StringConverter, s"$TestPrefix.$now.Z")

  "Query" should {

    "retreive all documents in the original set" in {
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even)
      )
      val result = testTable.read(query)

      result.toList must beEqualTo(docs)
    }

    "use an inclusive start key, but exclusive end key" in {
      val query = Query(
        docs.head.key,
        docs.last.key,
        Seq(Number, Text, Even)
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.take(docs.length - 1))
    }

  }

  "LimitFilter" should {

    "let us limit the length of the result set read" in {
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        LimitFilter(5)
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.take(5))
    }

  }

  "EqualsFilter" should {

    "let us find elements with matching cell values" in {
      val searchCell = docs.slice(4, 5).head.getCell(Number).get
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        EqualsFilter(searchCell)
        and LimitFilter(3)
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.slice(4, 5))
    }

  }

  "OrFilter" should {

    "let us combine filters" in {
      val searchCell1 = docs.slice(1, 2).head.getCell(Number).get
      val searchCell2 = docs.slice(4, 5).head.getCell(Number).get
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        EqualsFilter(searchCell1)
        or EqualsFilter(searchCell2)
      )
      val result = testTable.read(query)

      result.toList must beEqualTo(docs.slice(1, 2) ++ docs.slice(4, 5))
    }

  }

  "WhileFilter" should {

    "continue until a particular result is found" in {
      val searchCell = docs.slice(4,5).head.getCell(Number).get
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        WhileFilter(NotEqualsFilter(searchCell))
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.slice(0, 4))
    }

  }

  "EmptyFilter" should {

    "only include any columns that have no value" in {
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        EmptyFilter(Even)
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.filter(_.getValue(Number).get % 2 == 1))
    }

  }

  "NotEmptyFilter" should {

    "only include columns that have a value" in {
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        NotEmptyFilter(Even)
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.filter(_.getValue(Number).get % 2 == 0))
    }

  }

  "LessThanFilter" should {

    "include columns with a lexical value lower than the one compared" in {
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        LessThanFilter(Cell(Number, 4))
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.take(3))
    }

  }

  "LessThanOrEqualFilter" should {

    "include columns with a lexical value equal to or lower than the one compared" in {
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        LessThanOrEqualFilter(Cell(Number, 4))
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.take(4))
    }

  }

  "GreaterThanFilter" should {

    "include columns with a lexical value higher than the one compared" in {
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        GreaterThanFilter(Cell(Number, 4))
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.drop(4))
    }

  }

  "GreaterThanOrEqualFilter" should {

    "include columns with a lexical value higher than the one compared" in {
      val query = Query(
        docs.head.key,
        endKey,
        Seq(Number, Text, Even),
        GreaterThanOrEqualFilter(Cell(Number, 4))
      )
      val result = testTable.read(query)

      result must beEqualTo(docs.drop(3))
    }

  }

}
