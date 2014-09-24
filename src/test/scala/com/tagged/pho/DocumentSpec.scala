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
import org.specs2.mutable.Specification

class DocumentSpec extends Specification {

  "getValue" should {

    "return the value matching a column" in {
      val colA = Column(ColumnFamily("fam1"), Qualifier("A"), PhoenixConverters.StringConverter)
      val colB = Column(ColumnFamily("fam2"), Qualifier("B"), PhoenixConverters.LongConverter)
      val colC = Column(ColumnFamily("fam3"), Qualifier("C"), PhoenixConverters.ByteConverter)
      val colD = Column(ColumnFamily("fam3"), Qualifier("D"), PhoenixConverters.ByteConverter)

      val cellA = Cell(colA, "Hello")
      val cellB = Cell(colB, 42L)
      val cellC = Cell(colC, 127.toByte)

      val doc = Document(RowKey(PhoenixConverters.LongConverter, 9L), Seq(cellA, cellB, cellC))

      doc.getValue(colC) must beSome(127.toByte)
      doc.getValue(colB) must beSome(42L)
      doc.getValue(colA) must beSome("Hello")
      doc.getValue(colD) must beNone
    }

  }

}
