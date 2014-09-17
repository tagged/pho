package com.tagged.pho

import com.tagged.pho.converter.PhoenixConverters
import org.specs2.mutable.Specification

class DocumentSpec extends Specification {

  "getValue" should {

    "return the value matching a column" in {
      val colA = Column(ColumnFamily("fam1"), "A", PhoenixConverters.StringConverter)
      val colB = Column(ColumnFamily("fam2"), "B", PhoenixConverters.LongConverter)
      val colC = Column(ColumnFamily("fam3"), "C", PhoenixConverters.ByteConverter)
      val colD = Column(ColumnFamily("fam3"), "D", PhoenixConverters.ByteConverter)

      val cellA = Cell(colA, "Hello")
      val cellB = Cell(colB, 42L)
      val cellC = Cell(colC, 127.toByte)

      val doc = Document(RowKey(9L, PhoenixConverters.LongConverter), Iterable(cellA, cellB, cellC))

      doc.getValue(colC) must beSome(127.toByte)
      doc.getValue(colB) must beSome(42L)
      doc.getValue(colA) must beSome("Hello")
      doc.getValue(colD) must beNone
    }

  }

}
