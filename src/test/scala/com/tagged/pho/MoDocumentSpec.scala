package com.tagged.pho

import org.specs2.mutable.Specification

class MoDocumentSpec extends Specification {

  "getValue" should {

    "return the value matching a column" in {
      val colA = MoColumn(MoColumnFamily("fam1"), "A", PhoenixConversions.StringConverter)
      val colB = MoColumn(MoColumnFamily("fam2"), "B", PhoenixConversions.LongConverter)
      val colC = MoColumn(MoColumnFamily("fam3"), "C", PhoenixConversions.ByteConverter)
      val colD = MoColumn(MoColumnFamily("fam3"), "D", PhoenixConversions.ByteConverter)

      val cellA = MoCell(colA, "Hello")
      val cellB = MoCell(colB, 42L)
      val cellC = MoCell(colC, 127.toByte)

      val doc = MoDocument(MoRowKey(9L, PhoenixConversions.LongConverter), Iterable(cellA, cellB, cellC))

      doc.getValue(colC) must beSome(127.toByte)
      doc.getValue(colB) must beSome(42L)
      doc.getValue(colA) must beSome("Hello")
      doc.getValue(colD) must beNone
    }

  }

}
