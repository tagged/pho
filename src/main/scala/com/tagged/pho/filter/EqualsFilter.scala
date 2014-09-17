package com.tagged.pho.filter

import com.tagged.pho.Cell

case class EqualsFilter(cell: Cell[_]) extends PhoFilter {

  lazy val getFilter = cell.equalsFilter

}
