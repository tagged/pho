package com.tagged.pho.filter

import com.tagged.pho.MoCell

case class EqualsFilter(cell: MoCell[_]) extends PhoFilter {

  lazy val getFilter = cell.equalsFilter

}
