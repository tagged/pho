package com.tagged.morice

case class MoDocument[A](key: MoRowKey[A], values: Iterable[MoCell[_]]) {

  def getValue[X](column: MoColumn[X]): Option[X] = {
    values.find(_.column == column) match {
      case Some(cell) => Option(cell.value.asInstanceOf[X])
      case None => None
    }
  }

}
