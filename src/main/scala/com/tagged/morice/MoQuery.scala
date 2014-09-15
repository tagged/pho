package com.tagged.morice

case class MoQuery[A,B](
                    startRow: MoRowKey[A],
                    endRow: MoRowKey[A],
                    columns: Iterable[MoColumn[B]]
                    ) {

}
