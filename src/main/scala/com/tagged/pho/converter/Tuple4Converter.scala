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

package com.tagged.pho.converter

case class Tuple4Converter[A, B, C, D](a: PhoConverter[A], b: PhoConverter[B], c: PhoConverter[C], d: PhoConverter[D]) extends PhoConverter[(A, B, C, D)] {
  def toBytes(value: (A, B, C, D)) = Array.concat(a.toBytes(value._1), b.toBytes(value._2), c.toBytes(value._3), d.toBytes(value._4))

  def getToken(bytes: Array[Byte]): ConverterToken[(A, B, C, D)] = {
    val token1 = a.getToken(bytes)
    val after1 = bytes.drop(token1.bytes.length)
    val token2 = b.getToken(after1)
    val after2 = after1.drop(token2.bytes.length)
    val token3 = c.getToken(after2)
    val after3 = after2.drop(token2.bytes.length)
    val token4 = d.getToken(after3)
    val token = Array.concat(token1.bytes, token2.bytes, token3.bytes, token4.bytes)
    val value = (token1.value, token2.value, token3.value, token4.value)
    ConverterToken(token, value)
  }

}
