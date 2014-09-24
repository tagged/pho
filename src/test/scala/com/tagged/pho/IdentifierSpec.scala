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

import org.specs2.mutable.Specification

class IdentifierSpec extends Specification {

  "equals" should {

    "return true for identical values" in {
      val x = new Identifier("foo")
      (x == x) must beTrue
    }

    "return true for deeply equal values" in {
      (new Identifier("foo") == new Identifier("foo")) must beTrue
      (new Identifier("foo") == new Identifier("bar")) must beFalse
    }

    "compare correctly as right-side map values" in {
      val map = Map(
        new Identifier("foo") -> "foo",
        new Identifier("bar") -> "bar"
      )
      map(new Identifier("foo")) must beEqualTo("foo")
      map(new Identifier("bar")) must beEqualTo("bar")
    }

  }

  class MyIdentifierSubclass(bytes: Array[Byte]) extends Identifier(bytes) {
    def this(name: String) = this(Identifier.bytesFromString(name))
  }

  "toString" should {

    "be brief and legible" in {
      new Identifier("foo").toString must beEqualTo("Identifier(foo)")
    }

    "subclasses output their own name by default" in {
      new MyIdentifierSubclass("foo").toString must beEqualTo("MyIdentifierSubclass(foo)")
    }

  }

}
