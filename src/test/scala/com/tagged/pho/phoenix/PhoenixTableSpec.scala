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

package com.tagged.pho.phoenix

import org.specs2.mutable.Specification
import PhoenixFixtures._

class PhoenixTableSpec extends Specification {

  skipAllIf(!phoenixIsAvailable)

  "withConnection" should {

    "connect without complaint" in {
      val result = withConnection({ connection =>
        connection.isValid(1)  // 1 second
      })
      result must beTrue
    }

  }

  "select" should {

    "read the state of a phoenix table" in {
      val systemCatalog = select("SELECT * FROM system.catalog")
      systemCatalog.length must beGreaterThan(0)
    }

  }

  "execute" should {

    "let us make new tables" in {
      execute("CREATE TABLE IF NOT EXISTS foo (foo_id INTEGER PRIMARY KEY, bar VARCHAR)")
      success
    }

  }

}
