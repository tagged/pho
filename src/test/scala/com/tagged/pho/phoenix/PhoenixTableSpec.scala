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

}
