package org.asyncouchebase.bucket

import org.asyncouchbase.bucket.Path
import util.Testing

class PathSpec extends Testing {


  "a Path " should {

    "throw error in case of invalid value" in {
      intercept[IllegalArgumentException] (Path("name."))
    }

    "throw error in case of invalid value, different case" in {
      intercept[IllegalArgumentException] (Path("name.surname."))
    }

    "throw error in case of invalid value, different case again" in {
      intercept[IllegalArgumentException] (Path("name.surname.."))
    }

    "throw error in case of invalid value, different case again and again" in {
      intercept[IllegalArgumentException] (Path("name..."))
    }

    "accept valid path" in {
      Path("name")
      Path("name.value")
      Path("address.city.value")
    }


  }

}
