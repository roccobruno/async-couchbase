package org.asyncouchebase.query

import org.asyncouchbase.query.Expression._
import org.asyncouchbase.query.Query
import util.Testing

class QuerySpec extends Testing {

  "query " should {

    "create the right N1SQL statement with single expression" in {

      val query = new Query() SELECT "*" FROM "default" WHERE "name" === "test"

      val n1qlQuery = query.buildQuery

      n1qlQuery.statement().toString shouldBe "SELECT * FROM default WHERE name = 'test'"

    }

    "create the right N1SQL statement with two expression in OR" in {

      val query = new Query() SELECT "*" FROM "default" WHERE  ("name" === "test" OR "name" === "test2")

      query.buildQuery.statement().toString shouldBe "SELECT * FROM default WHERE (name = 'test' OR name = 'test2')"

    }

    "create the right N1SQL statement with two expression in AND" in {

      val query = new Query() SELECT "*" FROM "default" WHERE  ("name" === "test" AND "name" === "test2")

      query.buildQuery.statement().toString shouldBe "SELECT * FROM default WHERE (name = 'test' AND name = 'test2')"

    }


    "create the right N1SQL statement with multiple expression" in {

      val query = new Query() SELECT "*" FROM "default" WHERE  (("name" === "test" AND "surname" === "white99") OR ("name" === "test2" AND "surname" === "white"))

      val n1qlQuery = query.buildQuery

      n1qlQuery.statement().toString shouldBe "SELECT * FROM default WHERE ((name = 'test' AND surname = 'white99') OR (name = 'test2' AND surname = 'white'))"

    }


    "create the right N1SQL statement with 'IN' operator expression" in {

      val query = new Query() SELECT "*" FROM "default" WHERE  ("test" IN "interest")

      val n1qlQuery = query.buildQuery

      n1qlQuery.statement().toString shouldBe "SELECT * FROM default WHERE 'test' IN interest"

    }


  }



}
