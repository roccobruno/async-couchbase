package org.asyncouchebase.query

import org.asyncouchbase.example.User
import org.asyncouchbase.query.Expression._
import org.asyncouchbase.query._
import util.Testing

class QuerySpec extends Testing {

  "simple query " should {

    "create the right N1SQL statement with no expression" in {

      val query = new SimpleQuery[User]() SELECT "name" FROM "default"

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default"

    }

    "create the right N1SQL statement with no expression but selected fields" in {

      val query = new SimpleQuery[User]() SELECT "name, email, id" FROM "default"

      query.buildQuery.statement().toString shouldBe "SELECT name, email, meta().id FROM default"

    }

    "create the right N1SQL statement with single expression" in {

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE "name" === "test"

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE name = 'test'"

    }

    "create the right N1SQL statement with two expression in OR" in {

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE  ("name" === "test" OR "name" === "test2")

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE (name = 'test' OR name = 'test2')"

    }

    "create the right N1SQL statement with two expression in AND" in {

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE  ("name" === "test" AND "name" === "test2")

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE (name = 'test' AND name = 'test2')"

    }

    "create the right N1SQL statement with three expression in AND" in {

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE  ("name" === "test" AND "name" === "test2" AND "name" === "test3")

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE ((name = 'test' AND name = 'test2') AND name = 'test3')"

    }


    "create the right N1SQL statement with multiple expression" in {

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE  (("name" === "test" AND "surname" === "white99") OR ("name" === "test2" AND "surname" === "white"))

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE ((name = 'test' AND surname = 'white99') OR (name = 'test2' AND surname = 'white'))"

    }


    "create the right N1SQL statement with 'IN' operator expression" in {

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE  ("test" IN "interest")

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE 'test' IN interest"

    }


    "create the right N1SQL statement with 'BETWEEN' operator expression" in {

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE  ("dob" BETWEEN ("2016-11-08T17:08:35.389Z" AND "2016-12-08T17:08:35.389Z"))

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE dob BETWEEN STR_TO_MILLIS('2016-11-08T17:08:35.389Z') AND STR_TO_MILLIS('2016-12-08T17:08:35.389Z')"

    }


    "create the right N1SQL statement with 'BETWEEN' and 'AND operator expressions" in {

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE ("name" === "teste").AND("name" === "teste2").AND("dob" BETWEEN ("2016-11-08T17:08:35.389Z" AND "2016-12-08T17:08:35.389Z"))

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE ((name = 'teste' AND name = 'teste2') AND dob BETWEEN STR_TO_MILLIS('2016-11-08T17:08:35.389Z') AND STR_TO_MILLIS('2016-12-08T17:08:35.389Z'))"

    }

    "throw an IllegalArgumentException in case of wrong selector" in {
      intercept[IllegalArgumentException] {
        new SimpleQuery[User]() SELECT "test" FROM "default"
      }
    }


    "create the right N1SQL statement with no expression and all fields" in {

      val query = new SimpleQuery[User]() SELECT "*" FROM "default"

      query.buildQuery.statement().toString shouldBe "SELECT name,email,interests,dob,meta().id FROM default"

    }


  }



}
