package org.asyncouchebase.query

import org.asyncouchbase.example.{Token, User}
import org.asyncouchbase.query.Expression._
import org.asyncouchbase.query._
import org.joda.time.DateTime
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

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE  (("name" === "test") OR "name" === "test2")

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

      val from: DateTime = DateTime.now().withYear(2000).
        withDayOfMonth(30).
        withMonthOfYear(1).
        withHourOfDay(22).
        withMinuteOfHour(22).
        withSecondOfMinute(22).
        withMillisOfSecond(200)

      val date: DateTime = DateTime.now().withYear(2001).
        withDayOfMonth(31).
        withMonthOfYear(1).
        withHourOfDay(22).
        withMinuteOfHour(22).
        withSecondOfMinute(22).
        withMillisOfSecond(201)

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE  ("dob" BETWEEN (from AND date))

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE dob BETWEEN STR_TO_MILLIS('2000-01-30T22:22:22.200Z') AND STR_TO_MILLIS('2001-01-31T22:22:22.201Z')"

    }


    "create the right N1SQL statement with 'BETWEEN' and 'AND operator expressions" in {

      val from: DateTime = DateTime.now().withYear(2000).
        withDayOfMonth(30).
        withMonthOfYear(1).
        withHourOfDay(22).
        withMinuteOfHour(22).
        withSecondOfMinute(22).
        withMillisOfSecond(200)

      val date: DateTime = DateTime.now().withYear(2001).
        withDayOfMonth(31).
        withMonthOfYear(1).
        withHourOfDay(22).
        withMinuteOfHour(22).
        withSecondOfMinute(22).
        withMillisOfSecond(201)

      val query = new SimpleQuery[User]() SELECT "name" FROM "default" WHERE ("name" === "teste").AND("name" === "teste2").AND("dob" BETWEEN (from AND date))

      query.buildQuery.statement().toString shouldBe "SELECT name FROM default WHERE ((name = 'teste' AND name = 'teste2') AND dob BETWEEN STR_TO_MILLIS('2000-01-30T22:22:22.200Z') AND STR_TO_MILLIS('2001-01-31T22:22:22.201Z'))"

    }

    "throw an IllegalArgumentException in case of wrong selector" in {
      intercept[IllegalArgumentException] {
        new SimpleQuery[User]() SELECT "test" FROM "default"
      }
    }


    "create the right N1SQL statement with no expression and all fields" in {

      val query = new SimpleQuery[User]() SELECT "*" FROM "default"

      query.buildQuery.statement().toString shouldBe "SELECT default.*,meta().id FROM default"

    }


    "create the right N1SQL statement with > operator expression and all fields" in {
      val query = new SimpleQuery[User]() SELECT "*" FROM "default" WHERE ("dob" gt "blahh")

      query.buildQuery.statement().toString shouldBe "SELECT default.*,meta().id FROM default WHERE dob > 'blahh'"

    }

    "create the right N1SQL statement with > operator expression with field of type Date" in {
      val date: DateTime = DateTime.now().withYear(2000).
        withDayOfMonth(30).
        withMonthOfYear(1).
        withHourOfDay(22).
        withMinuteOfHour(22).
        withSecondOfMinute(22).
        withMillisOfSecond(200)
      val query = new SimpleQuery[User]() SELECT "*" FROM "default" WHERE ("dob" gt date)

      query.buildQuery.statement().toString shouldBe "SELECT default.*,meta().id FROM default WHERE dob > STR_TO_MILLIS('2000-01-30T22:22:22.200Z')"

    }

    "create the right N1SQL statement with > operator expression with Number field type" in {

      val query = new SimpleQuery[Token]() SELECT "*" FROM "default" WHERE ("dob" gt 3 )

      query.buildQuery.statement().toString shouldBe "SELECT default.*,meta().id FROM default WHERE dob > 3"

    }

    "create the right N1SQL statement with BETWEEN operator expression with Int field type" in {

      val query = new SimpleQuery[Token]() SELECT "*" FROM "default" WHERE ("count" BETWEEN (0 AND 2))

      query.buildQuery.statement().toString shouldBe "SELECT default.*,meta().id FROM default WHERE count BETWEEN 0 AND 2"

    }

    "create the right N1SQL statement with Boolean Expression" in {

      val query = new SimpleQuery[Token]() SELECT "*" FROM "default" WHERE ("recurring" === false)

      query.buildQuery.statement().toString shouldBe "SELECT default.*,meta().id FROM default WHERE recurring = false"

    }

  }



}
