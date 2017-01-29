package org.asyncouchebase.query

import org.asyncouchbase.query.ExpressionImplicits._
import org.asyncouchbase.query._
import org.joda.time.DateTime
import util.Testing

class QuerySpec extends Testing {

  "simple query " should {

    "create the right N1SQL statement with no expression" in {

      val query = SELECT("name") FROM "default"

      query.toString shouldBe "SELECT name FROM default"

    }

    "create the right N1SQL statement with no expression but selected fields" in {

      val query = SELECT( "name, email, id") FROM "default"

      query.toString shouldBe "SELECT name, email, meta().id FROM default"

    }

    "create the right N1SQL statement with single expression" in {

      val query = SELECT( "name") FROM "default" WHERE "name" === "test"

      query.toString shouldBe "SELECT name FROM default WHERE name = 'test'"

    }

    "create the right N1SQL statement with two expression in OR" in {

      val query = SELECT( "name") FROM "default" WHERE  ("name" === "test" OR "name" === "test2")

      query.toString shouldBe "SELECT name FROM default WHERE (name = 'test' OR name = 'test2')"

    }

    "create the right N1SQL statement with two expression in AND" in {

      val query = SELECT( "name") FROM "default" WHERE  ("name" === "test" AND "name" === "test2")

      query.toString shouldBe "SELECT name FROM default WHERE (name = 'test' AND name = 'test2')"

    }

    "create the right N1SQL statement with three expression in AND" in {

      val query = SELECT( "name") FROM "default" WHERE  ("name" === "test" AND "name" === "test2" AND "name" === "test3")

      query.toString shouldBe "SELECT name FROM default WHERE ((name = 'test' AND name = 'test2') AND name = 'test3')"

    }


    "create the right N1SQL statement with multiple expression" in {

      val query = SELECT( "name") FROM "default" WHERE  (("name" === "test" AND "surname" === "white99") OR ("name" === "test2" AND "surname" === "white"))

      query.toString shouldBe "SELECT name FROM default WHERE ((name = 'test' AND surname = 'white99') OR (name = 'test2' AND surname = 'white'))"

    }


    "create the right N1SQL statement with 'IN' operator expression" in {

      val query = SELECT( "name") FROM "default" WHERE  ("test" IN "interest")

      query.toString shouldBe "SELECT name FROM default WHERE 'test' IN interest"

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

      val query = SELECT( "name") FROM "default" WHERE  ("dob" BETWEEN (from AND date))

      query.toString shouldBe "SELECT name FROM default WHERE dob BETWEEN STR_TO_MILLIS('2000-01-30T22:22:22.200Z') AND STR_TO_MILLIS('2001-01-31T22:22:22.201Z')"

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

      val query = SELECT( "name") FROM "default" WHERE ("name" === "teste" AND "name" === "teste2" AND ("dob" BETWEEN (from AND date)))

      query.toString shouldBe "SELECT name FROM default WHERE ((name = 'teste' AND name = 'teste2') AND dob BETWEEN STR_TO_MILLIS('2000-01-30T22:22:22.200Z') AND STR_TO_MILLIS('2001-01-31T22:22:22.201Z'))"

    }




    "create the right N1SQL statement with no expression and all fields" in {

      val query = SELECT("*") FROM "default"

      query.toString shouldBe "SELECT default.*,meta().id FROM default"

    }


    "create the right N1SQL statement with > operator expression and all fields" in {
      val query = SELECT("*") FROM "default" WHERE ("dob" gt "blahh")

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE dob > 'blahh'"

    }

    "create the right N1SQL statement with > operator expression with field of type Date" in {
      val date: DateTime = DateTime.now().withYear(2000).
        withDayOfMonth(30).
        withMonthOfYear(1).
        withHourOfDay(22).
        withMinuteOfHour(22).
        withSecondOfMinute(22).
        withMillisOfSecond(200)
      val query = SELECT("*") FROM "default" WHERE ("dob" gt date)

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE dob > STR_TO_MILLIS('2000-01-30T22:22:22.200Z')"

    }

    "create the right N1SQL statement with > operator expression with Number field type" in {

      val query = SELECT("*") FROM "default" WHERE ("dob" gt 3 )

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE dob > 3"

    }

    "create the right N1SQL statement with < operator expression with Number field type" in {

      val query = SELECT("*") FROM "default" WHERE ("dob" lt 3 )

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE dob < 3"

    }

    "create the right N1SQL statement with <= operator expression with Number field type" in {

      val query = SELECT("*") FROM "default" WHERE ("dob" lte 3 )

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE dob <= 3"

    }

    "create the right N1SQL statement with >= operator expression with Number field type" in {

      val query = SELECT("*") FROM "default" WHERE ("dob" gte 3 )

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE dob >= 3"

    }

    "create the right N1SQL statement with BETWEEN operator expression with Int field type" in {

      val query = SELECT("*") FROM "default" WHERE ("count" BETWEEN (0 AND 2))

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE count BETWEEN 0 AND 2"

    }

    "create the right N1SQL statement with Boolean Expression" in {

      val query = SELECT("*") FROM "default" WHERE ("recurring" === false)

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE recurring = false"

    }

    "create the right N1SQL statement with Array Expression" in {

      val query = SELECT("*") FROM "default" WHERE ( ANY("line") IN ("journey.meansOfTransportation.tubeLines") SATISFIES ("line.id" IN "['piccadilly', 'northern']"))

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE ANY line IN journey.meansOfTransportation.tubeLines SATISFIES line.id IN ['piccadilly', 'northern'] END"

    }

    "create the right N1SQL statement with Array Expression and RANGE expression" in {

      val query = SELECT("*") FROM "default" WHERE ((("docType" === "Job") AND ( "journey.startsAt.time" BETWEEN (1700 AND 1800))) AND (ANY("line") IN ("journey.meansOfTransportation.tubeLines") SATISFIES ("line.id" IN "['piccadilly', 'northern']")))

      query.toString shouldBe "SELECT default.*,meta().id FROM default WHERE ((docType = 'Job' AND journey.startsAt.time BETWEEN 1700 AND 1800) AND ANY line IN journey.meansOfTransportation.tubeLines SATISFIES line.id IN ['piccadilly', 'northern'] END)"

    }

    "create the right right N1SQL statement with IS NOT NULL expression" in {

      val query  = SELECT("id") FROM "tube" WHERE (ANY("line") IN "lineStatuses" SATISFIES ("line.disruption" IS_NOT_NULL))

      query.toString shouldBe "SELECT meta().id FROM tube WHERE ANY line IN lineStatuses SATISFIES line.disruption IS NOT NULL END"
    }


    "create the right right N1SQL statement with count  expression" in {

      val query  = COUNT() FROM "tube" WHERE ("name" === "test")

      query.toString shouldBe "SELECT count(*) as count FROM tube WHERE name = 'test'"

    }
  }



}
