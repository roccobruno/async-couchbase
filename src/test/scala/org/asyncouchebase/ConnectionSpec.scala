package org.asyncouchebase

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.auth.ClassicAuthenticator
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.document.json.JsonObject._
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.query.{N1qlQuery, N1qlQueryResult, N1qlQueryRow}
import org.scalatest._

class ConnectionSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val env = DefaultCouchbaseEnvironment.builder()
    .socketConnectTimeout(TimeUnit.SECONDS.toMillis(100).toInt)
  .connectTimeout(TimeUnit.SECONDS.toMillis(1000)).queryTimeout(500000)
    .build();

  val cluster  = CouchbaseCluster.create("localhost")
    cluster.authenticate(new ClassicAuthenticator().cluster("Administrator", "Administrator"))

  override protected def afterAll(): Unit = {
    cluster.disconnect()
  }


  "A connector" should "be able to connect" in {


    val bucket = cluster.openBucket("bobbit")

    // Create a JSON Document
    val arthur = create()
      .put("name", "Arthur")
      .put("email", "kingarthur@couchbase.com")
      .put("interests", JsonArray.from("Holy Grail", "African Swallows"))


    // Store the Document
    bucket.upsert(JsonDocument.create("u:king_arthur", arthur),1, TimeUnit.MINUTES)

    // Load the Document and print it
    // Prints Content and Metadata of the stored Document
    System.out.println(bucket.get("u:king_arthur"))

    // Create a N1QL Primary Index (but ignore if it exists)
    bucket.bucketManager().createN1qlPrimaryIndex(true, false)

    // Perform a N1QL Query
    val result = bucket.query(
      N1qlQuery.parameterized("SELECT name FROM default WHERE $1 IN interests",
        JsonArray.from("African Swallows"))
    )

    // Print each found Row
      result.allRows()



  }




}
