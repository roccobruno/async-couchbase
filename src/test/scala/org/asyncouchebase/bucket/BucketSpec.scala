package org.asyncouchebase.bucket

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.{AsyncBucket, CouchbaseCluster}
import com.couchbase.client.java.auth.ClassicAuthenticator
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.query.N1qlQuery
import org.asyncouchbase.bucket.Bucket
import org.asyncouchbase.example.User
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Testing

import scala.concurrent.{Await, Awaitable}
import scala.concurrent.duration._

class BucketSpec extends Testing {


  val cluster = CouchbaseCluster.create("localhost")
  cluster.authenticate(new ClassicAuthenticator().cluster("Administrator", "Administrator"))

  override protected def afterAll(): Unit = {
    cluster.disconnect()
  }


  "a Bucket" should {
    "return a document by key" in {

      val bucketImpl = new Bucket {
        override def asyncBucket: AsyncBucket = cluster.openBucket("bobbit").async()
      }

      //prepopulate db
      await(bucketImpl.upsert[User]("u:king_arthur", User("Arthur","kingarthur@couchbase.com",Seq("test","test2"))))

      val result = await(bucketImpl.get[User]("u:king_arthur"))
      result.isDefined shouldBe true
      result.get.name shouldBe "Arthur"
      result.get.email shouldBe "kingarthur@couchbase.com"
      result.get.interests.size shouldBe 2

    }

    "upsert a document" in {

      val bucketImpl = new Bucket {
        override def asyncBucket: AsyncBucket = cluster.openBucket("bobbit").async()
      }
      val result = await(bucketImpl.upsert[User]("u:rocco", User("rocco","eocco@test.com",Seq())))
      result.isSuccess shouldBe true

      val readDocument = await(bucketImpl.get[User]("u:rocco"))
      readDocument.isDefined shouldBe true
      readDocument.get.name shouldBe "rocco"
      readDocument.get.email shouldBe "eocco@test.com"
      readDocument.get.interests.size shouldBe 0


    }

    "return 2 documents in " in {
      val bucketImpl = new Bucket {
        override def asyncBucket: AsyncBucket = cluster.openBucket("bobbit").async()
      }
      await(bucketImpl.upsert[User]("u:rocco2", User("rocco","eocco@test.com",Seq("test"))))

      val results = await(bucketImpl.find[User](N1qlQuery.parameterized("SELECT name, email, interests FROM bobbit WHERE $1 IN interests",
        JsonArray.from("test"))))

      results.size shouldBe 2
    }

  }


}
