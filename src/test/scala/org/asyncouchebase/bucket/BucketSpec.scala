package org.asyncouchebase.bucket

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.{AsyncBucket, CouchbaseCluster}
import com.couchbase.client.java.auth.ClassicAuthenticator
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.query.{N1qlQuery, ParameterizedN1qlQuery}
import org.asyncouchbase.bucket.BucketApi
import org.asyncouchbase.example.User
import org.asyncouchbase.index.IndexApi
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Testing

import scala.concurrent.{Await, Awaitable}
import scala.concurrent.duration._

class BucketSpec extends Testing {

  trait Setup {

    val bucketImpl = new IndexApi  {
      override def asyncBucket: AsyncBucket = cluster.openBucket("bobbit").async()
    }

  }


  val cluster = CouchbaseCluster.create("localhost")
  cluster.authenticate(new ClassicAuthenticator().cluster("Administrator", "Administrator"))

  override protected def afterAll(): Unit = {
    cluster.disconnect()
  }


  "a Bucket" should {
    "return a document by key" in new Setup {

      //prepopulate db
      await(bucketImpl.upsert[User]("u:king_arthur", User("Arthur","kingarthur@couchbase.com",Seq("test","test2"))))

      val result = await(bucketImpl.get[User]("u:king_arthur"))
      result.isDefined shouldBe true
      result.get.name shouldBe "Arthur"
      result.get.email shouldBe "kingarthur@couchbase.com"
      result.get.interests.size shouldBe 2

    }

    "upsert a document" in new Setup {

      val result = await(bucketImpl.upsert[User]("u:rocco", User("rocco","eocco@test.com",Seq())))
      result.isSuccess shouldBe true

      val readDocument = await(bucketImpl.get[User]("u:rocco"))
      readDocument.isDefined shouldBe true
      readDocument.get.name shouldBe "rocco"
      readDocument.get.email shouldBe "eocco@test.com"
      readDocument.get.interests.size shouldBe 0


    }

    "return 2 documents in " in new Setup  {
      await(bucketImpl.upsert[User]("u:rocco1", User("rocco","eocco@test.com",Seq("test"))))
      await(bucketImpl.dropIndex("#primary"))
      await(bucketImpl.createPrimaryIndex(deferBuild = false))

      Thread.sleep(5000)

      val query = N1qlQuery.parameterized("SELECT name, email, interests FROM bobbit WHERE $1 IN interests",
        JsonArray.from("test"))
      val results = await(bucketImpl.find[User](query))

      results.size shouldBe 2
      await(bucketImpl.dropIndex("#primary"))
    }

    "delete a doc by key" in new Setup  {


      await(bucketImpl.upsert[User]("u:rocco23", User("rocco","eocco@test.com",Seq("test"))))

      val rec = await(bucketImpl.get[User]("u:rocco23"))
      rec.isDefined shouldBe true

      val result = await(bucketImpl.delete("u:rocco23"))
      result.isSuccess shouldBe true

      val recs = await(bucketImpl.get[User]("u:rocco23"))
      recs.isDefined shouldBe false

    }


  }


}
