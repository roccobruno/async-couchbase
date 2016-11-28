package org.asyncouchebase.bucket

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.{AsyncBucket, CouchbaseCluster}
import com.couchbase.client.java.auth.ClassicAuthenticator
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


      val result = await(bucketImpl.upsert[User]("u:king_arthur", User("rocco","eocco@test.com",Seq())))

    }

  }


}
