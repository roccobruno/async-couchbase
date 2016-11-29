package org.asyncouchebase.bucket

import com.couchbase.client.java.auth.ClassicAuthenticator
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.{AsyncBucket, CouchbaseCluster}
import org.asyncouchbase.example.User
import org.asyncouchbase.index.IndexApi
import util.Testing

class BucketSpec extends Testing {

  trait Setup {

    val bucket = new IndexApi {
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
      await(bucket.upsert[User]("u:king_arthur", User("Arthur", "kingarthur@couchbase.com", Seq("test", "test2"))))

      val result = await(bucket.get[User]("u:king_arthur"))
      result.isDefined shouldBe true
      result.get.name shouldBe "Arthur"
      result.get.email shouldBe "kingarthur@couchbase.com"
      result.get.interests.size shouldBe 2

    }

    "upsert a document" in new Setup {

      val result = await(bucket.upsert[User]("u:rocco", User("rocco", "eocco@test.com", Seq())))
      result.isSuccess shouldBe true

      val readDocument = await(bucket.get[User]("u:rocco"))
      readDocument.isDefined shouldBe true
      readDocument.get.name shouldBe "rocco"
      readDocument.get.email shouldBe "eocco@test.com"
      readDocument.get.interests.size shouldBe 0


    }

    "return 2 documents in " in new Setup {
      await(bucket.upsert[User]("u:rocco1", User("rocco", "eocco@test.com", Seq("test"))))
      await(bucket.dropIndex(bucket.PRIMARY_INDEX_NAME))
      await(bucket.createPrimaryIndex(deferBuild = false))

      Thread.sleep(5000)

      val query = N1qlQuery.parameterized("SELECT name, email, interests FROM bobbit WHERE $1 IN interests",
        JsonArray.from("test"))
      val results = await(bucket.find[User](query))

      results.size shouldBe 2
      await(bucket.dropIndex(bucket.PRIMARY_INDEX_NAME))
    }

    "delete a doc by key" in new Setup {


      await(bucket.upsert[User]("u:rocco23", User("rocco", "eocco@test.com", Seq("test"))))

      val rec = await(bucket.get[User]("u:rocco23"))
      rec.isDefined shouldBe true

      val result = await(bucket.delete("u:rocco23"))
      result.isSuccess shouldBe true

      val recs = await(bucket.get[User]("u:rocco23"))
      recs.isDefined shouldBe false

    }

    "read single value from document" in new Setup {
      private val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val value = await(bucket.getValue[String](docId, "name", classOf[String]))
      value.get shouldBe "rocco"

      await(bucket.delete[User](docId))

    }

    "set single value to a document" in new Setup {

      private val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val res = await(bucket.setValue[String](docId, "name", "rocco2"))
      res.isSuccess shouldBe true

      val value = await(bucket.getValue[String](docId, "name", classOf[String]))
      value.get shouldBe "rocco2"


      await(bucket.delete[User](docId))

    }

    "set new key value pair to a document" in new Setup {
      private val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val res = await(bucket.setValue[String](docId, "surname", "bruno"))
      res.isSuccess shouldBe true

      val value = await(bucket.getValue[String](docId, "surname", classOf[String]))
      value.get shouldBe "bruno"

      await(bucket.delete[User](docId))
    }

    "return None if value not present in document" in new Setup {
      private val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val value = await(bucket.getValue[String](docId, "surname", classOf[String]))
      value shouldBe None

      await(bucket.delete[User](docId))

    }


  }


}
