package org.asyncouchebase.bucket

import com.couchbase.client.java.auth.ClassicAuthenticator
import com.couchbase.client.java.{AsyncBucket, CouchbaseCluster}
import org.asyncouchbase.example.User
import org.asyncouchbase.index.IndexApi
import org.asyncouchbase.query.Expression._
import org.asyncouchbase.query.SimpleQuery
import org.joda.time.DateTime
import play.api.libs.json.Json
import util.Testing

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future



/*
  IT needs a Couchbase instance running in localhost:8091 with default bucket
 */
class BucketSpec extends Testing {


  case class ID(id: String)
  object ID {
    implicit val format = Json.format[ID]
  }

  val bucket = new IndexApi {
    override def asyncBucket: AsyncBucket = cluster.openBucket("default").async()
  }

  def deleteAllDocs: Future[Unit] = {

    val query = new SimpleQuery[ID]() SELECT "*" FROM "default"

    def deleteAll(records: Seq[ID]): Unit = {
      records.foreach { rec:ID =>
        bucket.delete(rec.id)
      }
    }


    bucket.find[ID](query) map {
      records => println(s"$records"); deleteAll(records)
    }
  }

  trait Setup {





  }

  override protected def beforeAll(): Unit = {
    await(deleteAllDocs)
    await(bucket.dropAllIndexes())
    await(bucket.createPrimaryIndex(deferBuild = false))
  }


  val cluster = CouchbaseCluster.create("localhost")
  cluster.authenticate(new ClassicAuthenticator().cluster("Administrator", "Administrator"))

  override protected def afterAll(): Unit = {
//    await(bucket.dropAllIndexes())
    bucket.asyncBucket.close()
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

      //clean up
      await(bucket.delete("u:king_arthur"))

    }

    "upsert a document" in new Setup {

      val result = await(bucket.upsert[User]("u:rocco", User("rocco", "eocco@test.com", Seq())))
      result.isSuccess shouldBe true

      val readDocument = await(bucket.get[User]("u:rocco"))
      readDocument.isDefined shouldBe true
      readDocument.get.name shouldBe "rocco"
      readDocument.get.email shouldBe "eocco@test.com"
      readDocument.get.interests.size shouldBe 0

      //clean up
      await(bucket.delete("u:rocco"))
    }

    "return 2 documents in " in new Setup {

      await(bucket.upsert[User]("u:rocco1", User("rocco", "eocco@test.com", Seq("test"))))
      await(bucket.get[User]("u:rocco1")).isDefined shouldBe true
      await(bucket.upsert[User]("u:rocco2", User("rocco", "eocco@test.com", Seq("test"))))
      await(bucket.get[User]("u:rocco2")).isDefined shouldBe true
      await(bucket.upsert[User]("u:rocco3", User("rocco", "eocco@test.com", Seq("cacca"))))
      await(bucket.get[User]("u:rocco3")).isDefined shouldBe true

      Thread.sleep(5000)

      val query = new SimpleQuery[User]() SELECT "*" FROM "default" WHERE ("test" IN "interests")

      await(bucket.find[User](query))
      val results = await(bucket.find[User](query))

      results.size shouldBe 2
      //      clean up
      await(bucket.delete("u:rocco1"))
      await(bucket.delete("u:rocco2"))
      await(bucket.delete("u:rocco3"))
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

    "query by dateTime" in new Setup {

      private val docId: String = "u:testdate"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"), dob = DateTime.now().minusYears(20))))
      await(bucket.upsert[User](docId+"2", User("rocco2", "eocco@test.com", Seq("test"), dob = DateTime.now().minusYears(10))))

      Thread.sleep(5000)

      val query = new SimpleQuery[User]() SELECT "name, email, interests, dob, id" FROM "default" WHERE ("dob" BETWEEN (DateTime.now().minusYears(11) AND DateTime.now()))

      val results = await(bucket.find[User](query))

      results.size shouldBe 1
      results(0).name shouldBe "rocco2"

      val query2 = new SimpleQuery[User]() SELECT "name, email, interests, dob, id" FROM "default" WHERE ("dob" BETWEEN (DateTime.now().minusYears(22) AND DateTime.now()))
      val results2 = await(bucket.find[User](query2))

      results2.size shouldBe 2

      val query3 = new SimpleQuery[User]() SELECT "name, email, interests, dob, id" FROM "default" WHERE ("dob" BETWEEN (DateTime.now().minusYears(30) AND DateTime.now().minusYears(15)))
      val results3 = await(bucket.find[User](query3))

      results3.size shouldBe 1
      results3(0).name shouldBe "rocco"

      await(bucket.delete[User](docId))
      await(bucket.delete[User](docId+"2"))


    }


  }


}
