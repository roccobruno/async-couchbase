package org.asyncouchebase.bucket

import com.couchbase.client.java.auth.ClassicAuthenticator
import com.couchbase.client.java.error.DocumentAlreadyExistsException
import com.couchbase.client.java.{AsyncBucket, CouchbaseCluster}
import org.asyncouchbase.bucket.Path
import org.asyncouchbase.example.User
import org.asyncouchbase.index.IndexApi
import org.asyncouchbase.query.ExpressionImplicits._
import org.asyncouchbase.query.SELECT
import org.joda.time.DateTime
import play.api.libs.json.Json
import util.Testing

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/*
  IT needs a Couchbase instance running in localhost:8091 with default bucket
 */
class BucketSpec extends Testing {
  implicit val validateQuery = false

  case class ID(id: String)

  object ID {
    implicit val format = Json.format[ID]
  }

  val bucket = new IndexApi {
    override def asyncBucket: AsyncBucket = cluster.openBucket("default").async()
  }

  def deleteAllDocs: Future[Unit] = {

    val query = SELECT("*") FROM "default"

    def deleteAll(records: Seq[ID]): Unit = {
      records.foreach { rec: ID =>
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

    def isTherePrimaryIndex = await(bucket.findIndexes()).filter(_.name == "#primary").size > 0


    if (isTherePrimaryIndex)
      await(bucket.buildPrimaryIndex())
    else
      await(bucket.createPrimaryIndex(deferBuild = false))

    await(deleteAllDocs)
  }


  val cluster = CouchbaseCluster.create()
  cluster.authenticate(new ClassicAuthenticator().cluster("Administrator", "Administrator"))

  override protected def afterAll(): Unit = {
    bucket.asyncBucket.close()
    cluster.disconnect()
  }


  "a Bucket" should {
    "return a document by key" in  {

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

    "upsert a document" in  {

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

    "return 2 documents in " in  {

      await(bucket.upsert[User]("u:rocco1", User("rocco", "eocco@test.com", Seq("test"))))
      await(bucket.get[User]("u:rocco1")).isDefined shouldBe true
      await(bucket.upsert[User]("u:rocco2", User("rocco", "eocco@test.com", Seq("test"))))
      await(bucket.get[User]("u:rocco2")).isDefined shouldBe true
      await(bucket.upsert[User]("u:rocco3", User("rocco", "eocco@test.com", Seq("cacca"))))
      await(bucket.get[User]("u:rocco3")).isDefined shouldBe true

      Thread.sleep(10000)

      val query = SELECT("*") FROM "default" WHERE ("test" IN "interests")
      val results = await(bucket.find[User](query))

      results.size shouldBe 2
      //      clean up
      await(bucket.delete("u:rocco1"))
      await(bucket.delete("u:rocco2"))
      await(bucket.delete("u:rocco3"))
    }

    "delete a doc by key" in  {


      await(bucket.upsert[User]("u:rocco23", User("rocco", "eocco@test.com", Seq("test"))))

      val rec = await(bucket.get[User]("u:rocco23"))
      rec.isDefined shouldBe true

      val result = await(bucket.delete("u:rocco23"))
      result.isSuccess shouldBe true

      val recs = await(bucket.get[User]("u:rocco23"))
      recs.isDefined shouldBe false

    }

    "read single value from document" in  {
       val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val value = await(bucket.getValue[String](docId, "name", classOf[String]))
      value.get shouldBe "rocco"

      await(bucket.delete[User](docId))

    }

    "set single value to a document" in  {

      val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val res = await(bucket.setValue[String](docId, "name", "rocco2"))
      res.isSuccess shouldBe true

      val value = await(bucket.getValue[String](docId, "name", classOf[String]))
      value.get shouldBe "rocco2"


      await(bucket.delete[User](docId))

    }

    "set new key value pair to a document" in  {
       val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val res = await(bucket.setValue[String](docId, "surname", "bruno"))
      res.isSuccess shouldBe true

      val value = await(bucket.getValue[String](docId, "surname", classOf[String]))
      value.get shouldBe "bruno"

      await(bucket.delete[User](docId))
    }

    "set new multiple key value pairs to a document with path" in  {
      val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val values:Map[Path,Any] = Map(Path("surname") -> "bruno", Path("address.line1") -> "Flat 6 Osler Court", Path("name") -> "rocco22")

      val res = await(bucket.setValues(docId, values, createParent = true))
      res.isSuccess shouldBe true

      val user = await(bucket.get[User](docId))
      user.isDefined shouldBe true
      user.get.name shouldBe "rocco22"

      val value = await(bucket.getValue[String](docId, "surname", classOf[String]))
      value.get shouldBe "bruno"

      val valueAddress = await(bucket.getValue[String](docId, "address.line1", classOf[String]))
      valueAddress.get shouldBe "Flat 6 Osler Court"

      await(bucket.delete[User](docId))
    }

    "set new multiple key value pairs to a document with path without creating parent" in  {
      val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val values:Map[Path,Any] = Map( Path("address.line1") -> "Flat 6 Osler Court")

      val res = await(bucket.setValues(docId, values))
      res.isSuccess shouldBe false
      res.msg shouldBe ("Multiple mutation could not be applied. First problematic failure at 0 with status SUBDOC_PATH_NOT_FOUND")

     val valueAddress = await(bucket.getValue[String](docId, "address.line1", classOf[String]))
      valueAddress.isDefined shouldBe false

      await(bucket.delete[User](docId))
    }



    "set new multiple key value pairs to a document " in  {
      val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val values:Map[Path,Any] = Map(Path("surname") -> "bruno", Path("name") -> "rocco22")

      val res = await(bucket.setValues(docId, values))
      res.isSuccess shouldBe true

      val user = await(bucket.get[User](docId))
      user.isDefined shouldBe true
      user.get.name shouldBe "rocco22"

      val value = await(bucket.getValue[String](docId, "surname", classOf[String]))
      value.get shouldBe "bruno"

      await(bucket.delete[User](docId))
    }


    "set new multiple key value pairs to a document, invalid path" in  {
      intercept[IllegalArgumentException] {
        val values:Map[Path,Any] = Map(Path("surname.") -> "bruno", Path("..name") -> "rocco22")
        await(bucket.setValues("test", values))
      }

    }


    "return None if value not present in document" in  {
       val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val value = await(bucket.getValue[String](docId, "surname", classOf[String]))
      value shouldBe None

      await(bucket.delete[User](docId))

    }

    "query by dateTime" in  {
      await(bucket.buildPrimaryIndex())
       val docId: String = "u:testdate"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"), dob = DateTime.now().minusYears(20))))
      await(bucket.upsert[User](docId + "2", User("rocco2", "eocco@test.com", Seq("test"), dob = DateTime.now().minusYears(10))))

      val query = SELECT("name, email, interests, dob, id") FROM "default" WHERE ("dob" BETWEEN (DateTime.now().minusYears(11) AND DateTime.now()))

      val results = await(bucket.find[User](query))

      results.size shouldBe 1
      results(0).name shouldBe "rocco2"

      val query2 = SELECT("name, email, interests, dob, id") FROM "default" WHERE ("dob" BETWEEN (DateTime.now().minusYears(22) AND DateTime.now()))
      val results2 = await(bucket.find[User](query2))

      results2.size shouldBe 2

      val query3 = SELECT("name, email, interests, dob, id") FROM "default" WHERE ("dob" BETWEEN (DateTime.now().minusYears(30) AND DateTime.now().minusYears(15)))
      val results3 = await(bucket.find[User](query3))

      results3.size shouldBe 1
      results3(0).name shouldBe "rocco"

      await(bucket.delete[User](docId))
      await(bucket.delete[User](docId + "2"))


    }


    "throw an IllegalArgumentException in case of wrong selector" in {
      intercept[IllegalArgumentException] {
        val query = SELECT("test") FROM "default"

        implicit val validateQuery = true
        bucket.find[User](query)
      }
    }

    "check whether a document exists" in {
      val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))

      val res = await(bucket.exist(docId))
      res shouldBe true

      await(bucket.delete[User](docId))
    }

    "check whether a document does not exist" in {
      val docId: String = "u:test"
      val res = await(bucket.exist(docId))
      res shouldBe false
    }


    "expiry a doc" in {
      val docId: String = "u:test"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test")), expiry =  1))
      Thread.sleep(2000)
      val res = await(bucket.get[User](docId))
      res.isDefined shouldBe false
    }

    "get and reset expiry time" in {
      val docId: String = "u:test33"
      await(bucket.upsert[User](docId, User("rocco", "eocco@test.com", Seq("test")), expiry = 2))
      Thread.sleep(1000)

      val readDocument = await(bucket.getAndTouch[User](docId, expiry = 5))
      readDocument.isDefined shouldBe true
      Thread.sleep(2000)

      val res = await(bucket.get[User](docId))
      res.isDefined shouldBe true
    }

    "insert a doc" in {
      val docId: String = "u:test"
      await(bucket.insert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))
      val res = await(bucket.get[User](docId))
      res.isDefined shouldBe true
      await(bucket.delete[User](docId))
    }

    "throws an exception when trying to insert a doc with same key" in {
      val docId: String = "u:test"
      await(bucket.insert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))
      intercept[DocumentAlreadyExistsException] {
        await(bucket.insert[User](docId, User("rocco", "eocco@test.com", Seq("test"))))
      }
      await(bucket.delete[User](docId))

    }

  }


}
