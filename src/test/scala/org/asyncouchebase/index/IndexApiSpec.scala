package org.asyncouchebase.index

import com.couchbase.client.java.{AsyncBucket, CouchbaseCluster}
import org.asyncouchbase.index.IndexApi
import util.Testing

class IndexApiSpec  extends Testing {


  val cluster = CouchbaseCluster.create("localhost")
  val bucket = new IndexApi {
    override def asyncBucket: AsyncBucket = cluster.openBucket("default").async()
  }


  override protected def beforeAll(): Unit = {
//    await(bucket.dropAllIndexes())
  }

  override protected def afterAll(): Unit = {
    cluster.disconnect()
  }

  trait Setup {



  }

  "A bucket with index " should {
    "create a secondary index " in new Setup  {
      await(bucket.dropIndex("def_name"))
      val recs = await(bucket.createIndex(Seq("name")))
      recs.isSuccess shouldBe true
    }

    "find created indexes" in new Setup {
      await(bucket.createPrimaryIndex())
      val result = await(bucket.findIndexes())
      result.size shouldBe 2
    }

    "build primary index" in new Setup {
      await(bucket.dropIndex(bucket.PRIMARY_INDEX_NAME))

      await(bucket.createPrimaryIndex(deferBuild = true))
      await(bucket.buildPrimaryIndex())

      val result = await(bucket.findIndexes())
      result.filter(_.name == bucket.PRIMARY_INDEX_NAME).foreach {
        ind =>
          Seq("deferred","building", "online") should contain(ind.state)
      }
    }

    "build secondary index" in new Setup {

      await(bucket.createIndex(Seq("email"), deferBuild = true))
      await(bucket.buildIndex(Seq("email")))

      val result = await(bucket.findIndexes())
      result.filter(index => index.name == "def_email").foreach {
        ind =>
          Seq("deferred","building", "online") should contain(ind.state)
      }

      await(bucket.dropIndex("def_email"))
    }

    "delete index" in new Setup {
      val res = await(bucket.dropIndex("def_name"))
      res.isSuccess shouldBe true
    }

    "create primary index" in new Setup {
      await(bucket.dropIndex(bucket.PRIMARY_INDEX_NAME))

      val res = await(bucket.createPrimaryIndex())
      res.isSuccess shouldBe true
    }

    "fail when trying to create the same primary index twice" in new Setup {
      await(bucket.createPrimaryIndex())
      val res = await(bucket.createPrimaryIndex())
      res.isSuccess shouldBe false
    }

    "fail when trying to create the same secondaty index twice" in new Setup {
     await(bucket.createIndex(Seq("name")))
      val res = await(bucket.createIndex(Seq("name")))
      res.isSuccess shouldBe false
    }



  }

}
