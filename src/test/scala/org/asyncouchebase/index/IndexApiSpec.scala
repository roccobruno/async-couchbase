package org.asyncouchebase.index

import com.couchbase.client.java.auth.ClassicAuthenticator
import com.couchbase.client.java.{AsyncBucket, CouchbaseCluster}
import org.asyncouchbase.index.IndexApi
import util.Testing

class IndexApiSpec  extends Testing {


  val cluster = CouchbaseCluster.create("localhost")
  cluster.authenticate(new ClassicAuthenticator().cluster("Administrator", "Administrator"))

  override protected def afterAll(): Unit = {
    cluster.disconnect()
  }

  trait Setup {

    val bucketImpl = new IndexApi {
      override def asyncBucket: AsyncBucket = cluster.openBucket("bobbit").async()
    }

  }

  "A bucket with index " should {

//
//    "create a secondary index " in new Setup  {
//
//      val recs = await(bucketImpl.createIndex(Seq("name")))
//      recs.isSuccess shouldBe true
//
//    }

    "find created indexes" in new Setup {

      val result = await(bucketImpl.findIndexes())
      result.size shouldBe 3

      result.foreach(r=> println(r))

    }



  }

}
