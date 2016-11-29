package org.asyncouchbase.index

import com.couchbase.client.java.query.N1qlQuery.simple
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.{Index, N1qlQuery, Select}
import com.couchbase.client.java.query.dsl.Expression._
import org.asyncouchbase.Converters.toFuture
import org.asyncouchbase.bucket.BucketApi
import org.asyncouchbase.model.{CBIndex, OpsResult}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait IndexApi extends BucketApi {

  def createIndex[T](fields: Seq[String]): Future[OpsResult] = {

    var indexName = "def_"

    fields.foreach(field =>
      indexName = s"${indexName}${field}_"
    )

    val indName = indexName.dropRight(1)
    val stat = Index.createIndex(indName).on("bobbit", x(indName.replace("def_", ""))).withDefer()

    toFuture(asyncBucket.query(simple(stat))) map {
      result =>
        OpsResult(isSuccess = result.parseSuccess()) //TODO replace to use finalSuccess
    }
  }


  def findIndexes(): Future[List[CBIndex]] = {

    val query = simple(
      select("indexes.*").
      from("system:indexes").
        where(i("keyspace_id").
          eq(s(asyncBucket.name()))))

    find[CBIndex](query)
  }

  def dropIndex(indexName: String): Future[OpsResult] = {
    toFuture(asyncBucket.query(simple(Index.dropIndex(asyncBucket.name(), indexName)))) map {
      result =>
        OpsResult(isSuccess = result.parseSuccess()) //TODO replace to use finalSuccess
    }

  }

}
