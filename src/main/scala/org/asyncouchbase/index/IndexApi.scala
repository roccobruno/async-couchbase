package org.asyncouchbase.index

import com.couchbase.client.java.query.N1qlQuery._
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.{Index, N1qlQuery, Select}
import com.couchbase.client.java.query.dsl.Expression._
import org.asyncouchbase.util.Converters.toFuture
import org.asyncouchbase.bucket.BucketApi
import org.asyncouchbase.model.{CBIndex, OpsResult}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait IndexApi extends BucketApi {

  val INDEX_NAME_PREFIX = "def_"

  def createIndex[T](fields: Seq[String], deferBuild: Boolean = true): Future[OpsResult] = {

    var indexName = INDEX_NAME_PREFIX

    fields.foreach(field =>
      indexName = s"${indexName}${field}_"
    )

    val indName = indexName.dropRight(1)
    val stat = Index.createIndex(indName).on(asyncBucket.name(), x(indName.replace(INDEX_NAME_PREFIX, "")))

    def valuateDeferBuild = deferBuild match  {
      case false => stat
      case _ => stat.withDefer()
    }

    toFuture(asyncBucket.query(simple(valuateDeferBuild))) map {
      result =>
        OpsResult(isSuccess = result.parseSuccess()) //TODO replace to use finalSuccess
    }
  }

  def buildIndex[T](fields: Seq[String]): Future[OpsResult] = {
    var indexName = INDEX_NAME_PREFIX

    fields.foreach(field =>
      indexName = s"${indexName}${field}_"
    )

    val indName = indexName.dropRight(1)
    val stat = Index.buildIndex().on(asyncBucket.name()).indexes(indName)
    toFuture(asyncBucket.query(simple(stat))) map {
      result =>
        OpsResult(isSuccess = result.parseSuccess()) //TODO replace to use finalSuccess
    }
  }


  def buildPrimaryIndex(): Future[OpsResult] = {
    val indexName = "#primary"
    val stat = Index.buildIndex().on(asyncBucket.name()).indexes(indexName)
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

  def createPrimaryIndex(deferBuild: Boolean = true): Future[OpsResult] = {
    val query = Index.createPrimaryIndex().on(asyncBucket.name())

    def valuateDeferBuild = deferBuild match  {
      case false => query
      case _ => query.withDefer()
    }

    toFuture(asyncBucket.query(valuateDeferBuild)) map {
      result =>
        OpsResult(isSuccess = result.parseSuccess()) //TODO replace to use finalSuccess
    }
  }

}
