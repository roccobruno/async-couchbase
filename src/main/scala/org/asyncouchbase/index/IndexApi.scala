package org.asyncouchbase.index

import com.couchbase.client.java.query.N1qlQuery._
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.path.index.IndexType
import org.asyncouchbase.bucket.BucketApi
import org.asyncouchbase.model.{CBIndex, OpsResult}
import org.asyncouchbase.query.ExpressionImplicits._
import org.asyncouchbase.query.SELECT
import org.asyncouchbase.util.Converters.toFuture
import play.api.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait IndexApi extends BucketApi {
  val INDEX_NAME_PREFIX = "def_"
  val PRIMARY_INDEX_NAME = "#primary"





  def executeOp(observable: rx.Observable[AsyncN1qlQueryResult]): Future[OpsResult] = {
    toFuture(observable) map {
      result =>
        OpsResult(isSuccess = result.parseSuccess()) //TODO replace to use finalSuccess
    }
  }


  def createIndex[T](fields: Seq[String], deferBuild: Boolean = true): Future[OpsResult] = {

    var indexName = INDEX_NAME_PREFIX

    fields.foreach(field =>
      indexName = s"${indexName}${field}_"
    )

    val indName = indexName.dropRight(1)
    val stat = Index.createIndex(indName).on(asyncBucket.name(), x(indName.replace(INDEX_NAME_PREFIX, ""))).using(IndexType.GSI)

    def valuateDeferBuild = deferBuild match  {
      case false => stat
      case _ => stat.withDefer()
    }

    executeOp(asyncBucket.query(simple(valuateDeferBuild)))
  }

  def buildIndex[T](fields: Seq[String]): Future[OpsResult] = {
    var indexName = INDEX_NAME_PREFIX

    fields.foreach(field =>
      indexName = s"${indexName}${field}_"
    )

    val indName = indexName.dropRight(1)
    val stat = Index.buildIndex().on(asyncBucket.name()).indexes(indName).using(IndexType.GSI)
    executeOp(asyncBucket.query(simple(stat)))
  }


  def buildPrimaryIndex(): Future[OpsResult] = {
    val stat = Index.buildIndex().on(asyncBucket.name()).indexes(PRIMARY_INDEX_NAME).using(IndexType.GSI)
    executeOp(asyncBucket.query(simple(stat)))
  }


  def findIndexes(): Future[List[CBIndex]] = {
    val query =  SELECT ("indexes.*") FROM "system:indexes" WHERE ("keyspace_id" === asyncBucket.name())
    find[CBIndex](query)
  }

  def dropIndex(indexName: String): Future[OpsResult] = {
    val query: SimpleN1qlQuery = simple(Index.dropIndex(asyncBucket.name(), indexName))
    executeOp(asyncBucket.query(query))
  }

  def createPrimaryIndex(deferBuild: Boolean = false): Future[OpsResult] = {
    val query = Index.createPrimaryIndex().on(asyncBucket.name()).using(IndexType.GSI)

    def valuateDeferBuild = deferBuild match  {
      case false => query
      case _ => query.withDefer()
    }

    executeOp(asyncBucket.query(valuateDeferBuild))
  }

  def dropAllIndexes(): Future[OpsResult] = {

    Logger.info("dropping all indexes")
    def deleteIndexes(indexes: Seq[CBIndex]): Future[OpsResult] = {

      indexes.map { index =>
        dropIndex(index.name)
      }.head
    }

    for{

      indexes <- findIndexes()
      if(indexes.nonEmpty)
      res <- deleteIndexes(indexes)
      if(indexes.isEmpty)
      res <- Future.successful(OpsResult(true))
    } yield res
  }

}
