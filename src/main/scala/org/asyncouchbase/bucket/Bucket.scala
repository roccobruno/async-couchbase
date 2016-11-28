package org.asyncouchbase.bucket

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{AsyncN1qlQueryResult, AsyncN1qlQueryRow, N1qlQuery}
import com.couchbase.client.java.{AsyncBucket, PersistTo, ReplicateTo}
import org.asyncouchbase.Converters
import org.asyncouchbase.Converters.observable2Enumerator
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.{Json, Reads, Writes}
import play.libs.concurrent.Futures
import rx.lang.scala.JavaConversions.toScalaObservable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait Bucket {

  def asyncBucket: AsyncBucket


  private[bucket] def gget[T](key: String)(bucket: AsyncBucket)(implicit r: Reads[T]): Future[Option[T]] = {



    toScalaObservable(bucket.get(key)).toBlocking.toFuture map {
      jsonDocument =>
        Json.parse(jsonDocument.content().toString).asOpt[T]
    }
  }

  def get[T](key: String)(implicit r: Reads[T]): Future[Option[T]] = gget[T](key)(asyncBucket)(r)

  def upsert[T](key: String, entity: T, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit r: Writes[T]): Future[OpsResult] = {
    val ent = JsonObject.fromJson(r.writes(entity).toString())
    toScalaObservable(asyncBucket.upsert(JsonDocument.create(key, ent))).toBlocking.toFuture map {
      _ =>
        OpsResult(true,"")
    } recover {
      case ex: Throwable => OpsResult(false, ex.getMessage)
    }
  }


  def find[T](query: N1qlQuery)(implicit r: Reads[T]): Future[List[T]] = {

    def convert(row : AsyncN1qlQueryResult) = {
      observable2Enumerator[AsyncN1qlQueryRow](row.rows()) run Iteratee.fold(List.empty[AsyncN1qlQueryRow]) { (l, e) => e :: l } map {
        _.reverse
      } map {
        s =>
          s map {
            ss =>
              r.reads(Json.parse(ss.value().toString)).get
          }
      }
    }

    val toFuture: Future[AsyncN1qlQueryResult] = toScalaObservable(asyncBucket.query(query)).toBlocking.toFuture

    for {
      obervable <- toFuture
      results <- convert(obervable)

    } yield results

  }


  case class OpsResult(isSuccess: Boolean, msg: String)

}
