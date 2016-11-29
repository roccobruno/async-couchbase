package org.asyncouchbase.bucket

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query._
import com.couchbase.client.java.{AsyncBucket, PersistTo, ReplicateTo}
import org.asyncouchbase.util.Converters.{observable2Enumerator, toFuture}
import org.asyncouchbase.model.{CBIndex, OpsResult}
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.{JsResult, Json, Reads, Writes}
import rx.lang.scala.JavaConversions.toScalaObservable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait BucketApi {


  def asyncBucket: AsyncBucket



  def upsert[T](key: String, entity: T, persistTo: PersistTo = PersistTo.MASTER, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit r: Writes[T]): Future[OpsResult] = {
    val ent = JsonObject.fromJson(r.writes(entity).toString())
    toFuture(asyncBucket.upsert(JsonDocument.create(key, ent))) map {
      _ =>
        OpsResult(isSuccess = true, "")
    } recover {
      case ex: Throwable => OpsResult(isSuccess = false, ex.getMessage) //TODO catch the right exception and transform
    }
  }

  def delete[T](key: String): Future[OpsResult] = {
    toFuture(asyncBucket.remove(key)) map {
      _ => OpsResult(isSuccess = true, "")
    } recover {
      case ex: Throwable => OpsResult(isSuccess = false, ex.getMessage)
    }
  }


  def find[T](query: N1qlQuery)(implicit r: Reads[T]): Future[List[T]] = {

    def convert(row: AsyncN1qlQueryResult) = {
      observable2Enumerator[AsyncN1qlQueryRow](row.rows()) run Iteratee.fold(List.empty[AsyncN1qlQueryRow]) { (l, e) => e :: l } map {
        _.reverse
      } map {
        s => s map (ss => r.reads(Json.parse(ss.value().toString)).get)
      }
    }

    for {
      observable <- toFuture(asyncBucket.query(query))
      results <- convert(observable)
    } yield results

  }


  def get[T](key: String)(implicit r: Reads[T]): Future[Option[T]] =
    toFuture(asyncBucket.get(key)) map (doc => Json.parse(doc.content().toString).asOpt[T]) recover {
      case _: Throwable => None //TODO
    }
}
