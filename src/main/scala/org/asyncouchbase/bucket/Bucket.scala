package org.asyncouchbase.bucket

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.{AsyncBucket, PersistTo, ReplicateTo}
import play.api.libs.json.{Json, Reads, Writes}
import rx.lang.scala.JavaConversions
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
    toScalaObservable(asyncBucket.upsert(JsonDocument.create(key, ent))).toBlocking.toFuture
  }


  case class OpsResult(isSuccess: Boolean, msg: String)

}
