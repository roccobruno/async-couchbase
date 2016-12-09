package org.asyncouchbase.bucket

import com.couchbase.client.core.logging.CouchbaseLoggerFactory
import com.couchbase.client.java.document.JsonDocument.create
import com.couchbase.client.java.document.json.JsonObject.fromJson
import com.couchbase.client.java.query._
import com.couchbase.client.java.{AsyncBucket, PersistTo, ReplicateTo}
import org.asyncouchbase.model.OpsResult
import org.asyncouchbase.query.SimpleQuery
import org.asyncouchbase.util.Converters._
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.Json.parse
import play.api.libs.json._
import rx.lang.scala.JavaConversions.toScalaObservable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait BucketApi {
 val logger =  CouchbaseLoggerFactory.getInstance(classOf[BucketApi])

   def asyncBucket: AsyncBucket


  def upsert[T](key: String, entity: T, persistTo: PersistTo = PersistTo.ONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit r: Writes[T]): Future[OpsResult] = {
    val ent = fromJson(r.writes(entity).toString())
    toFuture(asyncBucket.upsert(create(key, ent), persistTo, replicateTo)) map {
      _ =>
        OpsResult(isSuccess = true, "")
    } recover {
      case ex: Throwable => {
        logger.warn(s"Error in upserting document with id= $key. Error message - ${ex.getMessage}")
        logger.debug(s"Error of type ${ex.getCause}")
        OpsResult(isSuccess = false, ex.getMessage)
      }
    }
  }

  def delete[T](key: String, persistTo: PersistTo = PersistTo.ONE, replicateTo: ReplicateTo = ReplicateTo.NONE): Future[OpsResult] = {
    toFuture(asyncBucket.remove(key, persistTo, replicateTo)) map {
      _ => OpsResult(isSuccess = true, "")
    } recover {
      case ex: Throwable => {
        logger.warn(s"Error in deleting document with id= $key. Error message - ${ex.getMessage}")
        logger.debug(s"Error of type ${ex.getCause}")
        OpsResult(isSuccess = false, ex.getMessage) //TODO catch the right exception and transform
      }
    }
  }

  trait Metadata

  case class MetadataRecord(cas: String, id: String, flags: String) extends Metadata

  object MetadataRecord {

    implicit val format = Json.format[MetadataRecord]

  }





  def find[T](query: SimpleQuery[T])(implicit r: Reads[T]): Future[List[T]] = {

    //{"$1":{"flags":33554432,"id":"u:rocco1","cas":1480962217402761200,"type":"json"},"default":{"name":"rocco","interests":["test"],"dob":1480962217391,"email":"eocco@test.com"}}

    def convert(row: AsyncN1qlQueryResult) = {
      observable2Enumerator[AsyncN1qlQueryRow](row.rows()) run Iteratee.fold(List.empty[AsyncN1qlQueryRow]) { (l, e) => e :: l } map {
        _.reverse
      } map {
        s => s map (ss => r.reads(parse(ss.value().toString)).get)
      }
    }

    val buildQuery: N1qlQuery = query.buildQuery
    val executeQuery = for {
      observable <- toFuture(asyncBucket.query(buildQuery))
      results <- convert(observable)
    } yield results

    executeQuery recover {
      case ex: Throwable => logger.error(s"ERROR IN FIND method query ${buildQuery.n1ql()} - err ${ex.getMessage}")
    }

    executeQuery
  }


  def get[T](key: String)(implicit r: Reads[T]): Future[Option[T]] =
    toFuture(asyncBucket.get(key)) map (doc => parse(doc.content().toString).asOpt[T]) recover {
      case _: Throwable => None //TODO
    }


  def getValue[V](key: String, fieldName: String, valueType: Class[V]): Future[Option[V]] = {
    toFuture(asyncBucket.mapGet(key,fieldName,valueType)) map {
      value => Some(value)
    } recover {
      case _: Throwable => None //TODO logging
    }
  }

  def setValue[V](key: String, fieldName: String, fieldValue: V): Future[OpsResult] = {

    toFuture(asyncBucket.mapAdd(key, fieldName, fieldValue)) map {
      _ => OpsResult(isSuccess = true, "")
    } recover {
      case ex: Throwable => {
        logger.warn(s"Error in setting value in document with id= $key. Error message - ${ex.getMessage}")
        logger.debug(s"Error of type ${ex.getCause}")
        OpsResult(isSuccess = false, ex.getMessage)
      }
    }

  }
}
