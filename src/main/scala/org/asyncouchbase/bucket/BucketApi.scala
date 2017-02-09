package org.asyncouchbase.bucket

import com.couchbase.client.core.logging.CouchbaseLoggerFactory
import com.couchbase.client.java.document.JsonDocument.create
import com.couchbase.client.java.document.json.JsonObject.fromJson
import com.couchbase.client.java.error.DocumentAlreadyExistsException
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.subdoc.AsyncMutateInBuilder
import com.couchbase.client.java.{AsyncBucket, PersistTo, ReplicateTo}
import org.asyncouchbase.model.OpsResult
import org.asyncouchbase.query.SimpleQuery
import org.asyncouchbase.util.Converters._
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.Json._
import play.api.libs.json._
import rx.lang.scala.JavaConversions.toScalaObservable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.universe._


case class Count(count: Int)
object  Count {
  implicit val format = Json.format[Count]
}

case class Path(path: String) {

  def validPath = {
    val points = path.count( _ == '.')
    !path.contains('.') || path.split('.').size == points + 1
  }
  require(validPath,"the supplied path is invalid")
}


trait BucketApi {
 val logger =  CouchbaseLoggerFactory.getInstance(classOf[BucketApi])

  implicit val validateQuery = false

  def asyncBucket: AsyncBucket


  def errorHandling(operation: String, key: String): PartialFunction[Throwable, OpsResult] = {
    case ex: Throwable => {
      logger.warn(s"Error in $operation document with id= $key. Error message - ${ex.getMessage}")
      logger.debug(s"Error of type ${ex.getCause}")
      OpsResult(isSuccess = false, ex.getMessage)
    }
  }

  def handlingDocExistException(key : String): PartialFunction[DocumentAlreadyExistsException, OpsResult] = {
    case ex: DocumentAlreadyExistsException =>
      logger.warn(s"Error in inserting document with id= $key. Error message - ${ex.getMessage}")
      logger.debug(s"Error of type ${ex.getCause}")
      OpsResult(isSuccess = false, ex.getMessage)
  }

  def insert[T](key: String, entity: T, expiry: Int = 0, persistTo: PersistTo = PersistTo.ONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit r: Writes[T]): Future[OpsResult] = {
    val ent = fromJson(r.writes(entity).toString())
    toFuture(asyncBucket.insert(create(key, expiry, ent), persistTo, replicateTo)) map {
      _ =>
        OpsResult(isSuccess = true)
    }
  }

  def upsert[T](key: String, entity: T, expiry: Int = 0, persistTo: PersistTo = PersistTo.ONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit r: Writes[T]): Future[OpsResult] = {
    val ent = fromJson(r.writes(entity).toString())
    toFuture(asyncBucket.upsert(create(key, expiry, ent), persistTo, replicateTo)) map {
      _ =>
        OpsResult(isSuccess = true)
    } recover errorHandling("upserting", key)
  }

  def delete[T](key: String, persistTo: PersistTo = PersistTo.ONE, replicateTo: ReplicateTo = ReplicateTo.NONE): Future[OpsResult] = {
    toFuture(asyncBucket.remove(key, persistTo, replicateTo)) map {
      _ => OpsResult(isSuccess = true)
    } recover errorHandling("deleting", key)
  }

  def count(query: SimpleQuery, consistency: ScanConsistency = ScanConsistency.STATEMENT_PLUS) : Future[Count] = {
    find[Count](query, consistency) map {
      case head:: tail => head
    } recover {
      case ex: Throwable =>
        logger.error(s"ERROR IN COUNT method query ${query.buildQuery.n1ql()} - err ${ex.getMessage}")
        Count(0)

    }
  }

  def find[T: TypeTag](query: SimpleQuery, consistency: ScanConsistency = ScanConsistency.STATEMENT_PLUS)(implicit r: Reads[T], validateQuery: Boolean): Future[List[T]] = {

    def convert(row: AsyncN1qlQueryResult) = {
      observable2Enumerator[AsyncN1qlQueryRow](row.rows()) run Iteratee.fold(List.empty[AsyncN1qlQueryRow]) { (l, e) => e :: l } map {
        _.reverse
      } map {
        s => s map (ss => r.reads(parse(ss.value().toString)).get)
      }
    }

    if(validateQuery)
    query.validateSelector[T]

    val buildQuery: N1qlQuery = query.buildQuery
    buildQuery.params().consistency(consistency)
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
      _ => OpsResult(isSuccess = true)
    } recover errorHandling("setting value", key)
  }

  def removeValue(key: String, fieldName: String): Future[OpsResult] = {
    toFuture(asyncBucket.mapRemove(key, fieldName)) map {
      _ => OpsResult(isSuccess = true)
    } recover errorHandling("setting value", key)
  }

  def setValuesOnExistingDoc(key: String, values: Map[Path, Any], createParent: Boolean = false, persistTo: PersistTo = PersistTo.ONE, replicateTo: ReplicateTo = ReplicateTo.NONE): Future[OpsResult] = {
    val builder: AsyncMutateInBuilder = asyncBucket.mutateIn(key)
    setValuesWithDef[Any](key, values, createParent, builder.insert, builder, persistTo, replicateTo)
  }

  def setValues(key: String, values: Map[Path, Any], createParent: Boolean = false, persistTo: PersistTo = PersistTo.ONE, replicateTo: ReplicateTo = ReplicateTo.NONE): Future[OpsResult] = {
    val builder: AsyncMutateInBuilder = asyncBucket.mutateIn(key)
    setValuesWithDef[Any](key, values, createParent, builder.upsert, builder, persistTo, replicateTo)
  }

  private def setValuesWithDef[T](key: String, values: Map[Path, T], createParent: Boolean = false,
                               f: (String,T,Boolean) => AsyncMutateInBuilder , builder: AsyncMutateInBuilder, persistTo: PersistTo , replicateTo: ReplicateTo ): Future[OpsResult] = {

    builder.withDurability(persistTo, replicateTo)

    def addToBuilder(valueToAdd :(Path,T)):Unit = {
      f(valueToAdd._1.path,valueToAdd._2,createParent)
    }

    values.foreach(addToBuilder)

    toFuture(builder.execute()) map {
      _ => OpsResult(isSuccess = true)
    } recover errorHandling("setting values", key)
  }


  def exist[T](key: String): Future[Boolean] = {

    val res: Future[java.lang.Boolean] = toFuture[java.lang.Boolean](asyncBucket exists key) recover {
      case ex: Throwable => {
        logger.warn(s"Error in setting value in document with id= $key. Error message - ${ex.getMessage}")
        logger.debug(s"Error of type ${ex.getCause}")
        false
      }
    }
    res map {
      case java.lang.Boolean.TRUE => true
      case _ => false
    }
  }


  def getAndTouch[T](key: String, expiry: Int)(implicit r: Reads[T]): Future[Option[T]] = {
    toFuture(asyncBucket.getAndTouch(key, expiry)) map {
      doc => parse(doc.content().toString).asOpt[T]
    } recover {
      case _: Throwable => None //TODO
    }
  }
}
