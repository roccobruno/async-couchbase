package org.asyncouchbase

import play.api.libs.json._
import play.api.libs.functional.syntax._

package object model {

  case class OpsResult(isSuccess: Boolean, msg: String = "")

  case class CBIndex(name: String, isPrimary: Boolean, keys: Seq[String], id: String, state: String)
  object CBIndex {


   implicit val reads: Reads[CBIndex] =  (
        (__ \ "index_key").read[Seq[String]] and
        (__ \ "keyspace_id").read[String] and
        (__ \ "is_primary").readNullable[Boolean] and
        (__ \ "name").read[String] and
        (__ \ "id").read[String] and
        (__ \ "state").read[String]) {(
          indexKey: Seq[String], keySpaceId: String, isPrimary: Option[Boolean], name: String, id:String, state:String
        ) => CBIndex(name, isPrimary.getOrElse(false), indexKey, id, state)}


  }

}
