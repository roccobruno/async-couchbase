package org.asyncouchbase.example

import java.util.UUID

import org.joda.time.DateTime
import play.api.libs.json.Json


trait InternalId {
  def getId: String
}
case class User(name: String, email: String, interests: Seq[String], dob: DateTime = DateTime.now(), id: Option[String] = None)

object User {
  implicit def format = Json.format[User]
}

case class Token(private val id: Option[String] = Some(UUID.randomUUID().toString),
                 token : String,
                 accountId: String ,
                 docType: String = "Token",
                 lastTimeUpdate: DateTime = DateTime.now())  {
//  def getId = this.id.getOrElse(throw new IllegalStateException("found an Token without an ID!!!"))
}