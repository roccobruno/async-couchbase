package org.asyncouchbase.example

import org.joda.time.DateTime
import play.api.libs.json.Json

case class User(name: String, email: String, interests: Seq[String], dob: DateTime = DateTime.now(), id: Option[String] = None)

object User {
  implicit def format = Json.format[User]
}