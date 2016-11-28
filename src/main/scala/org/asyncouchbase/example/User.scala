package org.asyncouchbase.example

import play.api.libs.json.Json

case class User(name: String, email: String, interests: Seq[String])

object User {
  implicit def format = Json.format[User]
}