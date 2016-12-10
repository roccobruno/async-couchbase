package org.asyncouchbase.util

import java.util.concurrent.ConcurrentHashMap

import scala.reflect.runtime.universe._

object Reflection {

  private val mapping = new ConcurrentHashMap[String, String]()

  def getMethods[T: TypeTag] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList


  private def processPrivateFields(fieldName: String) =
    fieldName.split("\\$")(0)

  //TODO to cache
  def getListFields[T: TypeTag]: String = {
   val func = new java.util.function.Function[String,String] () {
     override def apply(t: String): String  = getMethods[T].map(vv => processPrivateFields(vv.name.toString)).reverse.
       mkString(",").replace("id", "meta().id")
   }

    mapping.computeIfAbsent(typeOf[T].toString, func)
  }

}
