package org.asyncouchbase.util

import scala.reflect.runtime.universe._

object Reflection {

  def getMethods[T: TypeTag] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList

  //TODO to cache
  def getListFields[T: TypeTag] = {
    getMethods[T].map(vv => vv.name).reverse.
      mkString(",").replace("id", "meta().id")
  }

}
