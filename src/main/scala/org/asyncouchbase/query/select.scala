package org.asyncouchbase.query

import com.couchbase.client.java.query.N1qlQuery
import org.asyncouchbase.util.Reflection
import org.joda.time.DateTime

import scala.reflect.runtime.universe._

sealed trait WhereExpression
sealed trait BinaryExpression extends WhereExpression


class DateRange(firstValue: DateTime) extends Range[DateTime] {
  override def toString: String = s"STR_TO_MILLIS('$firstValue') AND STR_TO_MILLIS('$secondValue')"
}

class IntRange(firstValue: Int) extends Range[Int] {
  override def toString: String = s"$firstValue AND $secondValue"
}

trait Range[T] {

  var secondValue: T = _
  def AND (secondValue: T) = {
    this.secondValue = secondValue
    this
  }

}

class RangeExpression[T](fieldName: String) extends BinaryExpression {

  def name = fieldName
  private var range : Option[Range[T]]= None

  def BETWEEN(range: Range[T]) = {
    this.range = Some(range)
    this
  }

  override def toString: String = s"$fieldName BETWEEN ${range.getOrElse("")}"
}

class INExpression[T](value: String) extends BinaryExpression {

  def fieldValue = value
  private var fieldName = ""

  def IN(fieldName: String) = {
    this.fieldName = fieldName
    this
  }

  override def toString: String = s"$fieldValue IN $fieldName"
}

 trait Expression[T] extends BinaryExpression {

  private var operator = "="
  def _operator = operator

  var value: T

  def ===(value: T) = {
    this.value = value
    this
  }

  def gt(value: T) = {
    this.value = value
    this.operator = ">"
    this
  }

  def lt(value: T) = {
    this.value = value
    this.operator = "<"
    this
  }

  def gte(value: T) = {
    this.value = value
    this.operator = "<="
    this
  }

  def lte(value: T) = {
    this.value = value
    this.operator = ">="
    this
  }



}

class ExpressionTree(rightExpression: WhereExpression) extends WhereExpression {

  def expression = Some(rightExpression)
  def _leftExpression = leftExpression
  def _operator = operator

  private var leftExpression: Option[WhereExpression] = None
  private var operator = "AND"

  def AND(expression: WhereExpression): ExpressionTree = {

    leftExpression match {
      case None => {
        leftExpression = Some(expression)
        this
      }
      case Some(ex) => {
        new ExpressionTree(this) AND expression
      }
    }
  }

  def OR(expression: WhereExpression): ExpressionTree = {

    leftExpression match {
      case None => {
        leftExpression = Some(expression)
        this.operator = "OR"
        this
      }
      case Some(ex) => {
        new ExpressionTree(this) OR expression
      }
    }
  }

  override def toString: String = s"${expression} $operator ${leftExpression}"
}

trait ArrayExpression extends WhereExpression


case class ANY(fieldName: String) extends ArrayExpression {

  private var arrayName = ""
  private var condition: Option[INExpression[String]] = None


  def IN(arrayName: String) = {
    this.arrayName = arrayName
    this
  }

  def SATISFIES(condition: INExpression[String]) = {
    this.condition = Some(condition)
    this
  }

  def END() ={
    this
  }

  override def toString: String = s"ANY $fieldName IN '$arrayName' SATISFIES ${condition.getOrElse(throw new IllegalArgumentException("SATISFIES requires a IN expression!"))} END"
}

class DateExpression(fieldName: String) extends Expression[DateTime] {

  override var value: DateTime = _

  override def toString: String = s"${fieldName} ${_operator} STR_TO_MILLIS('$value')"
}

class StringExpression(fieldName: String) extends Expression[String] {

  override var value: String = _

  override def toString: String = s"${fieldName} ${_operator} '$value'"
}

class IntExpression(fieldName: String) extends Expression[Int] {//TODO define other types

  override var value: Int = _

  override def toString: String = s"${fieldName} ${_operator} $value"


}

class BooleanExpression (fieldName: String) extends Expression[Boolean] {//TODO define other types

  override var value: Boolean = _

  override def toString: String = s"${fieldName} ${_operator} $value"


}


object Expression {
  implicit def toExpression(fieldName: String) = new StringExpression(fieldName)
  implicit def toBooleanExpression(fieldName: String) = new BooleanExpression(fieldName)
  implicit def toDateExpression(fieldName: String) = new DateExpression(fieldName)
  implicit def toNumberExpression(fieldName: String) = new IntExpression(fieldName)
  implicit def toINExpression(fieldValue: String) = new INExpression[String](fieldValue)
  implicit def toDateRangeExpression(fieldValue: String) = new RangeExpression[DateTime](fieldValue)
  implicit def toIntRangeExpression(fieldValue: String) = new RangeExpression[Int](fieldValue)
  implicit def toDateRange(fieldValue: DateTime) = new DateRange(fieldValue)
  implicit def toIntRange(fieldValue: Int) = new IntRange(fieldValue)
  implicit def toExpressionTree(expression: Expression[String]) = new ExpressionTree(expression)

}

sealed trait Query

abstract class AbstractQuery extends Query {

  var selector: String

  def validateSelector[T: TypeTag] = {
    selector match {
      case "*" =>
      case _ => {
        val fieldsInEntity = Reflection.getListFields[T]
        selector.split(",").foreach(name => {
          if(!fieldsInEntity.contains(name.trim))
            throw new IllegalArgumentException(s"the Query selector is not valid. A specified field [$name] would not be returned")
        })
      }
    }
  }

}



object SELECT extends AbstractQuery {

  def apply(selector: String ) = {

    this.selector = selector match {
      case "*" => "*"
      case _ => selector.replace("id", "meta().id")
    }
    new SimpleQuery(ss = this.selector)
  }

  override var selector: String = "*"
}


class SimpleQuery(validationOn : Boolean = true, ss: String = "*") extends AbstractQuery {




  private var bucketName = ""
  private var expression: Option[WhereExpression] = None

  def _bucketName = bucketName

  def FROM(tableName: String) = {
    this.bucketName = tableName
    this
  }


  def WHERE(expression: WhereExpression) = {
    this.expression = Some(expression)
    this
  }

  private def buildWhereClause(expression: Option[WhereExpression]): String = {
      expression.get match {
        case ex: ANY => ex.toString
        case ex: BinaryExpression => ex.toString
        case ex: ExpressionTree => s"(${buildWhereClause(ex.expression)} ${ex._operator} ${buildWhereClause(ex._leftExpression)})"
      }
  }

  def buildQuery: N1qlQuery = {

    val whereExp = expression match {
      case None => ""
      case _ => s" WHERE ${buildWhereClause(expression)}"
    }

    def adjustSelector = selector match {
      case "*" => s"$bucketName.*,meta().id"
      case _=> selector
    }

    val statement = s"SELECT $adjustSelector FROM $bucketName${whereExp}"
    N1qlQuery.simple(statement)

  }

  override def toString: String = buildQuery.statement().toString

  override var selector: String = ss
}