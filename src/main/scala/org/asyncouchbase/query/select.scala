package org.asyncouchbase.query

import com.couchbase.client.java.query.N1qlQuery
import org.asyncouchbase.util.Reflection

sealed trait WhereExpression
sealed trait BinaryExpression extends WhereExpression
import scala.reflect.runtime.universe._


class Range(firstValue: String) {

  def _firstValue = firstValue
  var secondValue = ""
  def AND (secondValue: String) = {
    this.secondValue = secondValue
    this
  }

  override def toString: String = s"STR_TO_MILLIS('$firstValue') AND STR_TO_MILLIS('$secondValue')"
}

class RangeExpression(fieldName: String) extends BinaryExpression {

  def name = fieldName
  private var range : Option[Range]= None

  def BETWEEN(range: Range) = {
    this.range = Some(range)
    this
  }

  override def toString: String = s"$fieldName BETWEEN ${range.getOrElse("")}"
}

class INExpression(value: String) extends BinaryExpression {

  def fieldValue = value
  private var fieldName = ""

  def IN(fieldName: String) = {
    this.fieldName = fieldName
    this
  }

  override def toString: String = s"'$fieldValue' IN $fieldName"
}

class Expression(fieldName: String) extends BinaryExpression {

  def name = fieldName

  private var value = ""

  def ===(value: String) = {
    this.value = value
    this
  }


  override def toString: String = s"$name = '$value'"
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


object Expression {
  implicit def toExpression(fieldName: String) = new Expression(fieldName)
  implicit def toINExpression(fieldValue: String) = new INExpression(fieldValue)
  implicit def toRangeExpression(fieldValue: String) = new RangeExpression(fieldValue)
  implicit def toRange(fieldValue: String) = new Range(fieldValue)
  implicit def toExpressionTree(expression: Expression) = new ExpressionTree(expression)
}

sealed trait Query

abstract class AbstractQuery[T: TypeTag] extends Query {

  protected var selector: String = "*"

  protected def validateSelector = {
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




class SimpleQuery[T: TypeTag](validationOn : Boolean = true) extends AbstractQuery[T] {


  private var bucketName = ""
  private var expression: Option[WhereExpression] = None

  def _bucketName = bucketName

  def SELECT(selector: String) = {

    this.selector =   selector match {
      case "*" => Reflection.getListFields[T]
      case _ => selector.replace("id","meta().id")
    }

    if(validationOn) validateSelector

    this
  }

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
        case ex: BinaryExpression => ex.toString
        case ex: ExpressionTree => s"(${buildWhereClause(ex.expression)} ${ex._operator} ${buildWhereClause(ex._leftExpression)})"
      }
  }

  def buildQuery: N1qlQuery = {

    val whereExp = expression match {
      case None => ""
      case _ => s" WHERE ${buildWhereClause(expression)}"
    }

    val statement = s"SELECT $selector FROM $bucketName${whereExp}"
    N1qlQuery.simple(statement)

  }

}