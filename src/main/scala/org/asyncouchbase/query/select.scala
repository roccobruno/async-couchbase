package org.asyncouchbase.query

import com.couchbase.client.java.query.N1qlQuery


trait WhereExpression
trait BinaryExpression extends WhereExpression



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

class Query {

  private var selector: String = "*"
  private var bucketName = ""
  private var expression: Option[WhereExpression] = None

  def SELECT(selector: String) = {
    this.selector = selector
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