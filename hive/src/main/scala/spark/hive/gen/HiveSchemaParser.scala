package spark.hive.gen

import scala.util.parsing.combinator._
import scala.io.Source
import java.io._

object HiveSchemaParser extends RegexParsers {
  def name: Parser[String] = """[a-zA-Z0-9_]+""".r

  def schemaField: Parser[Field] = name ~ hiveType ^^ {
    case name ~ type_ => Field(name, type_)
  }

  def schemaFields: Parser[List[Field]] = rep(schemaField)

  def hiveType: Parser[Type] = atom | array | map | struct | failure("type expected")

  def atom: Parser[Type] =
    ("int" | "tinyint" | "bigint" | "smallint" | "double" | "string") ^^ Map(
      "int" -> Atom("Int"),
      "tinyint" -> Atom("Int"),
      "smallint" -> Atom("Int"),
      "bigint" -> Atom("Long"),
      "string" -> Atom("String"),
      "double" -> Atom("Double")
    )

  def array: Parser[Type] = "array" ~ "<" ~ hiveType ~ ">" ^^ {
    case "array" ~ "<" ~ elem ~ ">" => ArrayType(elem)
  }

  def map: Parser[Type] = "map" ~ "<" ~ hiveType ~ "," ~ hiveType ~ ">" ^^ {
    case "map" ~ "<" ~ key ~ "," ~ value ~ ">" => MapType(key, value)
  }

  def struct: Parser[Type] = "struct" ~ "<" ~ repsep(structField, ",") ~ ">" ^^ {
    case "struct" ~ "<" ~ fields ~ ">" => Struct(fields)
  }

  def structField: Parser[Field] = name ~ ":" ~ hiveType ^^ {
    case name ~ ":" ~ type_ => Field(name, type_)
  }

  def parseSchemaFields(in: Reader): ParseResult[List[Field]] = {
    parseAll(schemaFields, in)
  }
}











