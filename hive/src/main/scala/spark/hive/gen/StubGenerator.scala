package spark.hive.gen

import scala.util.parsing.combinator._
import scala.io.Source
import java.io._

case class Field(name: String, fieldType: Type)

sealed trait Type
case class Atom(scalaType: String) extends Type
case class ArrayType(elemType: Type) extends Type
case class MapType(keyType: Type, valueType: Type) extends Type
case class Struct(fields: List[Field]) extends Type

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

class StubGenerator {
  def run(className: String, in: Reader, whitelist: Seq[String], out: Writer) {
    HiveSchemaParser.parseSchemaFields(in) match {
      case HiveSchemaParser.Success(fields, _) =>
        out.write("import spark._\n")
        out.write("import spark.hive._\n")
        out.write("import org.apache.hadoop.io.{BytesWritable, Text}\n")
        out.write("\n")
        writeClass(out, className, fields, whitelist)
        out.flush
      case f: HiveSchemaParser.Failure =>
        System.err.println(f)
        System.exit(1)
    }
  }

  def scalaType(type_ : Type, name: String): String = type_ match {
    case Struct(_) => name.capitalize
    case Atom(t) => t
    case ArrayType(t) => "Array[" + scalaType(t, name) + "]"
    case MapType(k, v) => "Map[" + scalaType(k, name + "Key") +
                          ", " + scalaType(v, name + "Val") + "]"
  }

  def writeNestedClasses(out: Writer, name: String, type_ : Type,
                         whitelist: Seq[String], indent: String): Unit = {
    type_ match {
      case ArrayType(t) =>
        writeNestedClasses(out, name, t, whitelist, indent)
      case MapType(k, v) =>
        writeNestedClasses(out, name + "Key", k, whitelist, indent)
        writeNestedClasses(out, name + "Val", v, whitelist, indent)
      case Struct(fields) =>
        out.write("\n")
        writeClass(out, name.capitalize, fields, whitelist, indent)
      case _ =>
    }
  }

  def writeClass(out: Writer, name: String, fields: List[Field],
                 whitelist: Seq[String], indent: String = "") {
    def write(text: String, args: Any*) {
      out.write(indent + text.format(args: _*))
    }
    def writeln(text: String, args: Any*) {
      write(text, args: _*)
      write("\n")
    }

    writeln("class " + name + " (")

    // Print the fields themselves
    val fieldDefs = fields.filter(f => whitelist.contains(f.name)).map {
      case Field(name, type_) => "  val " + name + ": " + scalaType(type_, name)
    }.mkString(",\n" + indent)
    writeln(fieldDefs)

    writeln(") {}\n")

    writeln("object " + name + " {")

    // Print read method
    writeln("  def read(range: ByteRange, sep: Byte = 1): Option[%s] {", name)
    writeln("    val ranges = range.split(sep)")
    val indices = for {
      (Field(name, _), index) <- fields.zipWithIndex
      if whitelist.contains(name)
    } yield { index }
    val minSize = if (indices.size == 0) 0 else indices.max + 1
    writeln("    if (ranges.size >= %d) {", minSize)
    for ((Field(name, type_), index) <- fields.zipWithIndex if whitelist.contains(name)) {
      write("      val %s: Option[%s] = ", name, scalaType(type_, name))
      writeReadCode(out, name, type_, "ranges(" + index + ")", "sep", indent + "      ")
      writeln("")
      writeln("      if (%s == None) return None", name)
    }
    writeln("      return Some(new %s(%s))", name,
      fields.filter(f => whitelist.contains(f.name)).map(_.name + ".get").mkString(", "))
    writeln("    }")
    writeln("    return None")
    writeln("  }")

    // Print the classes for any struct fields
    for (Field(name, type_) <- fields if whitelist.contains(name)) {
      writeNestedClasses(out, name, type_, whitelist, indent + "  ")
    }

    // Print the sequenceFile method that creates an RDD with this type of element
    writeln("")
    writeln("  def sequenceFile(sc: SparkContext, path: String): RDD[%s] = {", name)
    writeln("    sc.sequenceFile[BytesWritable, Text](path).flatMap {")
    writeln("      case (key, text) => %s.read(new ByteRange(text.getBytes, text.getLength))", name)
    writeln("    }")
    writeln("  }")

    writeln("}")
  }

  // Generate read code for a given field (TODO: should really be made a write* call)
  def writeReadCode(out: Writer, name: String, type_ : Type, range: String,
                    sep: String, indent: String) {
    def write(text: String, args: Any*) {
      out.write(indent + text.format(args: _*))
    }
    def writeln(text: String, args: Any*) {
      write(text, args: _*)
      out.write("\n")
    }
    type_ match {
      case Atom(t) =>
        out.write("%s.parse%s".format(range, t))
      case Struct(t) =>
        out.write("%s.read(%s, %s + 1)".format(name.capitalize, range, sep))
      case ArrayType(t) =>
        out.write("{\n")
        writeln("  val parts = %s.split(%s + 1)", range, sep)
        write("  val objs = parts.map(part => ")
        writeReadCode(out, name, t, "part", sep + " + 1", indent + "    ")
        out.write(")\n")
        writeln("  if (objs.count(_ == None) > 0)")
        writeln("    None")
        writeln("  else")
        writeln("    Some(Array[%s](objs.flatten: _*))", scalaType(t, name))
        write("}")
      case _ => out.write("UNSUPPORTED")
    }
  }
}

object StubGenerator {
  def main(args: Array[String]) {
    val (className, schemaFile, whitelistFile, writer) = args match {
      case Array(cn, sf, wf, of) => (cn, sf, wf, new FileWriter(of))
      case Array() => {
        println("*** Using default arguments for debugging! ***\n")
        ("Session", "test.schema", "test.wl", new OutputStreamWriter(System.out))
      }
    }
    val whitelist = Source.fromFile(whitelistFile).getLines().map(_.trim).toArray
    val gen = new StubGenerator
    gen.run(className, new FileReader(schemaFile), whitelist, writer)
  }
}