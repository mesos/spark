package spark.hive.gen

// Hive data types
sealed trait Type
case class Atom(scalaType: String) extends Type
case class ArrayType(elemType: Type) extends Type
case class MapType(keyType: Type, valueType: Type) extends Type
case class Struct(fields: List[Field]) extends Type






