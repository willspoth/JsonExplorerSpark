package util

import Explorer.{JE_Array, JE_Boolean, JE_Empty_Array, JE_Empty_Object, JE_Null, JE_Numeric, JE_Obj_Array, JE_Object, JE_String, JE_Tuple, JE_Var_Object, JsonExplorerType}
import util.JsonSchema._

object JsonExplorerTypeToJsonSchemaType {
  def convert(t: JsonExplorerType): JsonSchemaType = {
    t  match {
      case JE_String => Str
      case JE_Numeric => Num
      case JE_Boolean => Bool
      case JE_Null => Null
      case JE_Object => Obj
      case _:JE_Object => Obj
      case JE_Array => Arr
      case _:JE_Array => Arr
      case JE_Empty_Object => Obj
      case JE_Empty_Array => Arr
      case JE_Var_Object => Obj
      case JE_Var_Object(xs) => Obj
      case JE_Obj_Array => Arr
      case JE_Obj_Array(xs) => Arr
    }
  }
}
