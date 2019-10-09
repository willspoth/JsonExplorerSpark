package util

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.fasterxml.jackson.core.JsonToken.{END_ARRAY, END_OBJECT, FIELD_NAME, START_ARRAY, START_OBJECT, VALUE_EMBEDDED_OBJECT, VALUE_FALSE, VALUE_NULL, VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_STRING, VALUE_TRUE}


object JsonSchema {

  sealed trait JsonSchemaType {
    def add(k:String, v: JsonSchemaType): Unit = ???
  }

  case class Obj(m: scala.collection.mutable.HashMap[String,JsonSchemaType]) extends JsonSchemaType {
    override def add(k:String, v: JsonSchemaType): Unit = m.put(k,v)
  }
  case class Arr(l: scala.collection.mutable.ListBuffer[JsonSchemaType]) extends JsonSchemaType {
    override def add(k:String, v: JsonSchemaType): Unit = l.append(v)
  }

  case object Str extends JsonSchemaType {
    override def toString: String = "string"
  }
  case object Obj extends JsonSchemaType {
    override def toString: String = "object"
  }
  case object Arr extends JsonSchemaType {
    override def toString: String = "array"
  }
  case object Num extends JsonSchemaType {
    override def toString: String = "number"
  }
  case object Bool extends JsonSchemaType {
    override def toString: String = "boolean"
  }
  case object Null extends JsonSchemaType {
    override def toString: String = "null"
  }


  sealed trait JsonSchemaStructure extends Any

  case class JSA_schema(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""$$schema":"${value.toString}""""
  }
  case class JSA_id(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""$$id":"${value.toString}""""
  }
  case class JSA_ref(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""$$ref":"${value.toString}""""
  }
  case class JSA_title(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""title":"${value.toString}""""
  }
  case class JSA_description(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""description":"${value.toString}""""
  }
  case class JSA_type(value: JsonSchemaType) extends JsonSchemaStructure {
    override def toString: String = s""""type":"${value.toString}""""
  }
  case class JSA_properties(value: Map[String,JSS]) extends JsonSchemaStructure {
    override def toString: String = s""""properties":{${value.map{case(k,v) => "\""+k+"\":"+v.toString}.mkString(",")}}"""
  }
  case class JSA_required(value: Set[String]) extends JsonSchemaStructure {
    override def toString: String = s""""required":[${value.map(x => '"' + x + '"').mkString(",")}]"""
  }
  case class JSA_items(value: JSS) extends JsonSchemaStructure {
    override def toString: String = s""""items":${value.toString}"""
  }
  case class JSA_anyOf(value: Seq[JSS]) extends JsonSchemaStructure {
    override def toString: String = s""""anyOf":[${value.map(x => x.toString).mkString(",")}]"""
  }
  case class JSA_oneOf(value: Seq[JSS]) extends JsonSchemaStructure {
    override def toString: String = s""""oneOf":[${value.map(x => x.toString).mkString(",")}]"""
  }
  case class JSA_maxItems(value: Double) extends JsonSchemaStructure {
    override def toString: String = s""""maxItems":${value.toString}"""
  }
  case class JSA_maxProperties(value: Double) extends JsonSchemaStructure {
    override def toString: String = s""""maxItems":${value.toString}"""
  }
  case class JSA_additionalProperties(value: Boolean) extends JsonSchemaStructure {
    override def toString: String = s""""additionalProperties":${value.toString}"""
  }
  //  case class JSA_definitions(value: Map[String,JSS]) extends JsonSchemaStructure {
  //    override def toString: String = s"\"definitions\":{${value.map{case(k,v) => "\""+k+"\":"+v.toString}.mkString(",")}}"
  //  }


  case class JSS(
                  schema: Option[ JSA_schema ] = None,
                  id: Option[ JSA_id ] = None,
                  ref: Option[ JSA_ref ] = None,
                  title: Option[ JSA_title ] = None,
                  description: Option[ JSA_description ] = None,
                  `type`: Option[ JSA_type ] = None,
                  properties: Option[ JSA_properties ] = None,
                  required: Option[ JSA_required ] = None,
                  items: Option[ JSA_items ] = None,
                  anyOf: Option[ JSA_anyOf ] = None,
                  oneOf: Option[ JSA_oneOf ] = None,
                  maxItems: Option[ JSA_maxItems ] = None,
                  maxProperties: Option[ JSA_maxProperties ] = None,
                  additionalProperties: Option[ JSA_additionalProperties ] = None
                )
  {
    override def toString: String = {
      "{" +
        Seq(
          schema,
          id,
          ref,
          title,
          description,
          `type`,
          properties,
          required,
          items,
          anyOf,
          oneOf,
          maxItems,
          maxProperties,
          additionalProperties
        ).filterNot(_.equals(None)).map(_.get.toString).mkString(",") +
        "}"
    }

  }
  object JSS {
    @Override
    def apply(vs: Seq[JsonSchemaStructure]): JSS = {
      var schema: Option[ JSA_schema ] = None
      var id: Option[ JSA_id ] = None
      var ref: Option[ JSA_ref ] = None
      var title: Option[ JSA_title ] = None
      var description: Option[ JSA_description ] = None
      var `type`: Option[ JSA_type ] = None
      var properties: Option[ JSA_properties ] = None
      var required: Option[ JSA_required ] = None
      var items: Option[ JSA_items ] = None
      var anyOf: Option[ JSA_anyOf ] = None
      var oneOf: Option[ JSA_oneOf ] = None
      var maxItems: Option[ JSA_maxItems ] = None
      var maxProperties: Option[ JSA_maxProperties ] = None
      var additionalProperties: Option[ JSA_additionalProperties ] = None
      vs.foreach( jss => {
        jss match {
          case v: JSA_schema => schema = Some(v)
          case v: JSA_id => id = Some(v)
          case v: JSA_ref => ref = Some(v)
          case v: JSA_title => title = Some(v)
          case v: JSA_description => description = Some(v)
          case v: JSA_type => `type` = Some(v)
          case v: JSA_properties => properties = Some(v)
          case v: JSA_required => required = Some(v)
          case v: JSA_items => items = Some(v)
          case v: JSA_anyOf => anyOf = Some(v)
          case v: JSA_oneOf => oneOf = Some(v)
          case v: JSA_maxItems => maxItems = Some(v)
          case v: JSA_maxProperties => maxProperties = Some(v)
          case v: JSA_additionalProperties => additionalProperties = Some(v)
        }
      })
      return new JSS(
        schema = schema,
        id = id,
        ref = ref,
        title = title,
        description = description,
        `type` = `type`,
        properties = properties,
        required = required,
        items = items,
        anyOf = anyOf,
        oneOf = oneOf,
        maxItems = maxItems,
        maxProperties = maxProperties,
        additionalProperties = additionalProperties
      )
    }
  }


  private def getJEObj(parser: JsonParser): JsonSchemaType = {
    // stack containing previous JsonExplorerType
    val JEStack: scala.collection.mutable.Stack[JsonSchemaType] = scala.collection.mutable.Stack[JsonSchemaType]()
    while(!parser.isClosed){
      parser.nextToken() match {
        case FIELD_NAME =>
        // basic types
        case VALUE_STRING => JEStack.top.add(parser.getCurrentName,Str)
        case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT => JEStack.top.add(parser.getCurrentName,Num)
        case VALUE_NULL => JEStack.top.add(parser.getCurrentName,Null)
        case VALUE_TRUE | VALUE_FALSE => JEStack.top.add(parser.getCurrentName,Bool)
        case START_OBJECT =>
          JEStack.push(new Obj(new scala.collection.mutable.HashMap[String,JsonSchemaType]()))

        case END_OBJECT =>
          var JEval = JEStack.pop()
          if(JEStack.isEmpty) { // ended base object, don't want to read null after so add to
            return JEval
          } else { // pop off stack and add to new top
            JEStack.top.add(parser.getCurrentName, JEval)
          }
        case START_ARRAY =>
          JEStack.push(new Arr(new scala.collection.mutable.ListBuffer[JsonSchemaType]()))
        case END_ARRAY =>
          var JEval = JEStack.pop()
          JEStack.top.add(parser.getCurrentName,JEval)

        case VALUE_EMBEDDED_OBJECT => throw new Exception("Embedded_Object_Found??? " + parser.getCurrentToken.asString())
      }
    }
    throw new Exception("Unescaped")
  }


  def serialize(row: String): JsonSchemaType = {
    val factory = new JsonFactory()
    val parser: JsonParser = factory.createParser(row)
    return getJEObj(parser)
  }

}
