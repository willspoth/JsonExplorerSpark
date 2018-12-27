package Explorer

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.core.JsonToken._


object JacksonSerializer {

  private def getJEObj(parser: JsonParser): JsonExplorerType = {
    // stack containing previous JsonExplorerType
    val JEStack: scala.collection.mutable.Stack[JsonExplorerType] = scala.collection.mutable.Stack[JsonExplorerType]()
    while(!parser.isClosed){
      parser.nextToken() match {
        case FIELD_NAME =>
        // basic types
        case VALUE_STRING => JEStack.top.add(parser.getCurrentName,JE_String)
        case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT => JEStack.top.add(parser.getCurrentName,JE_Numeric)
        case VALUE_NULL => JEStack.top.add(parser.getCurrentName,JE_Null)
        case VALUE_TRUE | VALUE_FALSE => JEStack.top.add(parser.getCurrentName,JE_Boolean)
        case START_OBJECT =>
          JEStack.push(new JE_Object(new scala.collection.mutable.HashMap[String,JsonExplorerType]()))

        case END_OBJECT =>
          var JEval = JEStack.pop()
          if(JEval.isEmpty())
            JEval = JE_Empty_Object
          if(JEStack.isEmpty) { // ended base object, don't want to read null after so add to
            return JEval
          } else { // pop off stack and add to new top
            JEStack.top.add(parser.getCurrentName,JEval)
          }
        case START_ARRAY =>
          JEStack.push(new JE_Array(new scala.collection.mutable.ListBuffer[JsonExplorerType]()))
        case END_ARRAY =>
          var JEval = JEStack.pop()
          if(JEval.isEmpty())
            JEval = JE_Empty_Array
          JEStack.top.add(parser.getCurrentName,JEval)

        case VALUE_EMBEDDED_OBJECT => throw new UnknownTypeException("Embedded_Object_Found??? " + parser.getCurrentToken.asString())
      }
    }
    throw new UnknownTypeException("Unescaped")
  }


  def serialize(rows: Iterator[String]): Iterator[JsonExplorerType] = {

    // local collector, collects attributeName and type and it's count.
    val mapped_rows: scala.collection.mutable.ListBuffer[JsonExplorerType] = new scala.collection.mutable.ListBuffer[JsonExplorerType]()
    val factory = new JsonFactory()
    while(rows.hasNext){
      val parser: JsonParser = factory.createParser(rows.next())
      mapped_rows += getJEObj(parser)
      // next row
    }
    return mapped_rows.iterator
  }

}
