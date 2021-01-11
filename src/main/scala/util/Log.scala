package util

import java.io.FileWriter
import scala.collection.mutable

object Log {

  private val logBuffer = mutable.ListBuffer[LogOutput]()

  def add(key: String, value: String) = logBuffer.append(LogOutput(key,value))
  def clearLog(): Unit = logBuffer.clear()
  def printLog() = logBuffer.foreach(x => println(x.toString))
  def writeLog(outputFileName: String, schema: String): Unit = {
    val outputFile = new FileWriter(outputFileName,true)
    outputFile.write("{" + logBuffer.map(_.toJson).mkString(",") + "}\n")
    outputFile.write(schema)
    outputFile.close()
  }

  private case class LogOutput(key:String, value:String){
    private def capitalize(str: String, spacer: String = "") = str.split(" ").map(x => x.head.toUpper + x.tail).mkString(spacer)
    override def toString: String = s"""${capitalize(key," ")}: ${value}"""
    def toJson: String = s""""${capitalize(key,"")}": "${value}""""
  }

}