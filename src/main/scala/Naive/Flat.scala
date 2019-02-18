package Naive

import BiMax.OurBiMax
import Explorer.JacksonSerializer
import JsonExplorer.SparkMain.LogOutput
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Flat {

  def test(train: RDD[String], validation: RDD[String], log: mutable.ListBuffer[LogOutput]): Unit = {
    val flatRows = train.mapPartitions(JacksonSerializer.serialize(_)).map(BiMax.OurBiMax.splitForValidation(_))
    val flatTotalSchema = flatRows.reduce((l,r) => l.union(r))
    val flatMandatorySchema = flatRows.reduce((l,r) => l.intersect(r))
    val flatOptionalSchema = flatTotalSchema -- flatMandatorySchema
    // calculate Precision
    log += LogOutput("FlatPrecision",BigInt(2).pow(flatOptionalSchema.size).toString(),"Flat Precision: ")
    // calculate Validation
    val (strict, notStrict) = validation.mapPartitions(x => JacksonSerializer.serialize(x)).map(x => OurBiMax.splitForValidation(x)).map(x => BiMax.OurBiMax.calculateValidation(x,ListBuffer(Tuple2(flatMandatorySchema,flatOptionalSchema)))).reduce((acc,y) => (acc._1+y._1,acc._2+y._2))
    log += LogOutput("FlatStrictValidation",((strict/validation.count().toDouble)*100.0).toString(),"Flat Strict Validation: ","%")
    log += LogOutput("FlatLooseValidation",((notStrict/validation.count().toDouble)*100.0).toString(),"Flat Loose Validation: ","%")

  }


}
