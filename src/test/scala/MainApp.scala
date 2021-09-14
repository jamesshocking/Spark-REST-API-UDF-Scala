package com.gastecka.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.{col, udf, from_json, explode}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object MainApp extends App {

  val appName: String = "main-app-test"

  // create a SQL Context
  //val conf = new SparkConf().setMaster("local[*]").setAppName(appName)
  // val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName(appName)
    .master("local[*]")
    .getOrCreate()

  // required for toDF on Seq
  import spark.implicits._

  // Define the UDF that will execute the HTTP(s) GET/POST for the RESTAPI
  // note: udf was deprecated with Spark 3.0.  If you're using Spark pre 3.0, use the following
  // For those using Spark pre 3.0, uncomment the folowing code:
  // create the local function that will execute our HTTP GET
  //val executeRestApi = (url: String) => {
  //  val httpRequest = new HttpRequest;
  //  httpRequest.ExecuteHttpGet(url)
  //}
  //
  // val executeRestApiUDF = udf(executeRestApi, restApiSchema)
  //
  // The following UDF code is for those using Spark 3.0 there-after
  val executeRestApiUDF = udf(new UDF1[String, String] {
    override def call(url: String) = {
      val httpRequest = new HttpRequest;
      httpRequest.ExecuteHttpGet(url).getOrElse("")
    }
  }, StringType)

  // Lets set up an example test
  // create the Dataframe to bind the UDF to
  case class RestAPIRequest (url: String)

  val restApiCallsToMake = Seq(RestAPIRequest("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json"))
  val source_df = restApiCallsToMake.toDF()

  // Define the schema used to format the REST response.  This will be used by from_json
  val restApiSchema = StructType(List(
    StructField("Count", IntegerType, true),
    StructField("Message", StringType, true),
    StructField("SearchCriteria", StringType, true),
    StructField("Results", ArrayType(
      StructType(List(
        StructField("Make_ID", IntegerType, true),
        StructField("Make_Name", StringType, true)
      ))
    ), true)
  ))

  // add the UDF column, and a column to parse the output to
  // a structure that we can interogate on the dataframe
  val execute_df = source_df
    .withColumn("result", executeRestApiUDF(col("url")))
    .withColumn("result", from_json(col("result"), restApiSchema))

  // call an action on the Dataframe to execute the UDF
  // process the results
  execute_df.select(explode(col("result.Results")).alias("makes"))
      .select(col("makes.Make_ID"), col("makes.Make_Name"))
      .show
}
