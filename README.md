# How to execute a REST API call on Apache Spark the Right Way - Scala

Note: This repository is a duplicate of another that I have created ([https://github.com/jamesshocking/Spark-REST-API-UDF](https://github.com/jamesshocking/Spark-REST-API-UDF)), albeit demonstrates how to execute REST API calls from Apache Spark using Scala.  The other repository is for those looking for the Python version of this code.

### Note
Oct 2022 - Since originally writing this demo, the example URL https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json is terminating all requests.  The end result is that it will appear as if the code isn't work.  The problem is that when the Python Requests library executes the request, the remote server terminates the request and an exception is thrown.  The code is still valid, but I recommend trying with a different endpoint

## Introduction

Apache Spark is a wonderful invention that can solve a great many problems.  Its flexibility and adaptability gives great power but also the opportunity for big mistakes.  One such mistake is executing code on the driver, which you thought would run in a distributed way on the workers.  One such example is when you execute Python code outside of the context of a Dataframe.

For example, when you execute code similar to:

```scala
val s = "Scala is amazing"
print(s)
```

Apache Spark will execute the code on the driver, and not a worker.  This isn't a problem with such a simple command, but what happens when you need to download large amounts of data via a REST API service?  In this and the demo code, I am using the OkHttp3 library [https://square.github.io/okhttp/](https://square.github.io/okhttp/).  For those needed to request an Auth Token to access a REST API, OkHttp greatly simplifies this process.

```scala
val client: OkHttpClient = new OkHttpClient();

val headerBuilder = new Headers.Builder
val headers = headerBuilder
  .add("content-type", "application/json")
  .build

val result = try {
    val request = new Request.Builder()
      .url(url)
      .headers(headers)
      .build();

    val response: Response = client.newCall(request).execute()
    response.body().string()
  }
  catch {
    case _: Throwable => "Something went wrong"
  }
  
print(result)
```

If we execute the code above, it will be executed on the Driver.  If I were to create a loop with multiple of API requests, there would be no parallelism, no scaling, leaving a huge dependency on the Driver.  This approach criples Apache Spark and leaves it no better than a single threaded program.  To take advantage of Apache Spark's scaling and distribution, an alternative solution must be sought.

The solution is to use a UDF coupled to a withColumn statement.  This example, demonstrates how one can create a DataFrame whereby each row represents a single request to the REST service.  A UDF (User Defined Function) is used to encapsulate the HTTP request, returning a structured column that represents the REST API response, which can then be sliced and diced using the likes of explode and other built-in DataFrame functions (Or collapsed, see [https://github.com/jamesshocking/collapse-spark-dataframe](https://github.com/jamesshocking/collapse-spark-dataframe)).

With the advent of Apache Spark 3.0, the udf function has been deprecated.  [Migrating from Apache Spark 2.x to 3.0](https://spark.apache.org/docs/3.0.0-preview/sql-migration-guide.html#upgrading-from-spark-sql-24-to-30).  The example code assumes Apache Spark 3.0 but review the code comments for the 2.x version of implementing a UDF.

## The Solution

For the sake of brevity I am assuming that a SparkSession has been created and assigned to a variable called spark.  In addition, for this example I will be used the OkHttp HTTP library.

The solution assumes that you need to consume data from a REST API, which you will be calling multiple times to get the data that you need.  In order to take advantage of the parallelism that Apache Spark offers, each REST API call will be encapsulated by a UDF, which is bound to a DataFrame.  Each row in the DataFrame will represent a single call to the REST API service.  Once an action is executed on the DataFrame, the result from each individual REST API call will be appended to each row as a Structured data type.

To demonstrate the mechanism, I will be using a free US Government REST API service that returns the makes and models of USA vehicles [https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json](https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json).

### Start by declaring your imports:

```scala
import okhttp3.{Headers, OkHttpClient, Request, Response}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.{col, udf, from_json, explode}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
```

### Now declare a function that will execute our REST API call

Use the OkHttp library to execute either an HTTP get or a post.  There is nothing special about this function, it even returns the REST service response as a String.

```scala
def ExecuteHttpGet(url: String) : Option[String] = {

  val client: OkHttpClient = new OkHttpClient();

  val headerBuilder = new Headers.Builder
  val headers = headerBuilder
    .add("content-type", "application/json")
    .build

  val result = try {
      val request = new Request.Builder()
        .url(url)
        .headers(headers)
        .build();

      val response: Response = client.newCall(request).execute()
      response.body().string()
    }
    catch {
      case _: Throwable => null
    }

  Option[String](result)
}
```

### Define the response schema and the UDF

This is one of the parts of Apache Spark that I really like.  I can pick and chose what values I want from the JSON returned by the REST API call.  All I have to do is declare what parts of the JSON I want in a schema, which will be used by the from_json function.  

```scala
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
```

Next I declare the UDF, making sure to set the return type as a String.  This will ensure that the new column, which is used to execute the UDF, will be ready to call from_json.  After executing the UDF, and then from_json, the row and column in the Dataframe will contain a structured object rather than plain JSON formatted text.  

```scala
val executeRestApiUDF = udf(new UDF1[String, String] {
  override def call(url: String) = {
    ExecuteHttpGet(url).getOrElse("")
  }
}, StringType)
```

### Create the Request DataFrame and Execute

The final piece is to create a DataFrame where each row represents a single REST API call.  The number of columns in the Dataframe are up to you but you will need at least one, which will host the URL and/or parameters required to execute the REST API call.  There are a number of ways to create a Dataframe from a Sequence but I am going to use a case class.

Using the US Goverments free-to-access vehicle make REST service, we would create a Dataframe as follows:

```scala
case class RestAPIRequest (url: String)

val restApiCallsToMake = Seq(RestAPIRequest("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json"))
val source_df = restApiCallsToMake.toDF()
```

The case class is used to define the columns of the Dataframe, and using the toDF method of the spark version of the Seq object (available via an import to the Sql Context implicits object), we end up with a Dataframe where each row represents a separate API request.

All being well, the Dataframe will look like:

|  url           | 
| ------------- |
|  https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json | 

Finally we can use withColumn on the Dataframe to execute the UDF and REST API, before using a second withColumn to convert the response String into a Structured object.

```scala
val execute_df = source_df
    .withColumn("result", executeRestApiUDF(col("url")))
    .withColumn("result", from_json(col("result"), restApiSchema))
```

As Spark is lazy, the UDF will execute once an action like count() or show() is executed against the Dataframe.  Spark will distribute the API calls amongst all the workers, before returning the results such as:

|  url           | result |
| -------------| --------|
|  https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json | [9773, Response r...] |

The REST service returns a number of attributes and we're only interested in the one identified as Results (i.e. result.Results), which happens to be an array.  Using the explode method, and a select, we can output all Make ID's and Name's returned by the service.  

```scala
execute_df.select(explode(col("result.Results")).alias("makes"))
      .select(col("makes.Make_ID"), col("makes.Make_Name"))
      .show
```

you would see:

|results_Make_ID|   results_Make_Name|
|---------------|--------------------|
|            440|        ASTON MARTIN|
|            441|               TESLA|
|            442|              JAGUAR|
|            443|            MASERATI|
|            444|          LAND ROVER|
|            445|         ROLLS ROYCE|
