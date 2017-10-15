import org.apache.spark.sql._
import com.amazonaws.auth._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.redshift.AmazonRedshiftClient
import _root_.com.amazon.redshift.jdbc41.Driver
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext



// Instance Profile for authentication to AWS resources
val provider = new InstanceProfileCredentialsProvider();
val credentials: AWSSessionCredentials = provider.getCredentials.asInstanceOf[AWSSessionCredentials];
val token = credentials.getSessionToken;
val awsAccessKey = credentials.getAWSAccessKeyId;
val awsSecretKey = credentials.getAWSSecretKey

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._;

// Read weather table from hive
val rawWeatherDF = sqlContext.table("weather")

// Retrieve the header
val header = rawWeatherDF.first()

// Remove the header from the dataframe
val noHeaderWeatherDF = rawWeatherDF.filter(row => row != header)

// UDF to convert the air temperature from celsius to fahrenheit
val toFahrenheit = udf {(c: Double) => c * 9 / 5 + 32};

val weatherDF = noHeaderWeatherDF.withColumn("new_tmin", toFahrenheit(noHeaderWeatherDF("tmin"))).withColumn("new_tmax", toFahrenheit(noHeaderWeatherDF("tmax"))).drop("tmax").drop("tmin").withColumnRenamed("new_tmax","tmax").withColumnRenamed("new_tmin","tmin");

// Provide the jdbc url for Amazon Redshift
val jdbcURL = "jdbc:redshift://awsRedshiftClusterEndPoint:yourPort/yourDBName?user=youUserName&password=yourPassword;

// Create and declare an S3 bucket where the temporary files are written
val s3TempDir = "s3://yourS3Bucket/out/";

// Query against the ord_flights table in Amazon Redshift
val flightsQuery = """SELECT dep_delay_group, DAY_OF_MONTH, DAY_OF_WEEK, FL_DATE, f_days_from_holiday(year,month,day_of_month) AS DAYS_TO_HOLIDAY,
       UNIQUE_CARRIER, FL_NUM, SUBSTRING(DEP_TIME,1,2) AS DEP_HOUR, CAST(DEP_DEL15 AS SMALLINT), CAST(AIR_TIME AS INTEGER),
       CAST(FLIGHTS AS SMALLINT), CAST(DISTANCE AS SMALLINT) 
FROM staging.ord_flights
WHERE origin = 'ORD' AND   cancelled = 0""";


// Create a Dataframe to hold the results of the above query
val flightsDF = sqlContext.read.format("com.databricks.spark.redshift").option("url", jdbcURL).option("tempdir", s3TempDir).option("query", flightsQuery).option("temporary_aws_access_key_id", awsAccessKey).option("temporary_aws_secret_access_key", awsSecretKey).option("temporary_aws_session_token", token).load();

// Join the two dataframes
val joinedDF = flightsDF.join(weatherDF, flightsDF("fl_date") === weatherDF("dt"));

//Now that we have unified the data, we can write it back to another table in our Amazon Redshift cluster. After that is done, 
//the table can serve as a datasource for advanced analytics, 
//such as for developing predictive models using the Amazon Machine Learning service.


//To write to Amazon Redshift, the spark-redshift library first creates a table in Amazon Redshift using JDBC. 
//Then it copies the partitioned DataFrame as AVRO partitions to a temporary S3 folder that you specify. Finally, it executes 
//the Amazon Redshift COPY command to copy the S3 contents to the newly created Amazon Redshift table. 
//You can also use the append option with spark-redshift to append data to an existing Amazon Redshift table. 
//In this example, we will write the data to a table named ‘ord_flights’ in Amazon Redshift.

joinedDF.write.format("com.databricks.spark.redshift").option("temporary_aws_access_key_id", awsAccessKey).option("temporary_aws_secret_access_key", awsSecretKey).option("temporary_aws_session_token", token).option("url", jdbcURL).option("dbtable", "flight_weather").option("tempdir", s3TempDir).mode("error").save();


joinedDF.write.format("com.databricks.spark.redshift").option("url", jdbcURL).option("dbtable", "flight_weather").option("tempdir", s3TempDir).mode("error").save();



joinedDF.write.format("com.databricks.spark.redshift").option("temporary_aws_access_key_id", awsAccessKey).option("temporary_aws_secret_access_key", awsSecretKey).option("temporary_aws_session_token", token).option("url", jdbcURL).option("dbtable", "staging.ord_flights").option("aws_iam_role", "yourIAMRole").option("tempdir", s3TempDir).mode("error").save()
