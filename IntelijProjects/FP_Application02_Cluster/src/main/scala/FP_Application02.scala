import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FP_Application02 extends App{

  val spark = SparkSession.builder
    .appName("FP_Application02")
    .master("yarn")
    .config("spark.hadoop.fs.defaultFS", "hdfs://clu01.softnet.tuc.gr:8020")
    .config("spark.yarn.jars", "hdfs://clu01.softnet.tuc.gr:8020/user/xenia/jars/*.jar")
    .config("spark.hadoop.yarn.resourcemanager.address", "http://clu01.softnet.tuc.gr:8189")
    .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
    .enableHiveSupport()
    .getOrCreate()

  //val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val hdfsURI = "hdfs://clu01.softnet.tuc.gr:8020"
  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfsURI)
  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)


  val dataPath = "/user/chrisa/nmea_aegean"

  val OutputPath01 = "/user/fp2/Outputs_Application02/Question1"
  val OutputPath02 = "/user/fp2/Outputs_Application02/Question2"
  val OutputPath03 = "/user/fp2/Outputs_Application02/Question3"
  val OutputPath04 = "/user/fp2/Outputs_Application02/Question4"
  val OutputPath05 = "/user/fp2/Outputs_Application02/Question5"


  val aegeanData = spark.read
    .option("header", "true")
    .csv(dataPath)

  //////////////////////// Question 1 ////////////////////////


  val dataWithDate = aegeanData.withColumn("date",to_date(col("timestamp")))   // Create a new DF by adding a column with the extracted date from the date - time timestamp


  val q1 = dataWithDate
    .groupBy(col("station"),col("date")) // Group the dataset by the "station" and "date" columns
    .count() // Count the number of entries for each group
    .orderBy(asc("date")) //  Order the groups by "date"


  //////////////////////// Question 2 ////////////////////////


  val q2 = aegeanData
    .groupBy(col("mmsi").alias("Vessel id (mmsi)")) // Group the dataset by the "mmsi" column and rename it to "Vessel id (mmsi)"
    .count() // Count the number of entries for each group
    .orderBy(desc("count")) // Order in descending way based on count
    .withColumnRenamed("count", "Number of tracked positions") // Rename the column count to Number of tracked positions
    .limit(1) // Return just the first row of the dataset (since the data is ordered in descending form)


  //////////////////////// Question 3 ////////////////////////


  val dataWithDate1 = dataWithDate
    .filter(col("station") === 8006) // keep only data that have the station column equals 8006
    .select(col("date"), col("mmsi"), col("speedoverground").alias("speedoverground1")) // Select the date, mmsi, and speedoverground columns, renaming speedoverground to speedoverground1

  val dataWithDate2 = dataWithDate
    .filter(col("station") === 10003) // keep only data that have the station column equals 10003
    .select(col("date"), col("mmsi"), col("speedoverground").alias("speedoverground2")) // Select the date, mmsi, and speedoverground columns, renaming speedoverground to speedoverground2

  val q3 = dataWithDate1
    .join(dataWithDate2, Seq("mmsi","date")) // Join the two DFs using both mmsi and date as keys
    .select(((col("speedoverground1") + col("speedoverground2")) / 2).alias("avgSogPerRow")) // Calculate the average speed over ground (SOG) per row
    .agg(avg("avgSogPerRow").alias("Average SOG")) // Calculate the overall average SOG and rename it to "Average SOG"


  //////////////////////// Question 4 ////////////////////////

  val dataPreProcessed = dataWithDate
    .withColumn("Heading_degrees01", when( col("heading") > 360 , col("heading")%360) // Create a new DF by adding a column which normalizes the heading degrees to (-360,360) => if abs(heading degrees) > 360 using mod%360
      .otherwise(col("heading"))
    )
    .withColumn("COG_degrees01", when( col("courseoverground") > 360 , col("courseoverground")%360) // Add a column which normalizes the courseoverground degrees to (-360,360) if courseoverground degrees > 360 using mod%360
      .otherwise(col("courseoverground"))
    )

  val q4 = dataPreProcessed
    .groupBy(col("station")) // Group the dataset by the station column
    .agg(avg(abs(col("Heading_degrees01") - col("COG_degrees01"))).alias("Average Absolute Per Station")) // Calculate the Average ABS per station
    .orderBy(desc("Average Absolute Per Station"))  // Order in descending way based on average ABS
    .select("station","Average Absolute Per Station") // Select just the station and the "Average Abs Per Station In Degrees" columns


  //////////////////////// Question 5 ////////////////////////


  val q5 = aegeanData
    .groupBy(col("status")) // Group the dataset by the status column
    .count() // Count the number of entries for each group
    .orderBy(desc("count")) // Order in descending way based on count
    .limit(3) // Return just the first three rows of the dataset (since the data is ordered in descending form)


  //////// Show - Print results on driver's screen ////////


  q1.show()
  q2.show()
  q3.show()
  q4.show()
  q5.show()


  //////// Store results in HDFS as separate files ////////

  q1.rdd.saveAsTextFile(OutputPath01)
  q2.rdd.saveAsTextFile(OutputPath02)
  q3.rdd.saveAsTextFile(OutputPath03)
  q4.rdd.saveAsTextFile(OutputPath04)
  q5.rdd.saveAsTextFile(OutputPath05)


  //////// Stop the underlying SparkContext ////////


  spark.stop()
}