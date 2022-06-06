import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Sayari {
  def main(args: Array[String]): Unit = {
    val t0 = System.currentTimeMillis()
    val spark = SparkSession.builder().master("local").appName("Sayari").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*
    ****Ingesting into Dataframes***
    gbrDf.count = 2213
    ofacDf.count = 8839
    */
    val gbrDf = spark.read.json("src/main/resources/gbr.jsonl")
    val ofacDf = spark.read.json("src/main/resources/ofac.jsonl")

    gbrDf.printSchema()
    ofacDf.printSchema()

    /*
    ****Exploded the Columns to remove the arrays***
    -casting ID column to long from string
    */
    val gbrExplodedDf = gbrDf.withColumn("addresses", explode_outer(col("addresses"))).
      withColumn("aliases", explode_outer(col("aliases"))).
      withColumn("id_numbers", explode_outer(col("id_numbers"))).
      withColumn("nationality", explode_outer(col("nationality"))).
      withColumn("reported_dates_of_birth", explode_outer(col("reported_dates_of_birth"))).
      withColumn("id", col("id").cast("long")).withColumnRenamed("id", "uk_id")

    val ofacExplodedDf = ofacDf.withColumn("addresses", explode_outer(col("addresses"))).
      withColumn("aliases", explode_outer(col("aliases"))).
      withColumn("id_numbers", explode_outer(col("id_numbers"))).
      withColumn("nationality", explode_outer(col("nationality"))).
      withColumn("reported_dates_of_birth", explode_outer(col("reported_dates_of_birth"))).
      withColumn("id", col("id").cast("long")).withColumnRenamed("id", "ofac_id")

    /*
    ****Flattening json****
    gbrFlatDf.count = 60748
    ofacFlatDf.count = 98274
    -Creating a column called "addresses_value_substring" which is the string before the second "," seperator of the column "addresses_value"
    */
    val gbrFlatDf = gbrExplodedDf.withColumn("addresses_country", col("addresses").getField("country")).
      withColumn("addresses_postal_code", col("addresses").getField("postal_code")).
      withColumn("addresses_value", col("addresses").getField("value")).
      withColumn("aliases_type", col("aliases").getField("type")).
      withColumn("aliases_value", col("aliases").getField("value")).
      withColumn("id_numbers_comment", col("id_numbers").getField("comment")).
      withColumn("id_numbers_value", col("id_numbers").getField("value")).drop("addresses","aliases","id_numbers").
      withColumn("addresses_value_substring", substring_index(col("addresses_value"), ",", 2)).distinct()

    val ofacFlatDf = ofacExplodedDf.withColumn("addresses_country", col("addresses").getField("country")).
      withColumn("addresses_postal_code", col("addresses").getField("postal_code")).
      withColumn("addresses_value", col("addresses").getField("value")).
      withColumn("aliases_type", col("aliases").getField("type")).
      withColumn("aliases_value", col("aliases").getField("value")).
      withColumn("id_numbers_comment", col("id_numbers").getField("comment")).
      withColumn("id_numbers_value", col("id_numbers").getField("value")).drop("addresses","aliases","id_numbers").
      withColumn("reported_dates_of_birth", when(to_date(col("reported_dates_of_birth"),"dd MMM yyyy").isNotNull, date_format(to_date(col("reported_dates_of_birth"),"dd MMM yyyy"),"dd/MM/yyyy"))).
      withColumn("addresses_value_substring", substring_index(col("addresses_value"), ",", 2)).distinct()

    /*
    ****Joins****
    -Although the ofacFlatDf(reported_dates_of_birth) was converted to the format dd/MM/yyyy,
    the gbrFlatDf(reported_dates_of_birth) has the date formatted in both dd/MM/yyyy and MM/dd/yyyy resulting in the column being not of any use in the joins.
    -The name and aliases_value columns were used as interchangeable join because both were of string datatype and contained similar data.
    -The id_numbers_value column contain the unique values of passports or other national identification cards.
    -The addresses_value_substring is derived from addresses_value column and this is the unique value that helps with identify all the "Entity"
    */

    val gbrSelectDf = gbrFlatDf.select("uk_id","name","aliases_value","type","addresses_value_substring","id_numbers_comment","id_numbers_value").distinct.persist(StorageLevel.MEMORY_AND_DISK)
    val ofacSelectDf = ofacFlatDf.select("ofac_id","name","aliases_value","type","addresses_value_substring","id_numbers_comment","id_numbers_value").
      withColumnRenamed("name","ofac_name").withColumnRenamed("aliases_value","ofac_aliases_value").
      withColumnRenamed("type","ofac_type").withColumnRenamed("addresses_value_substring","ofac_addresses_value_substring").
      withColumnRenamed("id_numbers_comment","ofac_id_numbers_comment").withColumnRenamed("id_numbers_value","ofac_id_numbers_value").distinct.persist(StorageLevel.MEMORY_AND_DISK)

    println("gbrSelectDf.count " + gbrSelectDf.count)
    println("ofacSelectDf.count " + ofacSelectDf.count)

    val df1 = gbrSelectDf.join(ofacSelectDf, gbrSelectDf("name") === ofacSelectDf("ofac_name"), "inner").distinct
    val df2 = gbrSelectDf.join(ofacSelectDf, gbrSelectDf("aliases_value") === ofacSelectDf("ofac_aliases_value"), "inner").distinct
    val df3 = gbrSelectDf.join(ofacSelectDf, gbrSelectDf("name") === ofacSelectDf("ofac_aliases_value"), "inner").distinct
    val df4 = gbrSelectDf.join(ofacSelectDf, gbrSelectDf("aliases_value") === ofacSelectDf("ofac_name"), "inner").distinct
    val df5 = gbrSelectDf.join(ofacSelectDf, gbrSelectDf("id_numbers_value") === ofacSelectDf("ofac_id_numbers_value"), "inner").distinct
    val df6 = gbrSelectDf.join(ofacSelectDf, gbrSelectDf("addresses_value_substring") === ofacSelectDf("ofac_addresses_value_substring"), "inner").distinct

    val finalDf = df1.union(df2).union(df3).union(df4).union(df5).union(df6).distinct()

    /*
    ****FinalDf****
    -File logic to write and rename file
    -Delete output folder first before writing
    */
    import org.apache.hadoop.fs._
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path("src/main/resources/output/"),true)

    //Writing to csv and renaming
    finalDf.coalesce(1).write.format("csv").option("header", "true").save("src/main/resources/output/")
    val file = fs.globStatus(new Path("src/main/resources/output" + "/part*"))(0).getPath().getName()
    fs.rename(new Path("src/main/resources/output" + "/" + file), new Path("src/main/resources/output/" + "final.csv"))

    val t1 = System.currentTimeMillis()
    println("Proccessing time for Sayri " +(t1 - t0) / 1000 +" in seconds")
  }
}