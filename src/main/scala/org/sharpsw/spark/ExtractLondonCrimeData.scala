package org.sharpsw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.sharpsw.spark.TraceUtil.{timed, timing}

object ExtractLondonCrimeData {
  val sparkSession: SparkSession = SparkSession.builder.appName("LondonCrimeDataExercise001").master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val fileContents = sparkSession.sparkContext.textFile(args(0))
    val (headerColumns, contents) = timed("Step 1 - reading contents", readContents(fileContents))
    contents.persist()
    headerColumns.foreach(println)
    val boroughs = timed("Extracting distinct boroughs", extractDistinctBoroughs(contents))
    timed("Exporting boroughs to csv", saveDataFrame(boroughs, "borough.csv"))

    val majorCrimeCategories = timed("Extracting major categories", extractDistinctMajorCrimeCategories(contents))
    timed("Exporting major categories to csv", saveDataFrame(majorCrimeCategories, "major_category.csv"))

    val minorCrimeCategories = timed("Extracting minor categories", extractDistinctMinorCrimeCategories(contents))
    timed("Exporting minor category to csv", saveDataFrame(minorCrimeCategories, "minor_category.csv"))

    val categories = timed("Extracting categories to csv", extractCombinedCategories(contents))
    timed("Exporting categories to csv", saveDataFrame(categories, "categories.csv"))

    println(timing)
  }

  def readContents(contents: RDD[String]): (List[String], DataFrame) = {
    val headerColumns = contents.first().split(",").toList
    val schema = dfSchema(headerColumns)

    val data = contents.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it).map(_.split(",").toList).map(row)
    val dataFrame = sparkSession.createDataFrame(data, schema)
    (headerColumns, dataFrame)
  }

  def extractDistinctBoroughs(contents: DataFrame): DataFrame = {
    extractDistinctValues(contents, "borough")
  }

  def extractDistinctMajorCrimeCategories(contents: DataFrame): DataFrame = {
    extractDistinctValues(contents, "major_category")
  }

  def extractDistinctMinorCrimeCategories(contents: DataFrame): DataFrame = {
    extractDistinctValues(contents, "minor_category")
  }

  def extractCombinedCategories(contents: DataFrame): DataFrame = {
    contents.select("major_category", "minor_category").distinct.sort("major_category", "minor_category")
  }

  private def extractDistinctValues(contents: DataFrame, columnName: String): DataFrame = {
    contents.select(contents(columnName)).distinct.orderBy(asc(columnName))
  }

  private def saveDataFrame(contents: DataFrame, fileName: String): Unit = {
    contents.coalesce(1).write.mode("overwrite").option("header", "true").csv(fileName)
  }

  def row(line: List[String]): Row = {
    //val list = line.head::line.tail.map(_.toDouble)
    val list = List(line.head, line(1), line(2), line(3), line(4).toInt, line(5).toInt, line(6).toInt)
    Row.fromSeq(list)
  }

  def dfSchema(columnNames: List[String]): StructType = {
    //val firstField = StructField(columnNames.head, StringType, nullable = false)
    //val fields = columnNames.map(field => StructField(field, StringType, nullable = false))
    val lsoaCodeField       = StructField(columnNames.head, StringType, nullable = false)
    val boroughField        = StructField(columnNames(1), StringType, nullable = false)
    val majorCategoryField  = StructField(columnNames(2), StringType, nullable = false)
    val minorCategoryField  = StructField(columnNames(3), StringType, nullable = false)
    val valueField          = StructField(columnNames(4), IntegerType, nullable = false)
    val yearField           = StructField(columnNames(5), IntegerType, nullable = false)
    val monthField          = StructField(columnNames(6), IntegerType, nullable = false)

    val fields = List(lsoaCodeField, boroughField, majorCategoryField, minorCategoryField, valueField, yearField, monthField)
    StructType(fields)
  }

}
