package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    
    val point = pointString.split(",").map(s => s.toDouble).iterator
    val rectanglePoints = queryRectangle.split(",").map(s => s.toDouble).iterator
        
    val x = point.next(); val y = point.next()

    val x1 = rectanglePoints.next(); val y1 = rectanglePoints.next()
    val x2 = rectanglePoints.next(); val y2 = rectanglePoints.next()

    if (x >= x1 && x <= x2 && y >= y1 && y <= y2)
      return true
        
    false
  }

  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val point1 = pointString1.split(",").map(s => s.toDouble).iterator
    val x1 = point1.next(); val y1 = point1.next()

    val point2 = pointString2.split(",").map(s => s.toDouble).iterator
    val x2 = point2.next(); val y2 = point2.next()

    val pointDistance = Euclidean_Distance(x1, y1, x2, y2)

    if (distance >= pointDistance)
      return true

    false
  }

  def Euclidean_Distance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    val value1 = math.pow(x1-x2, 2)
    val value2 = math.pow(y1-y2, 2)

    math.sqrt(value1 + value2)
  }


  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> { ST_Contains(queryRectangle, pointString) })

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> { ST_Contains(queryRectangle, pointString) })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> { ST_Within(pointString1, pointString2, distance) })

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> { ST_Within(pointString1, pointString2, distance) })
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
