package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    pickupInfo.createOrReplaceTempView("pickupInfo")
    val pointsInRange = spark.sql("select x,y,z, count(*) as pointsCount from pickupinfo where x>=" + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " group by x,y,z").persist()
    pointsInRange.createOrReplaceTempView("pointsInRange")
    
    var sigmaX=0 : Long;
    var sqSigmaX=0 : Long;
    val pointsList = pointsInRange.collect()
    
    for (row <- pointsList)
    {   
      var pointCount = row.mkString(",").split(",")(3).toLong
      sigmaX += pointCount
      sqSigmaX += pointCount*pointCount
    }
    
    val Xbar = sigmaX.toDouble/numCells.toDouble
    val S = Math.sqrt(sqSigmaX.toDouble/numCells.toDouble - Xbar*Xbar)
    
    spark.udf.register("calcZScore", (totalCount:Double, neighbourCount:Double) => (HotcellUtils.calcZScore(totalCount, neighbourCount, numCells, Xbar, S )) )
    
    val zscoreSql = spark.sql("select a0.x as x,a0.y as y,a0.z as z, sum(a1.pointsCount) as totalCount, count(*) as neighbourCount from pointsInRange as a0, pointsInRange as a1 where a1.x>= a0.x-1 and a1.x<=a0.x+1 and a1.y>=a0.y-1 and a1.y<=a0.y+1 and a1.z>=a0.z-1 and a1.z<=a0.z+1 group by a0.x,a0.y,a0.z").persist()
    zscoreSql.createOrReplaceTempView("zscoreview")
    
    val getZScoreView = spark.sql("select x,y,z,neighbourCount,calcZScore(totalCount, neighbourCount) as zScore from zscoreview order by zScore desc").persist()
    getZScoreView.createOrReplaceTempView("getZScoreView")
    
    val result = spark.sql("select x,y,z from getZScoreView")
    result.createOrReplaceTempView("result")

    return result
  }
}
