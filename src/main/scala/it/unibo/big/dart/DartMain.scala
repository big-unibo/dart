package it.unibo.big.dart

import breeze.stats.distributions.Gaussian
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

object DartMainGithub extends App {

  object DartConf {
    /** weights to smooth the habit probability **/
    def habitWeights = Array(0.4, 0.2, 0.1, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    /** spatial threshold for staypoint relevance (meters) **/
    var adjacencyThreshold = 200
    /** temporal normalizing factor for delta_time (seconds) **/
    var temporalThr = 3600
    /** spatial normalizing factor for delta_space search (meters) **/
    val spatialThr = 200
  }

  /** gaussian probability density function **/
  def gaussPDF(x: Double): Double = (1 / (scala.math.sqrt(2 * scala.math.Pi) * 0.4)) * scala.math.exp(Gaussian(0.0, 0.4).unnormalizedLogPdf(x))

  /** spatial function s **/
  def spatialGauss(value: Double, st: Double) = gaussPDF(value / st)

  /** temporal function t **/
  def temporalGauss(value: Double, tt: Double) = gaussPDF(value / tt)

  /**
   * Score function for similarity between two points
   *
   * @param spatialDistance  Spatial distance in meters
   * @param temporalDistance Temporal distance in meters
   * @param habitDistance  Delta habit
   * @return localScore
   */
  def score(spatialDistance: Double, temporalDistance: Double, habitDistance: Double): Double = {
    math.pow(temporalGauss(temporalDistance,DartConf.temporalThr), 0.5) * spatialGauss(spatialDistance,DartConf.spatialThr) +
      (1 - math.pow(temporalGauss(temporalDistance,DartConf.temporalThr), 0.5)) * spatialGauss(spatialDistance,DartConf.spatialThr) * habitDistance
  }

  object DB {
    var name: String = "database_name"
    
    var in_staypoints: String = "input_table_staypoints"
    var in_pointsOfStaypoint: String = "input_table_pointsOfStaypoints"
    var in_knownTrajectories: String = "input_table_knownTrajectories"

    var out_pointsToTweets: String = "output_table_step_1"
    var out_behavioral_model: String = "output_table_step_2"
    var out_staypoint_overlaps: String = "output_table_step_3"
    var out_results: String = "output_table_step_4"
    var out_results_rank: String = "output_table_step_5"
  }

  override def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("DART")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("geospark.global.index", "true")
      .config("geospark.join.gridtype", "rtree")
      .config("spark.kryoserializer.buffer.max", "512")
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",true)
      .enableHiveSupport()
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark.sqlContext)
    spark.sql(s"""use ${DB.name}""")

    computePointsToTweets(spark)
    computeBehavioralModel(spark)
    computeStaypointOverlaps(spark)
    computesScoresAndResults(spark)
    computeRanks(spark)
  }

  /**
   * Compute and store the table that associates GPS pings 
   * to tweets that are sufficiently close
   *
   * @param spark Spark session
   */
  def computePointsToTweets(spark:SparkSession): Unit = {

    spark.sql(s"drop table if exists ${DB.out_pointsToTweets} purge")

    // Prepare the two tables (which makes GeoSpark compute the spatial index)
    spark
      .sql(s"""select 
          userid, staypointid, longitude, latitude,
          ST_Transform(ST_GeomFromWKT(concat('POINT(',longitude, ' ', latitude, ')')),
            \'epsg:4326\', \'epsg:3857\') latlon
        from ${DB.in_staypoints}""")
      .createOrReplaceTempView("staypoints_geo")

    spark
      .sql(s"""select
          tweetid, tweetuserid, timest, longitude, latitude,
          ST_Transform(ST_GeomFromWKT(concat('POINT(',longitude, ' ', latitude, ')')),
            \'epsg:4326\', \'epsg:3857\') latlon
        from ${DB.in_knownTrajectories}
        where latitude is not null and longitude is not null""")
      .createOrReplaceTempView("tweets_geo")

    // First associate tweets to stay points..
    spark.sql(s"""select
          s.userid userId, s.staypointid staypointId,
          t.tweetid tweetId, t.tweetuserid tweetUserId,
          hour(from_unixtime(cast(t.timest as bigint))) hour,
          t.timest time, st_distance(s.latlon,t.latlon) as distance
        from tweets_geo t, staypoints_geo s
        where st_distance(s.latlon,t.latlon)<${DartConf.spatialThr}""")
      .createOrReplaceTempView("staypoints_to_tweets")

    // ..then get the pings of each stay point
    spark.sql(s"""select
          s2t.userId, s2t.staypointId, s2t.tweetId, s2t.tweetUserId, s2t.hour, s2t.distance,
          abs(s2t.time - p2s.timest) as timediff,
          p2s.timest timest
        from staypoints_to_tweets s2t,  ${DB.in_pointsOfStaypoint} p2s
        where s2t.staypointId = p2s.staypointid """)
      .write.bucketBy(200, "staypointId","hour")
      .saveAsTable(DB.out_pointsToTweets)
  }

  /**
   * Computes the statistical model of each user's habit by
   * counting the number of distinct days for which there
   * is a gps ping in a certain SP in a certain time bin
   *
   * @param spark Spark session
   */
  def computeBehavioralModel(spark:SparkSession): Unit = {

    spark.sql(s"select s.userid userId, s.staypointid staypointId, hour(from_unixtime(p.timest)) as timeSlot, " +
      s"  count(distinct date_format(from_unixtime(p.timest),'YYYY-MM-dd')) as cnt " +
      s"from ${DB.in_staypoints} s, ${DB.in_pointsOfStaypoint} p " +
      s"where s.userid = p.userid and s.staypointid = p.staypointid " +
      s"group by s.userid, hour(from_unixtime(p.timest)), s.staypointid")
      .createOrReplaceTempView("wsp")

    spark.sql("select userId, timeSlot, SUM(cnt) as tot from wsp group by userId, timeSlot")
      .createOrReplaceTempView("wh")

    /** Compute (#ping in staypoint in hour) / (#ping in hour) for a user **/
    spark.sql("select wsp.userId, wsp.staypointId, wsp.timeSlot, wsp.cnt/wh.tot as ratio " +
      "from wsp, wh " +
      "where wsp.userId = wh.userId and wsp.timeSlot = wh.timeSlot")
      .createOrReplaceTempView("wsp_ratio")

    spark.sql("select wsp1.userId, wsp1.staypointId, wsp1.timeSlot, " +
      " SUM( wsp2.ratio * " +
      s"   CASE WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=0 THEN ${DartConf.habitWeights(0)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=1 THEN ${DartConf.habitWeights(1)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=2 THEN ${DartConf.habitWeights(2)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=3 THEN ${DartConf.habitWeights(3)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=4 THEN ${DartConf.habitWeights(4)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=5 THEN ${DartConf.habitWeights(5)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=6 THEN ${DartConf.habitWeights(6)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=7 THEN ${DartConf.habitWeights(7)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=8 THEN ${DartConf.habitWeights(8)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=9 THEN ${DartConf.habitWeights(9)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=10 THEN ${DartConf.habitWeights(10)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=11 THEN ${DartConf.habitWeights(11)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=12 THEN ${DartConf.habitWeights(12)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=13 THEN ${DartConf.habitWeights(11)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=14 THEN ${DartConf.habitWeights(10)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=15 THEN ${DartConf.habitWeights(9)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=16 THEN ${DartConf.habitWeights(8)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=17 THEN ${DartConf.habitWeights(7)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=18 THEN ${DartConf.habitWeights(6)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=19 THEN ${DartConf.habitWeights(5)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=20 THEN ${DartConf.habitWeights(4)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=21 THEN ${DartConf.habitWeights(3)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=22 THEN ${DartConf.habitWeights(2)} " +
      s"        WHEN abs(wsp1.timeSlot-wsp2.timeSlot)=23 THEN ${DartConf.habitWeights(1)} " +
      s"  END " +
      " ) w " +
      "from wsp_ratio wsp1, wsp_ratio wsp2 " +
      "where wsp1.userId = wsp2.userId and wsp1.staypointId = wsp2.staypointId " +
      "group by wsp1.userId, wsp1.staypointId, wsp1.timeSlot")
      .write.bucketBy(200, "staypointId","timeSlot")
      .saveAsTable(DB.out_behavioral_model)
  }


  /**
   * Create the table out_staypoint_overlaps to verify, for each personal gazetteer,
   * the personal gazetteers it overlaps with. This implementation overcomes
   * GeoSpark's limitation by carrying out the self-join
   * between the staypoint table in a custom but efficient way
   *
   * @return
   */
  def computeStaypointOverlaps(spark:SparkSession): Unit = {
    val dfSp = spark.sql(s"""select
          userid uid, staypointid sid,
          longitude lon, latitude lat,
          round(longitude,3) joinlon, round(latitude,3) joinlat
        from ${DB.in_staypoints}""")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val dfSp21 = dfSp.withColumn("joinlat",col("joinlat")+0.001)
    val dfSp22 = dfSp.withColumn("joinlat",col("joinlat")+0.001)
      .withColumn("joinlon",col("joinlon")+0.001)
    val dfSp23 = dfSp.withColumn("joinlon",col("joinlon")+0.001)
    val dfSp24 = dfSp.withColumn("joinlat",col("joinlat")-0.001)
      .withColumn("joinlon",col("joinlon")+0.001)
    val dfSp25 = dfSp.withColumn("joinlat",col("joinlat")-0.001)
    val dfSp26 = dfSp.withColumn("joinlat",col("joinlat")-0.001)
      .withColumn("joinlon",col("joinlon")-0.001)
    val dfSp27 = dfSp.withColumn("joinlon",col("joinlon")-0.001)
    val dfSp28 = dfSp.withColumn("joinlat",col("joinlat")+0.001)
      .withColumn("joinlon",col("joinlon")-0.001)

    dfSp.createOrReplaceTempView("so_staypoints_geo_1")
    dfSp.union(dfSp21).union(dfSp22).union(dfSp23).union(dfSp24)
      .union(dfSp25).union(dfSp26).union(dfSp27).union(dfSp28)
      .createOrReplaceTempView("so_staypoints_geo_2")

    // Euclidean distance
    val dist = udf((lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
      val x = lat1 - lat2
      val y = (lon1 - lon2) * Math.cos(lat2)
      110.25 * Math.sqrt(x * x + y * y)
    })

    spark.sql(s"""select
          s1.uid as userId1, s2.uid as userId2,
          s1.sid staypointId1, s2.sid staypointId2,
          s1.lon lon1, s1.lat lat1, s2.lon lon2, s2.lat lat2
        from so_staypoints_geo_1 s1 join so_staypoints_geo_2 s2
          on (s1.joinlon = s2.joinlon and s1.joinlat = s2.joinlat)""")
      .filter(dist(col("lat1"),col("lon1"),col("lat2"),col("lon2"))
        < DartConf.adjacencyThreshold)
      .filter("staypointId1 <> staypointId2")
      .groupBy(col("userId1"),col("userId2"))
        .agg(collect_set("staypointId1") as "staypointIds1")
      .select("userId1","userId2","staypointIds1")
      .write.bucketBy(200, "userId1")
      .saveAsTable(DB.out_staypoint_overlaps)
  }

  /**
   * Compute global score
   *
   * @param matched_staypoints   The set of stay points of S matched in M
   * @param staypoint_overlaps   A set of stay points of S that match with other personal gazetteers
   * @param n_users_overlap      The number of personal gazetteers that overlap with S on staypoint_overlaps
   * @param sum_local_scores     The sum of localscores
   * @return global score
   */
  def score_staypoint_rel(matched_staypoints: Seq[String], staypoint_overlaps: Seq[String],
                          n_users_overlap: Long, sum_local_scores: Double): Double = {
    var x = 1
    if (is_x_contained_in_y(matched_staypoints, staypoint_overlaps)) {
      x = x + n_users_overlap.toInt
    }
    if (sum_local_scores == 0) {
      0.0
    }
    else {
      sum_local_scores.toDouble / x.toDouble
    }
  }

  /**
   * Verify if one set of stay point is contained in the other
   *
   * @param xs Set of stay point
   * @param ys Set of stay point
   * @return Whether xs is contained in ys
   */
  def is_x_contained_in_y(xs: Seq[String], ys: Seq[String]): Boolean = {
    ys != null && ys.containsSlice(xs)
  }

  /**
   * Compute closenesses, localScores, and the mappings (matched_staypoints),
   * and save in the out_results table
   *
   * @return
   */
  def computesScoresAndResults(spark:SparkSession): Unit = {
    import spark.implicits._

    // /*+ BROADCAST(wr) */ if the case study is small
    val dfToScore = spark.sql(s"""select p2t.userId, p2t.staypointId, p2t.tweetUserId, p2t.tweetId,
        p2t.distance, CAST(p2t.timediff as DOUBLE) as timediff, COALESCE(wr.w,0.0) habitDistance
      from ${DB.out_pointsToTweets} p2t
        left outer join ${DB.out_behavioral_model} wr
          on (p2t.hour = wr.timeSlot and p2t.staypointId = wr.staypointId) """)

    dfToScore
      // Compute closeness
      .map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3),
        score(row.getDouble(4), row.getDouble(5), row.getDouble(6)),
        spatialGauss(row.getDouble(4),DartConf.spatialThr),
        temporalGauss(row.getDouble(5),DartConf.temporalThr), row.getDouble(6)))
      .toDF("userId", "staypointId", "tweetUserId", "tweetId", "score", "s", "t", "w")
      .filter("score > 0")
      .repartition(1000, col("tweetId"), col("tweetUserId"))
      // For each tweet and user, keep only the ping with the higher closeness
      .withColumn("rank", row_number().over(Window.partitionBy("userId", "tweetUserId", "tweetId").orderBy(col("score").desc))).select("userId", "staypointId", "tweetUserId", "tweetId", "score", "s", "t", "w").where("rank == 1")
      // Compute the denominator of unq(k,S)
      .withColumn("sumScorePerTweet", sum(col("score")).over(Window.partitionBy("tweetUserId", "tweetId")))
      .createOrReplaceTempView("out_tmp_all_max_scores")

    //sum_weighted_scores is the local score
    val dfScore = spark.sql(
      s"""select
                  userid, tweetuserid,
                  sum(score*score/sumscorepertweet) sum_weighted_scores,
                  collect_set(case when score*score/sumscorepertweet > 0 then staypointid else null end) matched_staypoints
              from out_tmp_all_max_scores
              group by userid, tweetuserid""")
      .toDF("userId", "tweetUserId", "sum_weighted_scores", "matched_staypoints")

    dfScore.write.saveAsTable(DB.out_results)
  }

  /**
   * Compute globalScores and ranks, and save in the out_results_rank table
   *
   * @return
   */
  def computeRanks(spark:SparkSession): Unit = {

    spark.sql(
      s"""
            select
                r.userid, r.tweetuserid,
                coalesce(max(r.sum_weighted_scores)/coalesce(count(distinct s.userId2),0)+1 ,0),
                max(sum_weighted_scores)
            from ${DB.out_results} r
                left outer join ${DB.out_staypoint_overlaps} s
                    on (r.userid = s.userId1)
            where s.userId1 is null
              or size(array_intersect(r.matched_staypoints,s.staypointIds1))=size(r.matched_staypoints)
            group by r.userid, r.tweetuserid""")
      .toDF("userId", "tweetuserid", "globalScore", "sum_weighted_scores")
      .withColumn("denserank_sum", dense_rank().over(Window.partitionBy("userId")
        .orderBy(col("globalScore").desc, col("sum_weighted_scores").desc)))
      .write.saveAsTable(DB.out_results_rank)
  }


}