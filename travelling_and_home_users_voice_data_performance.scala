def extractCellNumSC(hex_value: String): String = {
    val cNumMap = Map("1" -> "NA", "16" -> "cNum0", "17" -> "cNum1", "18" -> "cNum2", "19" -> "cNum12", "20" -> "cNum13", "21" -> "cNum14", "22" -> "cNum20", "23" -> "cNum18", "24" -> "cNum19", "25" -> "cNum9", "26" -> "cNum10", "27" -> "cNum11", "28" -> "cNum8", "29" -> "cNum17", "30" -> "cNum26", "102" -> "cNum21", "103" -> "cNum22", "104" -> "cNum23", "106" -> "cNum49", "107" -> "cNum50", "35" -> "cNum3", "36" -> "cNum4", "37" -> "cNum5", "38" -> "cNum33", "39" -> "cNum34", "40" -> "cNum35", "45" -> "cNum40", "46" -> "cNum41", "48" -> "cNum6", "49" -> "cNum15", "50" -> "cNum24", "51" -> "cNum7", "52" -> "cNum16", "53" -> "cNum25", "54" -> "cNum27", "55" -> "cNum28", "56" -> "cNum29", "57" -> "cNum30", "58" -> "cNum31", "59" -> "cNum32")
    if (hex_value == null) {
      "NA"
    } else {
      cNumMap.getOrElse(hex_value, "NA")
    }
  }
  val extractCellNumDSC: (String) => String = extractCellNumSC
  val extractCellNumUDFSC = udf(extractCellNumDSC)

 def getBand(ecgi: String): String = {
    val bandMap = Map("10" -> "2300c1", "11" -> "2300c1", "12" -> "2300c1", "13" -> "2300c2", "14" -> "2300c2", "15" -> "2300c2", "16" -> "2300c1", "17" -> "2300c1"
      , "18" -> "2300c1", "19" -> "2300c2", "1a" -> "2300c2", "1b" -> "2300c2", "1c" -> "2300c1", "1d" -> "2300c1", "1e" -> "2300c1", "66" -> "2300c2"
      , "67" -> "2300c2", "68" -> "2300c2", "23" -> "1800c1", "24" -> "1800c1", "25" -> "1800c1", "26" -> "1800c2", "27" -> "1800c2", "28" -> "1800c2"
      , "29" -> "1800c1", "2a" -> "1800c1", "2b" -> "1800c1", "2c" -> "1800c2", "2d" -> "1800c2", "2e" -> "1800c2", "30" -> "850c1", "31" -> "850c1"
      , "32" -> "850c1", "33" -> "850c2", "34" -> "850c2", "35" -> "850c2", "36" -> "850c1", "37" -> "850c1", "38" -> "850c1", "39" -> "850c2"
      , "3a" -> "850c2", "3b" -> "850c2")

    //Map("10" -> "2300", "11" -> "2300", "12" -> "2300", "13" -> "2300", "14" -> "2300", "15" -> "2300", "16" -> "2300", "17" -> "2300", "18" -> "2300", "19" -> "2300", "1a" -> "2300", "1b" -> "2300", "1c" -> "2300", "1d" -> "2300", "1e" -> "2300", "66" -> "2300", "67" -> "2300", "68" -> "2300", "23" -> "1800", "24" -> "1800", "25" -> "1800", "26" -> "1800", "27" -> "1800", "28" -> "1800", "29" -> "1800", "2a" -> "1800", "2b" -> "1800", "2c" -> "1800", "2d" -> "1800", "2e" -> "1800", "30" -> "850", "31" -> "850", "32" -> "850", "33" -> "850", "34" -> "850", "35" -> "850", "36" -> "850", "37" -> "850", "38" -> "850", "39" -> "850", "3a" -> "850", "3b" -> "850")

    if (ecgi == null) {
      "unmapped"
    } else {
      bandMap.getOrElse(ecgi.substring(7, 15).toLong.toHexString.takeRight(2), "unmapped")
    }
  }
  val getBandSC: (String) => String = getBand
  val getBandUDF = udf(getBandSC)

  def getSector(sectorID: String): String = {
    val sectorMap = Map("1" -> "alpha", "4" -> "alpha", "2" -> "beta", "5" -> "beta", "3" -> "gamma", "6" -> "gamma")
    if (sectorID == null) {
      "alpha"
    } else {
      sectorMap.getOrElse(sectorID, "alpha")
    }
  }
  val getSectorSC: (String) => String = getSector
  val getSectorUDF = udf(getSectorSC)

  def getHourSignature(input:scala.collection.mutable.WrappedArray[Int])={
    val str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if(input.length >0 )
    {
      input.toList.sorted.map(str(_)).mkString
    }
    else{
      ""
    }
  }
  val getHourSignatureSC :scala.collection.mutable.WrappedArray[Int] => String = getHourSignature
  val getHourSignatureUDF = udf(getHourSignatureSC)

  /**
    * Find distance between two points
    * @param lat1
    * @param lon1
    * @param lat2
    * @param lon2
    * @return
    */
  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    import math._
    val R = 6372.8 * 1000 //radius in meters
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians
    val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c
  }
  val getDistanceMeter: (Double, Double, Double, Double) => Double = haversine(_, _, _, _)
  val getDistanceMeterUDF = udf(getDistanceMeter)

  def commonHour(input:scala.collection.mutable.WrappedArray[scala.collection.mutable.WrappedArray[Int]])={

    if(input.length > 0  && input != null)
      input.flatMap( x => x).toSet.toList.sorted
    else
      List()
  }
  val commonHourSC :scala.collection.mutable.WrappedArray[scala.collection.mutable.WrappedArray[Int]] => List[Int] = commonHour
  val commonHourUDF = udf(commonHourSC)

  def getHours(hr1: Int, hr2: Int)={

    if(hr1 == null || hr2 == null) Array[Int]()
    else (hr1 to hr2).toArray
  }
  val getHoursRD :(Int,Int) => Array[Int] = getHours
  val getHoursUDF = udf(getHoursRD)


  def getSampleTCNEV1(day:String,circle:String)={
    import spark.implicits._
    val path_truecall_ne = "/data/network/truecall/ne/flatfiles/Truecall_NE"+day.replace("-","")+".csv"

    {spark.read.option("header", "true").csv(path_truecall_ne)
      .filter(substring($"ECGI", 4, 3) === lit(circle_map(circleCode)(3)))
      .withColumn("end_cell_id", concat(substring($"ECGI", 1, 6), lpad($"Cell_ID", 9, "0")))
      .withColumn("city_code", split(col("SAP_ID"), "-")(2))
      .withColumnRenamed("SAP_ID", "sap_id")
      .withColumnRenamed("EARFCN DL","earfcn_dl")
      .withColumn("band",getBandUDF($"end_cell_id"))
      .select($"end_cell_id",$"sap_id",$"eNB_Name",$"Jio_Centre",$"jc_sapid",$"R4G_State",$"enb_id",$"band",$"Sector",$"Latitude",$"Longitude",$"city")
      .withColumn("cnum", extractCellNumUDFSC(conv(substring(hex(regexp_replace(substring($"end_cell_id", 7, 9), "^0*", "").cast("int")), -2, 2), 16, 10)))
      .withColumn("ne_name", when($"sap_id".like("%ISC%")
        or $"sap_id".like("%OSC%")
        or ($"sap_id".like("%ESC%") and ($"eNB_Name".like("SMC%") or  $"eNB_Name".like("SWC%"))), concat($"sap_id",lit("-"),$"eNB_Name")
      ).otherwise($"sap_id"))
      .withColumn("sectorID",getSectorUDF($"Sector"))
      .withColumn("sectorName",
        when($"ne_name".like("%-ENB-%"), concat(substring($"ne_name",1,10),substring($"ne_name",15,10),lit("-MACRO-"),$"sectorID"))
          .when($"ne_name".like("%-ISC-%"), concat($"ne_name",lit("-ISC-"),$"sectorID"))
          .when($"ne_name".like("%-OSC-%"), concat($"ne_name",lit("-OSC-"),$"sectorID"))
          .when($"ne_name".like("%-ESC-%"), concat($"ne_name",lit("-ESC-"),$"sectorID"))
          .when($"ne_name".like("%-IBS-%"), concat($"ne_name",lit("-IBS-"),$"sectorID"))
          .otherwise(concat($"ne_name",lit("-"),$"sectorID"))
      )
      .select($"end_cell_id",$"sap_id",$"Jio_Centre",$"jc_sapid",$"R4G_State"
        ,$"enb_id",$"band",$"Latitude".as("cell_lat"),$"Longitude".as("cell_long")
        ,$"ne_name",$"cnum",$"sectorName",$"city"
      )

    }

    //df_truecall_ne.filter($"Latitude" >= latBL && $"Latitude" <= latTR && $"Longitude" >= lonBL && $"Longitude" <= lonTR)
  }

 
 
 val circle_map = Map("MU" -> List("Mumbai","network.lsr_mumbai_po", "mumbai", "874", "Mumbai")
    ,"GJ" -> List("Gujarat","network.lsr_gujarat_po", "gujarat", "857", "Gujarat")
    ,"JK" -> List("Jammu and Kashmir","network.lsr_jammu_po", "jammu", "860", "Jammu")
    ,"DL" -> List("Delhi","network.lsr_delhi_po", "delhi", "872", "Delhi")
    ,"KL" -> List("Kerala","network.lsr_kerala_po", "kerala", "862", "Kerala")
    ,"TN" -> List("Tamil Nadu","network.lsr_tamilnadu_po", "tamilnadu", "869", "TamilNadu")
    ,"WB" -> List("West Bengal","network.lsr_westbengal_po", "westbengal", "840", "WestBengal")
    ,"KA" -> List("Karnataka","network.lsr_karnataka_po", "karnataka", "861", "Karnataka")
    ,"UE" -> List("Uttar Pradesh (East)","network.lsr_up_east_po", "up_east", "871", "UP_East")
    ,"UW" -> List("Uttar Pradesh (West)","network.lsr_up_west_po", "up_west", "870", "UP_West")
    ,"PB" -> List("Punjab","network.lsr_punjab_po", "punjab", "867", "Punjab")
    ,"OR" -> List("Odisha","network.lsr_orissa_po", "orissa", "866", "Orissa")
    ,"KO" -> List("Kolkata","network.lsr_kolkata_po", "kolkata", "873", "Kolkata")
    ,"NE" -> List("North East","network.lsr_northeast_po", "northeast", "865", "NorthEast")
    ,"MP" -> List("Madhya Pradesh","network.lsr_madhyapradesh_po", "madhyapradesh", "863", "MadhyaPradesh")
    ,"MH" -> List("Maharashtra","network.lsr_maharashtra_po", "maharashtra", "864", "Maharashtra")
    ,"AP" -> List("Andhra Pradesh","network.lsr_andhrapradesh_po", "andhrapradesh", "854", "AndhraPradesh")
    ,"AS" -> List("Assam","network.lsr_assam_po", "assam", "855", "Assam")
    ,"BR" -> List("Bihar","network.lsr_bihar_po", "bihar", "856", "Bihar")
    ,"HP" -> List("Himachal Pradesh","network.lsr_hp_po", "hp", "859", "HP")
    ,"RJ" -> List("Rajasthan","network.lsr_rajasthan_po", "rajasthan", "868", "Rajasthan")
    ,"HR" -> List("Haryana","network.lsr_haryana_po", "haryana", "858", "Haryana")
  )
  
  val day = "2021-11-21"
// val hour = "11"
val circleCode  = "MU"
val circleName = circle_map(circleCode)(2)
val coaleasce_count = 256
val mnc = circle_map(circleCode)(3)
val cellThptThreshold  = 2048


val tcne_data = getSampleTCNEV1(day,"").repartition(1).persist()
// tcne_data.count()


val data=Seq(("405874068333493", "919321713856"),("405864005896869","917620603771"),("405874002914746","918451049506"),
            ("405874007589666","919082963042"),("405874005693599","918451945830"),("405874002914827","917738001719")
            ,("405874068370837","918591001313"),("405872065473797","919354935873")
            )
val rdd = spark.sparkContext.parallelize(data)
import org.apache.spark.sql.types.{StringType, StructField, StructType,LongType}
import org.apache.spark.sql.Row
val schema = StructType( Array(
                 StructField("imsi", StringType,true),
                 StructField("msisdn", StringType,true)
             ))
val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
val imsis = spark.createDataFrame(rowRDD,schema).withColumn("imsi",$"imsi".cast("long"))

// val dfFromRDD1 = dfFromRDD3.toDF("imsi","msisdn")

/*val targetImsi = {
    spark.read.format("orc").load("/data/network/lsr_agg_v1/circle=" + circleName + "/partition_date=" + day)
         .select("imsi","tot_duration")
        .groupBy("imsi")
        .agg(
            sum("tot_duration").as("tot_duration")
        )
        .filter($"tot_duration"between(3600,7200)).limit(101)
}*/


val baseEnv = {
    
    val mccmnc = "405" + mnc

    val lsmr = spark.table("network.lsmr_air_rlc_packet_po")
      .where($"partition_date" === lit(day) and $"cnum".isNotNull)
      .select($"event_time", $"qci", $"ne_id", $"ne_name", $"cnum", $"ipthruthpvodlbyte_kbytes", $"ipthruthpdltime_ms", $"partition_date")
      .filter($"qci" === "QCI9")
      //|| $"qci" === "QCI1")
      .withColumn("hour", substring($"event_time", 12, 2))
     //.filter($"hour" === lit(hour))
      .groupBy($"partition_date", $"hour", $"ne_name", $"cnum", $"ne_id")
      .agg(
        sum($"ipthruthpvodlbyte_kbytes").as("ipthruthpvodlbyte_kbytes")
        ,sum($"ipthruthpdltime_ms").as("ipthruthpdltime_ms")
      )
      .withColumn("ipthruavg_kbps", round(($"ipthruthpvodlbyte_kbytes" / $"ipthruthpdltime_ms") * 8000, 2))
      .filter($"ipthruavg_kbps" > 0)
      .select($"partition_date", $"hour", $"ne_name", $"cnum", $"ne_id", $"ipthruavg_kbps")
      .join(broadcast(tcne_data.filter($"end_cell_id".like(mccmnc + "%"))), Seq("ne_name", "cnum")).repartition(10)
    .persist
    
    val lsr = spark.read.format("orc").load("/data/network/lsr_agg_v1/circle=" + circleName + "/partition_date=" + day)
 //     .select($"partition_date", $"imsi",  $"end_cell_id", $"service_type", $"cnt_sessions", $"tot_duration", $"misc1",$"misc"
   //     , $"rsrp_data_good_duration", $"rsrp_data_average_duration", $"rsrp_data_poor_duration", $"hour",$"environment",$"voice_duration",$"confidence")
     .filter($"imsi".isNotNull)
    //.filter($"hour" === lit(hour))
    .join(imsis.select("imsi"),Seq("imsi"),"inner")
      .withColumn("misc2", split($"misc", ";")(1))
     .withColumn("misc1_split", split($"misc1",";"))
      .withColumn("env_tag", when($"environment".isin("indoor","outdoor stationary"),lit("in"))
                             .otherwise(lit("out")) 
                 )
      .coalesce(coaleasce_count).persist
    
    (lsmr,lsr)
}

{baseEnv._1.repartition(1).write.format("orc")
       .option("header","true")
       .mode("append")
       .orc("/tmp/Ridhima/lsmr_indoor_outdoor_performance")}
{baseEnv._2.repartition(1).write.format("orc")
        .option("header","true")
        .mode("append")
        .orc("/tmp/Ridhima/lsr_indoor_outdoor_performance_ALL_COLUMNS")}




val lsmr = baseEnv._1
val lsr = baseEnv._2
val summary_baseEnv = {
    lsr.filter($"environment".isin("indoor","outdoor stationary","mobile"))
        .withColumn("env", when($"environment" === "indoor",lit("IND"))
                          .when($"environment" === "outdoor stationary",lit("ODS"))
                         .when($"environment" === "mobile",lit("MOB"))
                         
                  )
        .groupBy("imsi", "end_cell_id", "partition_date","hour", "env")
      .agg(
        sum($"tot_duration").as("total_rrc_duration")
        , sum($"rsrp_data_good_duration" + $"rsrp_data_average_duration" + $"rsrp_data_poor_duration").as("total_rrc_duration_rsrp")
        , sum($"rsrp_data_poor_duration").as("bad_rsrp_rrc_duration_rsrp")
        , sum($"cnt_sessions").as("total_session")
        , sum($"voice_duration").as("voice_duration")
        , sum($"misc1_split".getItem(4)).as("voice_any_condition_duration")
       
      )
      .na.fill(0)
      .join(broadcast(lsmr), Seq("partition_date", "hour", "end_cell_id"), "left")
      .withColumn("flag_rrc_time_1m", when($"ipthruavg_kbps" < lit(1024), 1).otherwise(0))
     .withColumn("flag_rrc_time_2m", when($"ipthruavg_kbps" < lit(2024), 1).otherwise(0))
      .groupBy( "partition_date","imsi")
      .pivot("env")
      .agg(
          
        sum($"total_session").as("total_session")
        , sum($"total_rrc_duration").as("hsi_sec")
        , sum(when($"flag_rrc_time_1m" === 1, $"total_rrc_duration")).as("bad_hsi_1M_sec")
         , sum(when($"flag_rrc_time_2m" === 1, $"total_rrc_duration")).as("bad_hsi_2M_sec")
        , sum($"total_rrc_duration_rsrp").as("coverage_sec")
        , sum($"bad_rsrp_rrc_duration_rsrp").as("bad_coverage_sec")
          , sum($"voice_duration").as("voice_sec")
         , sum($"voice_any_condition_duration").as("bad_voice_sec")
        , collect_set(when($"flag_rrc_time_1m" === 1,$"hour")).as("bad_hsi_1M_hours")
        , collect_set(when($"flag_rrc_time_2m" === 1,$"hour")).as("bad_hsi_2M_hours")
        , collect_set(when($"bad_rsrp_rrc_duration_rsrp" > 0,$"hour")).as("bad_coverage_hours")
        , collect_set(when($"voice_any_condition_duration" > 0 ,$"hour")).as("bad_voice_hours")
      ).na.fill(0)
        .withColumn("dominant_env", when($"IND_hsi_sec" > $"MOB_hsi_sec" and $"IND_hsi_sec" > $"ODS_hsi_sec",lit("IND"))
                                    .when($"ODS_hsi_sec" > $"MOB_hsi_sec" and $"ODS_hsi_sec" > $"IND_hsi_sec",lit("ODS"))
                                    .when($"MOB_hsi_sec" > $"ODS_hsi_sec" and $"MOB_hsi_sec" > $"IND_hsi_sec",lit("MOB"))
                   )
      //.withColumn("bad_hsi_1M_pct", round(($"bad_hsi_1M_sec" * lit(100)) / $"hsi_sec", 1))
      //.withColumn("bad_hsi_2M_pct", round(($"bad_hsi_2M_sec" * lit(100)) / $"hsi_sec", 1))
      //.withColumn("bad_cv_pct", round(($"bad_coverage_sec" * lit(100)) / $"coverage_sec", 1))
      //.withColumn("bad_voice_pct", round(($"bad_voice_sec" * lit(100)) / $"voice_sec", 1))
      //.withColumn("dominant_env", )
      //.withColumn("out_bad_cv_pct", round(($"out_bad_coverage_sec" * lit(100)) / $"out_coverage_sec", 1))
      //.withColumn("out_bad_voice_pct", round(($"out_bad_voice_sec" * lit(100)) / $"out_voice_sec", 1))
      //.withColumn("out_bad_hsi_pct", round(($"out_bad_hsi_sec" * lit(100)) / $"out_hsi_sec", 1))
      .withColumn("circle", lit(circleName))
      
}

import org.apache.spark.sql.expressions.Window
val most_used_cell = {
    lsr.filter($"environment".isin("indoor","outdoor stationary","mobile"))
        .withColumn("env", when($"environment" === "indoor",lit("IND"))
                          .when($"environment" === "outdoor stationary",lit("ODS"))
                         .when($"environment" === "mobile",lit("MOB"))
                         
                  )
      .groupBy("imsi", "end_cell_id", "partition_date", "env")
      .agg(
        sum($"tot_duration").as("total_rrc_duration")
       
      )
    .withColumn("total_rrc_duration_", rand()+$"total_rrc_duration")
     .withColumn("rank",rank.over(Window.partitionBy("imsi","env").orderBy($"total_rrc_duration_".desc)))
      .na.fill(0).filter($"rank" === lit(1)).drop("total_rrc_duration_","rank")
     .join(broadcast(tcne_data.select("end_cell_id","ne_name","cnum","band","jc_sapid","sectorName")),Seq("end_cell_id"))
     .groupBy("imsi", "partition_date")
    .pivot("env")
    .agg(
        collect_set(concat($"ne_name",lit("-"),$"cnum",lit("["),$"band",lit("]"))).as("ne_name_cnum_band")
        ,collect_set(concat($"jc_sapid")).as("jc_sapid")
    )
}


