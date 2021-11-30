val iotHSS = "/tmp/Ridhima/Wideband_IoT_crosstab_7-6-2021-MSISDN_formatted.csv"
val iot_hss = 
{
    spark.read.format("csv").option("header","true").load(iotHSS).repartition(1)
}

val circle_map = Map(
    "MU" -> List("Mumbai","network.lsr_mumbai_po", "Mumbai")
  ,"GJ" -> List("Gujarat","network.lsr_gujarat_po", "Gujarat")
  ,"JK" -> List("Jammu and Kashmir","network.lsr_jammu_po", "Jammu")
  ,"DL" -> List("Delhi","network.lsr_delhi_po", "Delhi")
  ,"KL" -> List("Kerala","network.lsr_kerala_po", "Kerala")
  "TN" -> List("Tamil Nadu","network.lsr_tamilnadu_po", "TamilNadu")
  ,"WB" -> List("West Bengal","network.lsr_westbengal_po", "WestBengal")
  ,"KA" -> List("Karnataka","network.lsr_karnataka_po", "Karnataka")
  ,"UE" -> List("Uttar Pradesh (East)","network.lsr_up_east_po", "UP_East")
  ,"UW" -> List("Uttar Pradesh (West)","network.lsr_up_west_po", "UP_West")
  "PB" -> List("Punjab","network.lsr_punjab_po", "Punjab")
  ,"OR" -> List("Odisha","network.lsr_orissa_po", "Orissa")
  ,"KO" -> List("Kolkata","network.lsr_kolkata_po", "Kolkata")
  ,"NE" -> List("North East","network.lsr_northeast_po", "NorthEast")
  ,"MP" -> List("Madhya Pradesh","network.lsr_madhyapradesh_po", "MadhyaPradesh")
  ,"MH" -> List("Maharashtra","network.lsr_maharashtra_po", "Maharashtra")
  ,"AP" -> List("Andhra Pradesh","network.lsr_andhrapradesh_po", "AndhraPradesh")
  ,"AS" -> List("Assam","network.lsr_assam_po", "Assam")
  ,"BR" -> List("Bihar","network.lsr_bihar_po", "Bihar")
  ,"HP" -> List("Himachal Pradesh","network.lsr_hp_po", "HP")
  ,"RJ" -> List("Rajasthan","network.lsr_rajasthan_po", "Rajasthan")
  ,"HR" -> List("Haryana","network.lsr_haryana_po", "Haryana")
)

val circleList = circle_map.keys.toList


import java.util.Calendar
var tic = Calendar.getInstance.getTime()

import org.apache.spark.sql.DataFrame

def storeSummary(circleName:String, day:String,output:String, iot_hss:DataFrame)={
    
    println("Started processing "+circleName+" @ " + Calendar.getInstance.getTime() )
    
    val circle_map = Map("MU" -> List("Mumbai","network.lsr_mumbai_po", "Mumbai")
  ,"GJ" -> List("Gujarat","network.lsr_gujarat_po", "Gujarat")
  ,"JK" -> List("Jammu and Kashmir","network.lsr_jammu_po", "Jammu")
  ,"DL" -> List("Delhi","network.lsr_delhi_po", "Delhi")
  ,"KL" -> List("Kerala","network.lsr_kerala_po", "Kerala")
  ,"TN" -> List("Tamil Nadu","network.lsr_tamilnadu_po", "TamilNadu")
  ,"WB" -> List("West Bengal","network.lsr_westbengal_po", "WestBengal")
  ,"KA" -> List("Karnataka","network.lsr_karnataka_po", "Karnataka")
  ,"UE" -> List("Uttar Pradesh (East)","network.lsr_up_east_po", "UP_East")
  ,"UW" -> List("Uttar Pradesh (West)","network.lsr_up_west_po", "UP_West")
  ,"PB" -> List("Punjab","network.lsr_punjab_po", "Punjab")
  ,"OR" -> List("Odisha","network.lsr_orissa_po", "Orissa")
  ,"KO" -> List("Kolkata","network.lsr_kolkata_po", "Kolkata")
  ,"NE" -> List("North East","network.lsr_northeast_po", "NorthEast")
  ,"MP" -> List("Madhya Pradesh","network.lsr_madhyapradesh_po", "MadhyaPradesh")
  ,"MH" -> List("Maharashtra","network.lsr_maharashtra_po", "Maharashtra")
  ,"AP" -> List("Andhra Pradesh","network.lsr_andhrapradesh_po", "AndhraPradesh")
  ,"AS" -> List("Assam","network.lsr_assam_po", "Assam")
  ,"BR" -> List("Bihar","network.lsr_bihar_po", "Bihar")
  ,"HP" -> List("Himachal Pradesh","network.lsr_hp_po", "HP")
  ,"RJ" -> List("Rajasthan","network.lsr_rajasthan_po", "Rajasthan")
  ,"HR" -> List("Haryana","network.lsr_haryana_po", "Haryana")
)

    
    {
        spark.table(circle_map.getOrElse(circleName,List("","",""))(1))
         .where($"partition_date" === lit(day))
         .select($"imsi", $"s1_u_upload_data_volume", $"s1_u_download_data_volume")
        .filter($"imsi".isNotNull && $"s1_u_upload_data_volume".isNotNull && $"s1_u_download_data_volume".isNotNull)
         .join(iot_hss,Seq("IMSI"))
         .groupBy("imsi")
        .agg(
         sum($"s1_u_upload_data_volume" + $"s1_u_download_data_volume").as("total_usage_bytes")
        )
          .withColumn("circle_lsr",lit(circleName))
          .withColumn("day",lit(day))
          .repartition(1)
          .write.format("orc")
          .option("header","true")
          .mode("append")
          .orc("/user/Ridhima.Akshantulu/ISC_issues")
         
    }
    
     println("Completed processing "+circleName+" @ " + Calendar.getInstance.getTime() )
    
    
}


for(circle <- circleList){
    storeSummary(circle,"2021-06-07","network_dev.ridhima_WBIoT_device_analysis", iot_hss)
}

{
    spark.table("network_dev.ridhima_IoT_device_analysis")
        .filter($"day" === "2021-06-07")
        .withColumn("total_usage_bytes_mb", col("total_usage_bytes")/lit(1024*1024.0))
//         .withColumn("flag_mb",floor($"total_usage_bytes_mb"))
         .groupBy("circle_lsr")
//          .pivot("day")
         .agg(
             countDistinct($"imsi").as("cnt_imsi"),
             sum($"total_usage_bytes"/(1024*1024*1024.0)).as("consumption_volume_gb"),
             count(when($"total_usage_bytes_mb" > lit(0) && $"total_usage_bytes_mb"<= lit(1),1)).alias("range_0-1"),
            count(when($"total_usage_bytes_mb" > lit(1) && $"total_usage_bytes_mb"<= lit(2),1)).alias("range_1-2"),
            count(when($"total_usage_bytes_mb" > lit(2) && $"total_usage_bytes_mb"<= lit(3),1)).alias("range_2-3"),
            count(when($"total_usage_bytes_mb" > lit(3) && $"total_usage_bytes_mb"<= lit(4),1)).alias("range_3-4"),
            count(when($"total_usage_bytes_mb" > lit(4) && $"total_usage_bytes_mb"<= lit(5),1)).alias("range_4-5"),
            count(when($"total_usage_bytes_mb" > lit(5),1)).alias("range_more_than_5_mb")
//               min($"total_usage_bytes").as("total_usage_bytes_min"),
//              max($"total_usage_bytes").as("total_usage_bytes_max")
             
         )
        .orderBy($"circle_lsr")
         
}.show(1000)


