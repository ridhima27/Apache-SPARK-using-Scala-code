import org.apache.spark.sql.expressions.Window
val today = "2021-05-12"
val yesterday = "2021-05-11"
val current_slot = "2021-05-12 08:00:00"


def getEvent(today:String,yesterday:String,current_slot:String)={
    spark.table("network.ir_lu_analysis_15_mins")
         .filter($"partition_date".isin(today,yesterday) and $"priority" =!= 0)
         .select("country","plmn_operator","imsi_circle","event_time","event_tag","event_tag_updated","roamingprocedure","failure_reason")
         .filter(unix_timestamp($"event_time") > unix_timestamp(lit(current_slot))-3600)
         .withColumn("slot15",to_timestamp(unix_timestamp($"event_time") - unix_timestamp($"event_time")%lit(900)))
         .groupBy("plmn_operator","slot15")
          .agg(count(when($"roamingprocedure" === "2","event_time")).alias("attempt_current_lucs"),
               count(when($"roamingprocedure" === "23","event_time")).alias("attempt_current_lups"),
               count(when($"roamingprocedure" === "2" and $"failure_reason".isin("FAIL_NW_MAP_SYNTAX_ERROR","OTHERS_UNKNOWN","OTHERS_UNDEFINED","FAIL_NW_UDTS_ROUTING_FAILURE","FAIL_NW_UDTS_LINK_FAILURE", "FAIL_NW_UDTS_LINK_CONGESTION","FAIL_NW_MAP_ISD_TIMEOUT","FAIL_NW_UDTS_UNRECOGNIZED_TRANSACTION","FAIL_NW_UDTS_MAP_USER_ABORT","FAIL_NW_UDTS_RETURN_ERROR"),"event_time")).alias("Total_Failure_current_lucs"),
               count(when($"roamingprocedure" === "2" and $"failure_reason".isin("FAIL_TIMEOUT_PROBE"),"event_time")).alias("TIMEOUT_current_lucs"),
               count(when($"roamingprocedure" === "23" and $"failure_reason".isin("FAIL_NW_MAP_SYNTAX_ERROR","OTHERS_UNKNOWN","OTHERS_UNDEFINED","FAIL_NW_UDTS_ROUTING_FAILURE","FAIL_NW_UDTS_LINK_FAILURE", "FAIL_NW_UDTS_LINK_CONGESTION","FAIL_NW_MAP_ISD_TIMEOUT","FAIL_NW_UDTS_UNRECOGNIZED_TRANSACTION","FAIL_NW_UDTS_MAP_USER_ABORT","FAIL_NW_UDTS_RETURN_ERROR"),"event_time")).alias("Total_Failure_current_lups"),
               count(when($"roamingprocedure" === "23" and $"failure_reason".isin("FAIL_TIMEOUT_PROBE"),"event_time")).alias("TIMEOUT_current_lups")

              )
            .withColumn("success_Rate_current_lucs",round((lit(1)-($"Total_Failure_current_lucs")/($"attempt_current_lucs" - $"TIMEOUT_current_lucs"))*lit(100),2))
            .withColumn("success_Rate_current_lups",round((lit(1)-($"Total_Failure_current_lups")/($"attempt_current_lups" - $"TIMEOUT_current_lups"))*lit(100),2))
          .withColumn("iu_cs_attempts_m1",lag("attempt_current_lucs",1).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("iu_cs_attempts_m2",lag("attempt_current_lucs",2).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("iu_cs_attempts_m3",lag("attempt_current_lucs",3).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("iu_ps_attempts_m1",lag("attempt_current_lups",1).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("iu_ps_attempts_m2",lag("attempt_current_lups",2).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("iu_ps_attempts_m3",lag("attempt_current_lups",3).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("sr_cs_m1",lag("success_Rate_current_lucs",1).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("sr_cs_m2",lag("success_Rate_current_lucs",2).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("sr_cs_m3",lag("success_Rate_current_lucs",3).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("sr_ps_m1",lag("success_Rate_current_lups",1).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("sr_ps_m2",lag("success_Rate_current_lups",2).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
          .withColumn("sr_ps_m3",lag("success_Rate_current_lups",3).over(Window.partitionBy("plmn_operator").orderBy("slot15")))
            .filter($"slot15" === lit(current_slot))
          .withColumn("isIuCSaffected", when( ($"iu_cs_attempts_m1"-$"attempt_current_lucs")/$"attempt_current_lucs" > 0.1
                                              and ($"iu_cs_attempts_m2"-$"attempt_current_lucs")/$"attempt_current_lucs" > 0.1
                                              and ($"iu_cs_attempts_m3"-$"attempt_current_lucs")/$"attempt_current_lucs" > 0.1
                                              and $"attempt_current_lucs" > 50
                                              and $"iu_cs_attempts_m1" > 50
                                              and $"iu_cs_attempts_m2" > 50
                                              and $"iu_cs_attempts_m2" > 50
                                              ,"Y").otherwise("N")
                     )
          .withColumn("isIuPSaffected", when( ($"iu_ps_attempts_m1"-$"attempt_current_lups")/$"attempt_current_lups" > 0.1
                                              and ($"iu_ps_attempts_m2"-$"attempt_current_lups")/$"attempt_current_lups" > 0.1
                                              and ($"iu_ps_attempts_m3"-$"attempt_current_lups")/$"attempt_current_lups" > 0.1
                                              and $"attempt_current_lups" > 50
                                              and $"iu_ps_attempts_m1" > 50
                                              and $"iu_ps_attempts_m2" > 50
                                              and $"iu_ps_attempts_m2" > 50
                                              ,"Y").otherwise("N")
                     )
         .withColumn("isCSSRaffected", when( ($"sr_cs_m1"-$"success_Rate_current_lucs")/$"success_Rate_current_lucs" > 0.05
                                              and ($"sr_cs_m2"-$"success_Rate_current_lucs")/$"success_Rate_current_lucs" > 0.05
                                              and ($"sr_cs_m3"-$"success_Rate_current_lucs")/$"success_Rate_current_lucs" > 0.05
                                              and $"attempt_current_lucs" > 50
                                              and $"iu_cs_attempts_m1" > 50
                                              and $"iu_cs_attempts_m2" > 50
                                              and $"iu_cs_attempts_m2" > 50
                                              ,"Y").otherwise("N")
                     )
        .withColumn("isPSSRaffected", when( ($"sr_ps_m1"-$"success_Rate_current_lups")/$"success_Rate_current_lups" > 0.05
                                              and ($"sr_ps_m2"-$"success_Rate_current_lups")/$"success_Rate_current_lups" > 0.05
                                              and ($"sr_ps_m3"-$"success_Rate_current_lups")/$"success_Rate_current_lups" > 0.05
                                             and $"attempt_current_lups" > 50
                                              and $"iu_ps_attempts_m1" > 50
                                              and $"iu_ps_attempts_m2" > 50
                                              and $"iu_ps_attempts_m2" > 50
                                              ,"Y").otherwise("N")
                     )
        .withColumn("isIuCSzero", when(      $"attempt_current_lucs" === 0
                                              and $"iu_cs_attempts_m1" > 0
                                              and $"iu_cs_attempts_m2" > 0
                                              and $"iu_cs_attempts_m2" > 0
                                              ,"Y").otherwise("N")
                     )
         .withColumn("isIuPSzero", when(      $"attempt_current_lups" === 0
                                              and $"iu_ps_attempts_m1" > 0
                                              and $"iu_ps_attempts_m2" > 0
                                              and $"iu_ps_attempts_m2" > 0
                                              ,"Y").otherwise("N")
                     )
         .withColumn("isCSSRzero", when(      $"success_Rate_current_lucs" === 0
                                              and $"sr_cs_m1" > 0
                                              and $"sr_cs_m1" > 0
                                              and $"sr_cs_m1" > 0
                                              ,"Y").otherwise("N")
                     )
         .withColumn("isPSSRzero", when(      $"success_Rate_current_lups" === 0
                                              and $"sr_ps_m1" > 0
                                              and $"sr_ps_m1" > 0
                                              and $"sr_ps_m1" > 0
                                              ,"Y").otherwise("N")
                     )
   .orderBy("plmn_operator","slot15")   /*  */       

        
}
