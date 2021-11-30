import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

//raw data
val base_df={
    
    spark.table("network.lsr_mumbai_po").filter($"partition_date" ==="2021-11-22" and $"imsi"===lit("405874005701923"))
    .select("confidence","imsi","environment","end_latitude","end_longitude","partition_date","s1_u_upload_data_volume","s1_u_download_data_volume"
           ,"last_cqi","rsrp"
           )
    .withColumn("usage",round(($"s1_u_upload_data_volume" + $"s1_u_download_data_volume")/(lit(1024)*lit(1024)),2))
    .filter($"usage".isNotNull and $"end_latitude".isNotNull and $"end_longitude".isNotNull and $"last_cqi".isNotNull
            and $"rsrp".isNotNull )
//     .show(false)
//     filter(
//  last cqi and rsrp rrc durtion lat long   )
}

//creating feature vectors
val cols = Array("end_latitude", "end_longitude","usage","last_cqi","rsrp")
// merging both features into one  
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featureDf = assembler.transform(base_df)


//creating model with cluster size=8 and saving it
val kmeans ={ new KMeans()
    .setK(8)
    .setFeaturesCol("features")}
     .setPredictionCol("prediction")}
  val kmeansModel = kmeans.fit(featureDf)
// save model
  kmeansModel.write.overwrite()
    .save("/tmp/Ridhima/ml_models/kmeans_8clusters_cqi_usage_rsrp_usage")
val clusters=kmeansModel.clusterCenters
clusters.foreach(println)

// usgae lat long usage rsrp cqi k=8
// [19.13884764377683,72.82635479828322,0.14596566523605153,10.472103004291846,-99.83690987124463]
// [19.13856,72.82686949999999,81.38,9.5,-96.0]
// [19.139058454545463,72.8262865909091,0.1872727272727272,11.090909090909092,-85.54545454545455]
// [19.13900878703704,72.82631230555552,0.19712962962962965,4.314814814814815,-99.75925925925925]
// [19.139419666666665,72.82599616666667,42.154999999999994,7.333333333333333,-101.0]
// [19.138597128617345,72.82657139871377,0.17987138263665584,9.797427652733118,-104.81350482315112]
// [19.138981100000002,72.82633025,14.520500000000002,8.9,-101.65]
// [19.13894435211269,72.82636243661975,0.21626760563380287,10.056338028169014,-95.16197183098592]




//prediction accuracy 

val predictions = kmeansModel.transform(featureDf)

// Evaluate clustering by computing Silhouette score
val evaluator = new ClusteringEvaluator()

val silhouette = evaluator.evaluate(predictions)
println("Silhouette with squared euclidean distance ="+silhouette)

// Silhouette with squared euclidean distance =0.475386782734979 new params k-8

// k=8, Silhouette with squared euclidean distance =0.7911082383523574



