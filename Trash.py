
dct_encoder = {"Light":1,"Moderate":2,"Severe":3}
def encode(in_str):
    return(dct_encoder[in_str])


def preprocess_weather()->None:
    spark = SparkSession.builder.getOrCreate()
    df_raw = spark.read.option("header",True).csv("Data/head.csv")

    df_processed = df_raw.withColumn("daily_timestamp", pf.date_trunc("day", df_raw['StartTime(UTC)']))

    #df_processed = df_processed.groupBy("daily_timestamp").agg(pf.avg(pf.col))

    pivot_df = df_processed.groupBy("daily_timestamp").pivot("County").agg(pf.collect_list(pf.col("Type")),pf.collect_list(pf.col("Severity")), pf.avg(pf.col("Precipitation(in)")))

    #pivot_df = df_raw.groupBy("StartTime(UTC)").pivot("County").agg(pf.first(pf.col("Type","Severity")))
    pivot_df.toPandas().to_csv('Data/weather.csv')