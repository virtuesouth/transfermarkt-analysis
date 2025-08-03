from pyspark.sql import SparkSession
from Bronze import get_df # importing bronze layer
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
        .appName("SilverLayer") \
        .config("spark.sql.warehouse.dir","/Users/erdem/spark/hive_warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

#Getting Raw data
df_player_raw,df_transfer_10= get_df(spark)


def get_silver_data(spark):
        # df_transfer_10.show(5)
        # df_player_raw.show(5)

        #except player_id and player_name
        t1_cols = [ col(f"t1.{c}") for c in df_transfer_10.columns if c not in["player_id","player_name"] ]

        #alias df_transfer_10 AS t1 , df_player AS t2
        #join with player_id
        df_join = (df_transfer_10.alias("t1").join(df_player_raw.alias("t2"), col("t1.player_id") == col("t2.player_id"), how="left")
                  .select("t2.player_id",
                          "t2.name",
                          to_date("t2.date_of_birth").alias("date_of_birth"),
                          "t2.country_of_birth",
                          "t2.position",
                          "t2.sub_position",
                          *t1_cols
                         )
                  )

        df_join = df_join.withColumn("transfer_year",year("transfer_date"))

        # #calculated column for transfer age
        df_join_age = df_join.withColumn("age_at_transfer",(datediff("transfer_date","date_of_birth")/ 365.25).cast("int"))

        # #filtering missing position records for further analysis.
        df_join_age =df_join_age.filter(col("position") != "Missing")

        return df_join_age





# for test
if __name__ == "__main__":
    df_ready = get_silver_data(spark)
    df_ready.show(10)
    # df_ready.filter(col("position") == "Missing").show(20)
