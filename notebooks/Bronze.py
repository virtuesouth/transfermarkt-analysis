from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *



# spark = SparkSession.builder \
#         .appName("BronzeLayer") \
#         .config("spark.sql.warehouse.dir","/Users/erdem/spark/hive_warehouse") \
#         .getOrCreate()

def get_df(spark):
        #get player info
        df_player_raw = spark.read \
                .format("csv") \
                .option("header",True) \
                .option("inferSchema",True) \
                .load('/Users/erdem/Documents/Spark Veri Setleri/TransferMarkt/players.csv')

        # df_player_raw.show(5)
        # df_player_raw.printSchema()

        #get transfer info
        df_transfer_raw = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load('/Users/erdem/Documents/Spark Veri Setleri/TransferMarkt/transfers.csv')

        # df_transfer_raw.show(5)
        # df_transfer_raw.printSchema()

        ## last 10 year will analyse
        df_transfer_10 = df_transfer_raw.filter((year(col("transfer_date")) >= 2015) & (year(col("transfer_date")) <= 2025 ))

        # df_transfer_10.select(year("transfer_date")).distinct().orderBy("transfer_date").show(10)
        #
        # print(df_transfer_10.count())

        return df_player_raw,df_transfer_10