from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import seaborn as sns
from Silver import get_silver_data

spark = SparkSession.builder \
        .appName("GoldLayer") \
        .config("spark.sql.warehouse.dir","/Users/erdem/spark/hive_warehouse") \
        .enableHiveSupport() \
        .getOrCreate()


df_gold = get_silver_data(spark)
df_gold.show(5)

#---------------Average transfer fee---------
df_avg = df_gold.groupBy("position", "transfer_year").agg(
    avg("transfer_fee").alias("avg_fee")
)

#spark df to pandas df
pdf = df_avg.toPandas()

# Pivot: X = year, Y = avg_fee, color = position
pivot = pdf.pivot(index="transfer_year", columns="position", values="avg_fee")

#Trend position and tranfer_fee
pivot.plot(kind="line", marker="o")
plt.title("Average Transfer Fee by Position (2015–2025)")
plt.ylabel("Transfer Fee (€)")
plt.xlabel("Year")
plt.legend(title="Position", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.tight_layout()
plt.show()
#---------------Average transfer fee---------

#---------------Age transfer fee relation---------
pdf_age = df_gold.select("age_at_transfer", "position", "transfer_fee").toPandas()

# Grouping for age and positioning
df_avg_age = pdf_age.groupby(["age_at_transfer", "position"]) \
                    .agg({"transfer_fee": "mean"}) \
                    .reset_index()

plt.figure(figsize=(10,6))

#Same color map as before plot
color_map = {
    "Attack": "blue",
    "Defender": "orange",
    "Goalkeeper": "green",
    "Midfield": "red"
}
for position in df_avg_age["position"].unique():
    temp = df_avg_age[df_avg_age["position"] == position]
    plt.plot(temp["age_at_transfer"], temp["transfer_fee"],
             marker="o",
             label=position,
             color=color_map.get(position))

plt.title("Average Transfer Fee by Age and Position")
plt.xlabel("Age at Transfer")
plt.ylabel("Average Transfer Fee (€)")
plt.legend(title="Position")
plt.grid(True)
plt.tight_layout()
plt.show()
#---------------Age transfer fee relation---------


#---------------Have Transfers Delivered Success?---------

# "Without Club" cleaning
df_gold_filtered = df_gold.filter(col("to_club_name") != "Without Club")

# Get 5 clubs for transfer fee
df_top_value_clubs = df_gold_filtered.groupBy("to_club_name") \
                                     .agg(sum("transfer_fee").alias("total_fee")) \
                                     .orderBy(desc("total_fee")) \
                                     .limit(5)

# get list for pandas
top_value_club_names = [row["to_club_name"] for row in df_top_value_clubs.collect()]

df_gold_top_value_clubs = df_gold_filtered.filter(col("to_club_name").isin(top_value_club_names))

# to the pandas
pdf_top_value = df_gold_top_value_clubs.toPandas()

plt.figure(figsize=(12,6))

club_colors = {
    "Chelsea": "#034694",
    "Man City": "#6CABDD",
    "Paris SG": "#002D72",
    "Man Utd": "#DA291C",
    "Arsenal": "#EF0107"
}

sns.boxplot(
    data=pdf_top_value,
    x="to_club_name",
    y="transfer_fee",
    hue="to_club_name",
    palette=club_colors,
    legend=False
)

plt.title("Top 5 Clubs by Total Transfer Fee")
plt.xlabel("Club")
plt.ylabel("Transfer Fee (M€)")
plt.tight_layout()
plt.grid(True)
plt.ticklabel_format(style="plain", axis="y")
ax = plt.gca()
ticks = ax.get_yticks()
ax.set_yticks(ticks)
ax.set_yticklabels([f"{int(tick/1e6)}" for tick in ticks]) #for readability
plt.show()
#---------------Have Transfers Delivered Success?---------


#---------------Highest total income---------
# Identify the top 10 clubs with the highest total income from player sales
# Using df_gold_filtered for Without Club filter
df_top_selling_clubs = df_gold_filtered.groupBy("from_club_name") \
                                  .agg(sum("transfer_fee").alias("total_income")) \
                                  .orderBy(desc("total_income")) \
                                  .limit(10)

# Identify the top 10 clubs with the highest total income from player sales
# Using df_gold_filtered for Without Club filter
df_top_selling_clubs = df_gold_filtered.groupBy("from_club_name") \
                                       .agg(sum("transfer_fee").alias("total_income")) \
                                       .orderBy(desc("total_income")) \
                                       .limit(10)
#Collect the top club names
top_seller_names = [row["from_club_name"] for row in df_top_selling_clubs.collect()]

#Filter only top-selling clubs
df_outbound_top = df_top_selling_clubs.filter(col("from_club_name").isin(top_seller_names))

#Convert to Pandas
pdf_outbound = df_outbound_top.toPandas()

#Convert to millions for readability
pdf_outbound["total_income_million"] = pdf_outbound["total_income"] / 1e6

pdf_outbound = pdf_outbound.sort_values("total_income_million", ascending=False)


plt.figure(figsize=(12,6))
sns.barplot(
    data=pdf_outbound,
    x="from_club_name",
    y="total_income_million",
    hue="from_club_name",
    palette="viridis",
    legend=False
)
plt.title("Top 10 Clubs by Transfer Income (Outbound Transfers)")
plt.xlabel("Club")
plt.ylabel("Total Income (€M)")
plt.xticks(rotation=45, ha="right")
plt.grid(True)
plt.tight_layout()
plt.show()

#---------------Highest total income---------