import os
from pyspark.sql import functions as F
from createCSV import createCSVWithPath
from path import firstReport,secondReport,thirdReport,fifthReport,fourthReport

# 1st Report: Sport Count Year-Wise
def sport_count_yearwise(olympic_dim, player_fact_grouped,spark):
    result = olympic_dim \
        .join(player_fact_grouped, olympic_dim["Olympic_ID"] == player_fact_grouped["Olympic_ID"], "left") \
        .groupBy("Olympic_Year", "Olympic_Name") \
        .agg(F.countDistinct("Sport_ID").alias("Count")) \
        .orderBy(F.col("Olympic_Year").desc())
    
    result = result.select(
        F.col("Olympic_Year").alias("year"),
        F.col("Olympic_Name"),
        F.col("Count")
    )
    createCSVWithPath(firstReport, result,spark)

# 2nd Report: Countries Participated Year-Wise
def countries_participated_yearwise(country_fact,olympic_dim,spark):
    result = country_fact.groupBy("Olympic_Year") \
        .agg(F.countDistinct("Country_Code").alias("country_count")) \
        .select(F.col("Olympic_Year").alias("year"), "country_count") \
        .orderBy(F.col("Olympic_Year").desc())
    result = result \
             .join(olympic_dim,result["year"]==olympic_dim["Olympic_Year"],"left")
    result = result.orderBy(F.col("year").desc()).select("year","Olympic_Name",F.col("country_count").alias("country count"))
    createCSVWithPath(secondReport, result,spark)

# 3rd Report: Top 10 Countries in Medals in 2016
def top_10_countries_medals_2016(country_fact,spark):
    country_2016 = country_fact.filter(F.col("Olympic_Year") == 2016)
    medals_df = country_2016.select(
        "Country_Name",
        "Total_No_Of_Gold_Medals",
        "Total_No_Of_Silver_Medals",
        "Total_No_Of_Bronze_Medals"
    )
    medals_df = medals_df.withColumn(
        "Total_No_Of_Medals",
        F.col("Total_No_Of_Gold_Medals") + F.col("Total_No_Of_Silver_Medals") + F.col("Total_No_Of_Bronze_Medals")
    )
    top_10_countries = medals_df.orderBy(F.col("Total_No_Of_Medals").desc()).limit(10)
    medals_long_df = top_10_countries.select(
        "Country_Name",
        F.expr("stack(3, 'Gold', Total_No_Of_Gold_Medals, 'Silver', Total_No_Of_Silver_Medals, 'Bronze', Total_No_Of_Bronze_Medals) as (Medal, Medal_Count)")
    )
    medals_long_df = medals_long_df.filter(F.col("Medal_Count") > 0)

    createCSVWithPath(thirdReport, medals_long_df,spark)

# 4th Report: Male to Female Ratio Year-Wise
def male_female_ratio_yearwise(country_fact, country_dim,spark):
    male_female_ratio = country_fact.groupBy("Olympic_Year", "Country_Code") \
        .agg(
            F.sum("No_Of_Men_Players_Participated").alias("Total_Men"),
            F.sum("No_Of_Women_Players_Participated").alias("Total_Women")
        ) \
        .withColumn("Male_Female_Ratio",
                    F.when(F.col("Total_Women") != 0,
                           F.col("Total_Men") / F.col("Total_Women"))
                    .otherwise(0)) \
        .withColumn("Male_Female_Ratio", F.round("Male_Female_Ratio", 2))
    
    result = male_female_ratio.join(country_dim, male_female_ratio["Country_Code"] == country_dim["Country Code"], "inner")
    final_result = result.select("Olympic_Year", "Country Name", "Male_Female_Ratio")
    france_result = final_result.filter(F.col("Country Name") == "France")
    france_result = france_result.orderBy("Olympic_Year")
    createCSVWithPath(fourthReport, france_result,spark)

# 5th Report: Top 3 Medalists in Swimming
def top_3_medalists_in_swimming(player_fact_grouped, event_dim, sport_dim, player_dim,spark):
    swim = player_fact_grouped \
        .join(event_dim, "Event_ID") \
        .join(sport_dim, "Sport_ID") \
        .groupBy("Player_ID", "Sport_Name") \
        .agg(F.sum(F.when(F.col("Medal_ID").isNotNull(), 1).otherwise(0)).alias("Total_No_Of_Medals"))
    
    swim = swim.filter(F.col("Sport_Name") == 'Swimming')
    swim = swim.join(player_dim, "Player_ID") \
        .select("Sport_Name", "Player_Name", "Total_No_Of_Medals") \
        .groupBy("Player_Name", "Sport_Name") \
        .agg(F.sum("Total_No_Of_Medals").alias("Total_No_Of_Medals")) \
        .orderBy(F.col("Total_No_Of_Medals").desc()) \
        .limit(3)
    
    createCSVWithPath(fifthReport, swim,spark)
