# dimension_tables.py
from pyspark.sql import Window
from pyspark.sql.functions import rank, row_number, col
from pyspark.sql import functions as F

def create_player_dim(spark, player_data):
    player_window = Window.orderBy("Player_Name", "Player_Age", "Player_Height")
    player_dim = player_data.select("Player_Name", "Player_Gender", "Player_Age", "Player_Height") \
        .distinct() \
        .withColumn("Player_ID", rank().over(player_window)) \
        .select("Player_ID", "Player_Name", "Player_Gender", "Player_Age", "Player_Height")
    return player_dim

def create_olympic_dim(spark, player_data):
    olympic_window = Window.orderBy("Olympic_Year")
    olympic_dim = player_data.select("Olympic_Name", "Olympic_Year", "Olympic_Season", "Olympic_City") \
        .distinct() \
        .withColumn("Olympic_ID", row_number().over(olympic_window)) \
        .select("Olympic_ID", "Olympic_Name", "Olympic_Year", "Olympic_Season", "Olympic_City")
    return olympic_dim

def create_country_dim(spark, player_data, country_data):
    return player_data.select(
        col("Country_id").alias("Country Code"), 
        col("Country_Name").alias("Country Name")).distinct()

def create_sport_dim(spark, player_data):
    sport_dim = player_data.select("Sport_Name") \
        .distinct() \
        .withColumn("Sport_ID", row_number().over(Window.orderBy("Sport_Name"))) \
        .select("Sport_ID", "Sport_Name")
    return sport_dim

def create_event_dim(spark, player_data, sport_dim):
    event_dim = player_data.select("Event_Name", "Sport_Name") \
        .distinct() \
        .join(sport_dim, "Sport_Name", "inner") \
        .withColumn("Event_ID", row_number().over(Window.orderBy("Event_Name"))) \
        .select("Event_ID", "Event_Name", "Sport_ID")
    return event_dim

def create_medal_dim(spark, player_data):
    medal_df = player_data.select("Medal").distinct().filter(F.col("Medal") != "NA")
    medal_mapping = {"Gold": 1, "Silver": 2, "Bronze": 3}
    medal_mapping_df = spark.createDataFrame(medal_mapping.items(), ["Medal", "Medal_ID"])
    dim_medal = medal_df.join(medal_mapping_df, on="Medal", how="left").select("Medal_ID", "Medal")
    return dim_medal
