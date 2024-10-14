from pyspark.sql import Window
from pyspark.sql.functions import rank, row_number, col, first
from pyspark.sql import functions as F

def create_player_fact(spark, player_data, player_dim, country_dim, olympic_dim, event_dim, dim_medal):
    player_fact = player_data \
        .join(player_dim, ["Player_Name", "Player_Age", "Player_Height"], "inner") \
        .join(country_dim, player_data["Country_id"] == country_dim["Country Code"], "inner") \
        .join(olympic_dim, "Olympic_Year", "inner") \
        .join(event_dim, "Event_Name", "inner") \
        .join(dim_medal, "Medal", "left")

    player_fact = player_fact.withColumn("BMI", player_data["Player_Weight"] / (player_data["Player_Height"] ** 2))

    player_fact_grouped = player_fact.groupBy("Player_ID", "Olympic_ID", "Event_ID", "Sport_ID", "Medal_ID") \
        .agg(
            first("Player_Height").alias("Player_Height"),
            first("Player_Weight").alias("Player_Weight"),
            first("Country Code").alias("Country_Code"),
            first("Olympic_ID").alias("Olympic_ID1"),
            first("Event_ID").alias("Event_ID1"),
            first("Sport_ID").alias("Sport_ID1"),
            first("Medal_ID").alias("Medal_ID1"),
            first("BMI").alias("BMI")
        )

    window_spec = Window.orderBy("Player_ID")
    player_fact_grouped = player_fact_grouped.withColumn("Player_Fact_ID", row_number().over(window_spec))

    return player_fact_grouped.select(
        "Player_Fact_ID", "Player_ID", "Player_Height", "Player_Weight", "Country_Code",
        col("Olympic_ID1").alias("Olympic_ID"),
        col("Event_ID1").alias("Event_ID"),
        col("Sport_ID1").alias("Sport_ID"),
        col("Medal_ID1").alias("Medal_ID"), "BMI"
    )

def create_country_fact(spark, playerData, country_dim, olympic_dim):
    # Your logic to join and create the country fact table
    country_fact = playerData \
    .join(country_dim, playerData["Country_Name"] == country_dim["Country Name"], "inner") \
    .join(olympic_dim, "Olympic_Year", "left")

    # Group by country and year to calculate aggregates including medal count
    country_fact = country_fact.groupBy("Country_Name", "Country Code", "Olympic_Year", "Olympic_ID") \
        .agg(
            F.count("Player_Name").alias("Total_No_Of_Players_Participated"),
            F.sum(F.when(F.col("Player_Gender") == "M", 1).otherwise(0)).alias("No_Of_Men_Players_Participated"),
            F.sum(F.when(F.col("Player_Gender") == "F", 1).otherwise(0)).alias("No_Of_Women_Players_Participated"),
            F.count(F.when((F.col("Medal").isNotNull()) & (F.col("Medal") != "NA"), 1)).alias("Total_No_Of_Medals"),  # Total number of medals
            F.count(F.when(F.col("Medal") == "Gold", 1)).alias("Total_No_Of_Gold_Medals"),  # Gold medal count
            F.count(F.when(F.col("Medal") == "Silver", 1)).alias("Total_No_Of_Silver_Medals"),  # Silver medal count
            F.count(F.when(F.col("Medal") == "Bronze", 1)).alias("Total_No_Of_Bronze_Medals"),  # Bronze medal count
            F.count(F.when((F.col("Medal").isNotNull()) & (F.col("Medal") != "NA"), 1)).alias("No_Of_Players_Won")  # Players who won any medal
        ) \
        .withColumn("No_Of_Players_Lose",
                    F.col("Total_No_Of_Players_Participated") - F.col("No_Of_Players_Won"))  # Calculate players who didn't win a medal
    
    # Calculate Male to Female Ratio, ensuring no division by zero
    country_fact = country_fact.withColumn("Male_Female_Ratio",
                    F.when(F.col("No_Of_Women_Players_Participated") != 0,
                        F.col("No_Of_Men_Players_Participated") / F.col("No_Of_Women_Players_Participated"))
                    .otherwise(0))  # Avoid division by zero
    
    country_fact = country_fact.withColumn("Country_Fact_ID", F.row_number().over(Window.orderBy("Country Code")))
            
    # Ensure the correct column names are used in the select statement
    return country_fact.select(
 
        F.col("Country Code").alias("Country_Code"),
        "Country_Name",
        "Olympic_ID",
        "Olympic_Year",
        "Total_No_Of_Medals",
        "Total_No_Of_Gold_Medals",
        "Total_No_Of_Silver_Medals",
        "Total_No_Of_Bronze_Medals",
        "Total_No_Of_Players_Participated",
        "No_Of_Women_Players_Participated",
        "No_Of_Men_Players_Participated",
        "Male_Female_Ratio",
        "No_Of_Players_Won",
        "No_Of_Players_Lose"
    )
