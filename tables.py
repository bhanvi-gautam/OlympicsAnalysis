import os
from createCSV import createCSVWithPath
from path import playerDimensionPath,olympicDimensionPath,countryDimensionPath,sportDimensionPath,eventDimensionPath,medalDimensionPath,playerFactPath,countryFactPath

# Function to save dimension tables
def save_dimension_tables(player_dim, olympic_dim, country_dim, sport_dim, event_dim, medal_dim,spark):
    createCSVWithPath(playerDimensionPath, player_dim,spark)
    createCSVWithPath(olympicDimensionPath, olympic_dim,spark)
    createCSVWithPath(countryDimensionPath, country_dim,spark)
    createCSVWithPath(sportDimensionPath, sport_dim,spark)
    createCSVWithPath(eventDimensionPath, event_dim,spark)
    createCSVWithPath(medalDimensionPath, medal_dim,spark)
    print("Dimension tables saved successfully!")

# Function to save fact tables
def save_fact_tables(player_fact, country_fact,spark):
    createCSVWithPath(playerFactPath, player_fact,spark)
    createCSVWithPath(countryFactPath, country_fact,spark)
    print("Fact tables saved successfully!")
