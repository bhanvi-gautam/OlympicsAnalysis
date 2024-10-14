from pyspark.sql import SparkSession
from data_loader import load_csv_data
from dimension_tables import create_player_dim, create_olympic_dim, create_country_dim, create_sport_dim, create_event_dim, create_medal_dim
from fact_tables import create_player_fact, create_country_fact
from reports import sport_count_yearwise, countries_participated_yearwise, top_10_countries_medals_2016, male_female_ratio_yearwise, top_3_medalists_in_swimming
from tables import save_dimension_tables, save_fact_tables
from path import source1,source2

def main():
    spark = SparkSession.builder.appName('OlympicsAnalysis').getOrCreate()
    
    # Load Data
    player_data = load_csv_data(spark, source1)
    country_data = load_csv_data(spark, source2)

    # Create Dimension Tables
    player_dim = create_player_dim( player_data)
    olympic_dim = create_olympic_dim(player_data)
    country_dim = create_country_dim(player_data)
    sport_dim = create_sport_dim(player_data)
    event_dim = create_event_dim(player_data, sport_dim)
    medal_dim = create_medal_dim(spark, player_data)

    # Create Fact Tables
    player_fact = create_player_fact(spark, player_data, player_dim, country_dim, olympic_dim, event_dim, medal_dim)
    country_fact = create_country_fact(spark, player_data, country_dim, olympic_dim)

    # Call to save dimension tables
    save_dimension_tables(player_dim, olympic_dim, country_dim, sport_dim, event_dim, medal_dim,spark)

    # Call to save fact tables
    save_fact_tables(player_fact, country_fact,spark)

    # Call each report generation function with the required DataFrames
    sport_count_yearwise(olympic_dim, player_fact,spark)
    countries_participated_yearwise(country_fact,olympic_dim,spark)
    top_10_countries_medals_2016(country_fact,spark)
    male_female_ratio_yearwise(country_fact, country_dim,spark)
    top_3_medalists_in_swimming(player_fact, event_dim, sport_dim, player_dim,spark)


if __name__ == '__main__':
    main()
