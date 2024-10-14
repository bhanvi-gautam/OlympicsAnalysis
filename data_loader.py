# data_loader.py
def load_csv_data(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)
