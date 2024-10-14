import os
from data_loader import load_csv_data

def createCSVWithPath(filePath, df, spark):
    # Ensure the directory exists
    directory = os.path.dirname(filePath)
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    if not os.path.exists(filePath):
        # Save the DataFrame as CSV if the file does not exist
        df.toPandas().to_csv(filePath, index=False)
    else:
        # Load the existing DataFrame
        existingDF = load_csv_data(spark, filePath)
        
        # Compare DataFrames and overwrite if they are not equal
        if existingDF.collect() == df.collect():
            print(f"{filePath} already exists and is identical. Skipping write operation.")
        else:
            print(f"{filePath} differs. Overwriting with new data.")
            df.toPandas().to_csv(filePath, index=False)
