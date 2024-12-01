from pyspark.sql import SparkSession
from data_loader import DataLoader
from analysis import CrashAnalysis
from config import Config
import os

def create_spark_session():
    """
    Creates a SparkSession for the application.

    Returns:
        SparkSession: A configured SparkSession instance.
    """
    spark = SparkSession.builder \
        .appName("CrashAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    return spark

def load_data(config, spark):
    """
    Loads datasets using the DataLoader class.

    Args:
        config (Config): The configuration object containing file paths.
        spark (SparkSession): The active SparkSession for loading data.

    Returns:
        dict: A dictionary with loaded DataFrames.
    """
    data_loader = DataLoader(spark)
    
    data = {
        "person": data_loader.load_data(config.get_data_path("primary_person")),
        "charges": data_loader.load_data(config.get_data_path("charges")),
        "endorsements": data_loader.load_data(config.get_data_path("endorsements")),
        "restrict": data_loader.load_data(config.get_data_path("restrict")),
        "damages": data_loader.load_data(config.get_data_path("damages")),
        "unit": data_loader.load_data(config.get_data_path("unit"))
    }
    
    return data

def run_analyses(analysis, analysis_config):
    """
    Runs all analyses defined in the configuration.

    Args:
        analysis (CrashAnalysis): The CrashAnalysis object containing analysis methods.
        analysis_config (dict): A dictionary of analysis names and descriptions.

    Returns:
        dict: A dictionary with analysis names as keys and analysis results as values.
    """
    results = {}
    
    for analysis_name, analysis_info in analysis_config.items():
        print(f"Running {analysis_name}: {analysis_info['description']}")
        analysis_func = getattr(analysis, analysis_name)
        results[analysis_name] = analysis_func()
    
    return results

def save_results_to_csv(results: dict, spark: SparkSession, output_dir: str = "results"):
    """
    Save the analysis results to separate CSV files using PySpark.

    Args:
        results (dict): A dictionary of analysis names and their respective DataFrame outputs.
        spark (SparkSession): The SparkSession instance to use for saving results.
        output_dir (str): The directory where the CSV files will be saved.
    """

    for analysis_name, result in results.items():
        output_path = os.path.join(output_dir, f"{analysis_name}")
        
        if isinstance(result, int):
            df_scalar = spark.createDataFrame([(analysis_name, result)], ["Analysis", "Count"])
            df_scalar.write.csv(output_path, mode="overwrite", header=True)
        else:
            result.write.csv(output_path, mode="overwrite", header=True)
    
    print(f"Analysis results saved in the directory: {output_dir}")

def main():
    """
    Main function to execute the entire analysis pipeline.
    
    1. Creates a SparkSession.
    2. Loads data using the DataLoader class.
    3. Executes analyses as defined in the config file.
    4. Saves results to CSV files.
    """
    spark = create_spark_session()

    config = Config() 
    data = load_data(config, spark) 

    analysis = CrashAnalysis(data['person'], data['charges'], data['endorsements'], 
                             data['restrict'], data['damages'], data['unit'])

    analysis_config = config.config["analysis"]
    results = run_analyses(analysis, analysis_config)

    save_results_to_csv(results, spark)

    spark.stop()

if __name__ == "__main__":
    main()
