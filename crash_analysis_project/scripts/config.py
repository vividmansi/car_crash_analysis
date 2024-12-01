import yaml

class Config:
    """
    Config class to handle the loading and retrieval of configuration data from a YAML file.

    This class is responsible for:
    - Loading the configuration file.
    - Retrieving file paths for input datasets.
    - Retrieving analysis configurations for different analyses defined in the config file.
    """
    
    def __init__(self, config_path="configs/config.yaml"):
        """
        Initializes the Config object and loads the configuration data from the provided YAML file.

        Args:
            config_path (str): Path to the YAML configuration file. Default is "configs/config.yaml".
        """
        self.config = self.load_config(config_path)

    def load_config(self, config_path):
        """
        Loads the YAML configuration file.

        Args:
            config_path (str): Path to the YAML file to be loaded.

        Returns:
            dict: The configuration data loaded from the YAML file.
        """
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def get_data_path(self, dataset_name):
        """
        Retrieves the file path for a specific dataset defined in the configuration.

        Args:
            dataset_name (str): The name of the dataset (e.g., 'primary_person', 'charges').

        Returns:
            str: The file path to the dataset as defined in the config file.
        """
        return self.config['input_data'].get(dataset_name)

    def get_all_data_paths(self):
        """
        Retrieves all the dataset file paths defined in the configuration.

        Returns:
            dict: A dictionary containing all the dataset file paths.
        """
        return self.config.get('input_data', {})

    def get_analysis(self, analysis_name: str) -> dict:
        """
        Retrieves the configuration details for a specific analysis.

        Args:
            analysis_name (str): The name of the analysis (e.g., 'crash_analysis_1').

        Returns:
            dict: The configuration details for the analysis.
        """
        return self.config["analysis"].get(analysis_name)

