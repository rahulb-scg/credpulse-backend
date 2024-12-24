import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from backend.config import config
from backend.utils import (
    get_absolute_filepath,
    file_type_handler,
    export_output
)
from backend.data_handler.preprocessor import preprocess
from backend.models import tmm1

def main(config_file_path: str, data_file_path: Optional[str] = None, config_type: str = 'db') -> Dict[str, Any]:
    """
    Main function to process data and generate reports.
    
    Args:
        config_file_path: Path to the configuration file
        data_file_path: Optional path to data file (for CSV/Excel sources)
        config_type: Type of configuration ('db' or 'csv')
    
    Returns:
        Dict containing processed data and analysis results
    """
    logging.info("Starting data processing pipeline")
    logging.debug(f"Config file: {config_file_path}, Data file: {data_file_path}, Type: {config_type}")

    try:
        # Load configuration if config file exists
        data_config = None
        if os.path.exists(config_file_path):
            with open(config_file_path, 'r') as f:
                data_config = json.load(f)
            logging.info("Configuration file loaded successfully")
        
        # Process input file based on type
        df = file_type_handler(config_file_path if data_file_path is None else data_file_path)
        if df is None:
            raise ValueError("Failed to process input file")
        
        logging.info(f"Successfully loaded data with {len(df)} rows")

        # Preprocess data
        preprocessed_data = preprocess(df, data_config)
        logging.info("Data preprocessing completed")

        # Run model
        results = tmm1.run_model(preprocessed_data)
        logging.info("Model execution completed")

        # Export results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = os.path.join(
            config.output['dir_model_ready'],
            f"output_{timestamp}"
        )
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        output_path = os.path.join(output_dir, config.output['file_base'])
        export_output(results['data'], output_path)
        logging.info(f"Results exported to {output_path}")

        return {
            'status': 'success',
            'timestamp': timestamp,
            'output_path': output_path,
            'summary': results.get('summary', {}),
            'metrics': results.get('metrics', {})
        }

    except Exception as e:
        logging.error(f"Error in main processing pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example usage
    config_file = "path/to/config.json"
    result = main(config_file)

