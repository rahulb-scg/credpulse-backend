import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd

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
        else:
            raise ValueError(f"Configuration file not found: {config_file_path}")
        
        # Process input file based on type
        df = file_type_handler(config_file_path if data_file_path is None else data_file_path)
        if df is None:
            raise ValueError("Failed to process input file")
        
        logging.info(f"Successfully loaded data with {len(df)} rows")

        # Preprocess data
        preprocessed_data = preprocess(df, data_config)
        logging.info("Data preprocessing completed")

        # Run model with configuration
        results = tmm1.run_model(preprocessed_data, data_config)
        logging.info("Model execution completed")

        # Export results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = os.path.join(
            config.output['dir_model_ready'],
            f"output_{timestamp}"
        )
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Prepare data for export
        export_data = pd.DataFrame({
            'metric': ['ALLL', 'CECL_Factor', 'WAL', 'CECL_Amount', 'Opening_Balance', 
                      'Ending_Balance', 'Origination_Amount', 'Snapshot_Date', 
                      'Forecasted_Months', 'Forecasted_Period_From', 'Forecasted_Period_To'],
            'value': [
                results['ALLL'],
                results['CECL_Factor'],
                results['WAL'],
                results['CECL_Amount'],
                results['Opening_Balance'],
                results['Ending_Balance'],
                results['Origination_Amount'],
                results['Snapshot_Date'],
                results['Forecasted_Months'],
                results['Forecasted_Period_From'],
                results['Forecasted_Period_To']
            ]
        })
        
        # Export results
        output_path = os.path.join(output_dir, config.output['file_base'])
        export_output(export_data, output_path)
        logging.info(f"Results exported to {output_path}")

        # Save transition matrix and CGL curve
        transition_matrix_path = os.path.join(output_dir, 'transition_matrix.csv')
        cgl_curve_path = os.path.join(output_dir, 'cgl_curve.csv')
        
        results['Transition_Matrix'].to_csv(transition_matrix_path)
        results['CGL_Curve'].to_csv(cgl_curve_path)
        
        logging.info("Additional model outputs saved")

        return {
            'status': 'success',
            'timestamp': timestamp,
            'output_path': output_path,
            'summary': {
                'ALLL': float(results['ALLL']),
                'CECL_Factor': float(results['CECL_Factor']),
                'CECL_Amount': float(results['CECL_Amount']),
                'Opening_Balance': float(results['Opening_Balance']),
                'Ending_Balance': float(results['Ending_Balance'])
            },
            'metrics': {
                'transition_matrix_path': transition_matrix_path,
                'cgl_curve_path': cgl_curve_path,
                'forecasted_period': {
                    'from': results['Forecasted_Period_From'],
                    'to': results['Forecasted_Period_To']
                }
            }
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

