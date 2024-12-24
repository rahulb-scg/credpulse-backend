from dotenv import load_dotenv
import os
import logging
from typing import Dict, Any, Optional

class Config:
    """Central configuration management class for CredPulse."""
    
    def __init__(self):
        """Initialize configuration with environment variables."""
        load_dotenv()
        self._initialize_logging()
        self._db_configs: Dict[str, Dict[str, Any]] = {}

    def _initialize_logging(self):
        """Set up basic logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def add_database_config(self, name: str, config: Dict[str, Any]) -> None:
        """
        Add a new database configuration.
        Args:
            name: Unique identifier for this database configuration
            config: Database configuration dictionary
        """
        required_keys = {'host', 'port', 'database', 'user', 'password', 'engine'}
        if not all(key in config for key in required_keys):
            missing = required_keys - set(config.keys())
            raise ValueError(f"Missing required database configuration keys: {missing}")
        
        self._db_configs[name] = config
        logging.info(f"Added database configuration: {name}")

    def get_database_config(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get database configuration by name.
        Args:
            name: Name of the database configuration to retrieve
        Returns:
            Database configuration dictionary if found, None otherwise
        """
        config = self._db_configs.get(name)
        if config is None:
            logging.warning(f"No database configuration found for name: {name}")
        return config

    @property
    def database(self) -> Dict[str, Any]:
        """Get default CredPulse database configuration."""
        config = {
            'host': os.getenv('CREDPULSE_DB_HOST', 'localhost'),
            'port': os.getenv('CREDPULSE_DB_PORT', '5432'),
            'database': os.getenv('CREDPULSE_DB_NAME', 'credpulse'),
            'user': os.getenv('CREDPULSE_DB_USER', 'credpulse'),
            'password': os.getenv('CREDPULSE_DB_PASSWORD', 'credpulse'),
            'engine': os.getenv('CREDPULSE_DB_ENGINE', 'postgresql')
        }
        # Auto-register this as the default configuration if not already present
        if 'default' not in self._db_configs:
            self.add_database_config('default', config)
        return config

    @property
    def mongodb(self) -> Dict[str, Any]:
        """Get MongoDB configuration."""
        return {
            'host': os.getenv('MONGO_DB_HOST', 'localhost'),
            'port': os.getenv('MONGO_DB_PORT', '27017'),
            'user': os.getenv('MONGO_DB_USER', 'credpulse'),
            'password': os.getenv('MONGO_DB_PASSWORD', 'credpulse'),
            'database': os.getenv('MONGO_DB_NAME', 'credpulse'),
            'collection': os.getenv('MONGO_DB_COLLECTION', 'outputs'),
            'auth_source': os.getenv('MONGO_DB_AUTH_SOURCE', 'admin')
        }

    @property
    def aws(self) -> Dict[str, str]:
        """Get AWS configuration."""
        return {
            'access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'region': os.getenv('AWS_REGION', 'us-east-1'),
            's3_bucket_name': os.getenv('AWS_S3_BUCKET_NAME', '')
        }

    @property
    def flask(self) -> Dict[str, Any]:
        """Get Flask configuration."""
        return {
            'upload_folder': os.getenv('UPLOAD_FOLDER', 'uploads'),
            'allowed_extensions': set(os.getenv('ALLOWED_EXTENSIONS', '').split(',')),
            'test_folder': os.getenv('TEST_FOLDER', 'backend/test')
        }

    @property
    def model(self) -> Dict[str, Any]:
        """Get model configuration."""
        return {
            'sample_fraction': float(os.getenv('MODEL_SAMPLE_FRACTION', '0.5')),
            'random_state': int(os.getenv('MODEL_RANDOM_STATE', '42'))
        }

    @property
    def output(self) -> Dict[str, str]:
        """Get output configuration."""
        return {
            'dir_model_ready': os.getenv('OUTPUT_DIR_MODEL_READY', 'backend/test/test_data'),
            'file_base': os.getenv('OUTPUT_FILE_BASE', 'output.csv')
        } 