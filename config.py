import os
import yaml
from datetime import datetime, timedelta
from dagster import Config, ConfigurableResource, Field, StringSource

class FieldRoutesCredentials(Config):
    """Configuration for FieldRoutes office credentials"""
    office_id: int
    base_url: str
    auth_key: str = Field(default=None, description="Authentication key for FieldRoutes API")
    auth_token: str = Field(default=None, description="Authentication token for FieldRoutes API")
    
class OfficeMetadata(Config):
    """Tracks the last successful run for an office"""
    office_id: int
    last_successful_run_utc: datetime = None
    
    def get_window(self, end_time=None):
        """Get the time window for incremental extraction"""
        if end_time is None:
            end_time = datetime.utcnow()
            
        # Default to 24 hours ago if no last run
        start_time = self.last_successful_run_utc or (end_time - timedelta(days=1))
        
        return {
            "dateUpdatedStart": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "dateUpdatedEnd": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "start_datetime": start_time,
            "end_datetime": end_time
        }

class FieldRoutesConfig(ConfigurableResource):
    """Resource that loads and manages office credentials"""
    config_path: str = Field(
        default="configs/office_credentials.yml",
        description="Path to the YAML config file containing credentials"
    )
    
    def get_all_offices(self):
        """Load all office configurations from YAML"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        offices = []
        for office in config.get('offices', []):
            creds = FieldRoutesCredentials(
                office_id=office['office_id'],
                base_url=office['base_url'],
                auth_key=office['auth_key'],
                auth_token=office['auth_token']
            )
            
            metadata = OfficeMetadata(
                office_id=office['office_id'],
                last_successful_run_utc=datetime.fromisoformat(office.get('last_successful_run_utc', '2020-01-01T00:00:00'))
            )
            
            offices.append((creds, metadata))
            
        return offices
        
    def update_last_run(self, office_id, run_time):
        """Update the last successful run time for an office"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        for office in config.get('offices', []):
            if office['office_id'] == office_id:
                office['last_successful_run_utc'] = run_time.isoformat()
                break
                
        with open(self.config_path, 'w') as f:
            yaml.dump(config, f)
