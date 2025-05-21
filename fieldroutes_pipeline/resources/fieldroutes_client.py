import requests
import time
from typing import List, Dict, Any, Optional
from dagster import ConfigurableResource, Field

class FieldRoutesClient(ConfigurableResource):
    """Client for interacting with FieldRoutes API"""
    max_retries: int = Field(default=3, description="Maximum number of retry attempts")
    retry_delay: float = Field(default=1.0, description="Initial delay between retries in seconds")
    throttle_sleep: float = Field(default=0.5, description="Sleep time between requests to avoid throttling")
    
    def _make_request(self, method, url, data=None, auth=None, retry_count=0):
        """Make HTTP request with retry logic"""
        try:
            if method == "GET":
                response = requests.get(url, params=data, headers=auth)
            else:  # POST
                response = requests.post(url, json=data, headers=auth)
                
            response.raise_for_status()
            time.sleep(self.throttle_sleep)  # Respect rate limits
            return response.json()
            
        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            if retry_count < self.max_retries:
                wait_time = self.retry_delay * (2 ** retry_count)  # Exponential backoff
                time.sleep(wait_time)
                return self._make_request(method, url, data, auth, retry_count + 1)
            else:
                raise Exception(f"Request failed after {self.max_retries} retries: {str(e)}")
    
    def get_auth_headers(self, credentials):
        """Convert credentials to headers format"""
        return {
            "authenticationKey": credentials.auth_key,
            "authenticationToken": credentials.auth_token,
            "Content-Type": "application/json"
        }
    
    def search_entity(self, credentials, entity, params, include_data=False):
        """Execute a search request to get IDs or data for an entity"""
        url = f"{credentials.base_url}/{entity}/search"
        auth = self.get_auth_headers(credentials)
        
        # Always include office ID and include_data flag
        data = {
            **params,
            "officeIDs": credentials.office_id,
            "includeData": 1 if include_data else 0
        }
        
        return self._make_request("POST", url, data, auth)
    
    def get_entity_batch(self, credentials, entity, ids):
        """Get a batch of entities by IDs"""
        if not ids:
            return []
            
        url = f"{credentials.base_url}/{entity}/get"
        auth = self.get_auth_headers(credentials)
        
        # FieldRoutes expects entity + IDs as the parameter name
        data = {
            f"{entity}IDs": ids,
            "officeIDs": credentials.office_id
        }
        
        return self._make_request("POST", url, data, auth)
    
    def extract_entity(self, credentials, entity, time_window, batch_size=1000, predict_size=False):
        """
        Extract an entity with proper pagination handling
        
        If predict_size is True, will try to use includeData=1 for small datasets
        """
        # First, attempt to get a count or make an educated guess on size
        use_include_data = predict_size
        
        if predict_size:
            # Could implement a count endpoint call here if available
            # For now, we'll try includeData=1 and fall back if needed
            pass
        
        # Initial search to get IDs
        search_results = self.search_entity(
            credentials, 
            entity, 
            time_window,
            include_data=use_include_data
        )
        
        # The first 1,000 records may be included directly
        records = search_results.get("resolvedObjects", [])
        
        # Check if there are unresolved IDs (more than initial 1,000)
        unresolved_ids = search_results.get(f"{entity}IDsNoDataExported", [])
        
        # If we have unresolved IDs, fetch them in batches
        for i in range(0, len(unresolved_ids), batch_size):
            batch_ids = unresolved_ids[i:i+batch_size]
            batch_data = self.get_entity_batch(credentials, entity, batch_ids)
            records.extend(batch_data)
            
        return records
