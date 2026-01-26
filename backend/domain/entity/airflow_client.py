from dataclasses import dataclass
from typing import Dict, Any, Optional
import requests
import json


@dataclass
class Airflow:
    """Airflow client for DAG triggers and connection management"""
    url: str
    username: str
    password: str
    
    def __post_init__(self):
        self.auth = (self.username, self.password)
        self.headers = {"Content-Type": "application/json"}
    
    @classmethod
    def from_dict(cls, config: Dict):
        return cls(
            url=config.get("url"),
            username=config.get("username"),
            password=config.get("password")
        )
    
    # ==================== Connection Management ====================
    
    def upsert_connection(self, config: dict) -> bool:
        """Create or update Airflow Connection"""
        connection_id = config["connection_name"]
        
        port = config.get("port")
        if isinstance(port, str):
            port = int(port)
        
        payload = {
            "connection_id": connection_id,
            "conn_type": "postgres",
            "host": config["host"],
            "port": port,
            "schema": config["database"], 
            "login": config["username"],
            "password": config["password"]
        }
        
        jdbc_properties = config.get("jdbc_properties", {})
        if jdbc_properties:
            payload["extra"] = json.dumps(jdbc_properties)
        
        print(f"Airflow payload: {json.dumps({**payload, 'password': '***'}, indent=2)}")
        
        try:
            response = requests.patch(
                f"{self.url}/api/v1/connections/{connection_id}",
                json=payload,
                auth=self.auth,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 404:
                # Connection doesn't exist, create new one (POST)
                response = requests.post(
                    f"{self.url}/api/v1/connections",
                    json=payload,
                    auth=self.auth,
                    headers=self.headers,
                    timeout=30
                )
            
            # Check for success
            if response.status_code in [200, 201]:
                print(f"Airflow connection upserted successfully: {connection_id}")
                return True
            
            # Handle error
            error_detail = self._parse_error_response(response)
            print(f"Airflow API error [{response.status_code}]: {error_detail}")
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            print(f"Request exception: {type(e).__name__}: {e}")
            raise
        
        return False
    
    def delete_connection(self, connection_name: str) -> bool:
        """Delete Airflow Connection"""
        try:
            response = requests.delete(
                f"{self.url}/api/v1/connections/{connection_name}",
                auth=self.auth,
                timeout=30
            )
            
            # 200/204 = deleted, 404 = already doesn't exist
            if response.status_code in [200, 204, 404]:
                return True
            
            error_detail = self._parse_error_response(response)
            print(f"Failed to delete connection: [{response.status_code}] {error_detail}")
            return False
            
        except requests.exceptions.RequestException as e:
            print(f"Delete connection exception: {e}")
            return False
    
    def get_connection(self, connection_name: str) -> Optional[dict]:
        """Get Airflow Connection"""
        try:
            response = requests.get(
                f"{self.url}/api/v1/connections/{connection_name}",
                auth=self.auth,
                timeout=30
            )
            
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Get connection exception: {e}")
            return None
    
    def test_connectivity(self) -> Dict[str, Any]:
        """Test if Airflow API is reachable and credentials are valid"""
        try:
            response = requests.get(
                f"{self.url}/api/v1/health",
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                return {
                    "status": "healthy",
                    "reachable": True,
                    "authenticated": True
                }
            else:
                return {
                    "status": "unhealthy",
                    "reachable": True,
                    "authenticated": False,
                    "status_code": response.status_code
                }
                
        except requests.exceptions.ConnectionError:
            return {
                "status": "unreachable",
                "reachable": False,
                "error": f"Cannot connect to {self.url}"
            }
        except Exception as e:
            return {
                "status": "error",
                "reachable": False,
                "error": str(e)
            }
    
    def _parse_error_response(self, response: requests.Response) -> str:
        """Parse error response from Airflow API"""
        try:
            error_data = response.json()
            if "detail" in error_data:
                return error_data["detail"]
            elif "title" in error_data:
                detail = error_data.get("detail", "")
                return f"{error_data['title']}: {detail}" if detail else error_data["title"]
            else:
                return json.dumps(error_data)
        except:
            return response.text or f"HTTP {response.status_code}"