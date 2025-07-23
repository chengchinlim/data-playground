import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    username: str = "postgres"
    password: str = ""
    
    # Add property alias for backward compatibility with dlt
    @property
    def user(self) -> str:
        return self.username
    
    @classmethod
    def from_env(cls):
        # Get the project root directory (parent of config directory)
        project_root = Path(__file__).parent.parent
        env_file = project_root / ".env.local"
        env_vars = {}
        
        if env_file.exists():
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key.strip()] = value.strip().strip('"').strip("'")
        
        return cls(
            host=env_vars.get("DB_HOST", "localhost"),
            port=int(env_vars.get("DB_PORT", "5432")),
            database=env_vars.get("DB_NAME", "postgres"),
            username=env_vars.get("DB_USER", "postgres"),
            password=env_vars.get("DB_PASSWORD", "")
        )
    
    def __init__(self, host=None, port=None, database=None, username=None, password=None):
        """Initialize DatabaseConfig, using from_env() if no parameters provided."""
        if all(param is None for param in [host, port, database, username, password]):
            # If no parameters provided, load from environment
            env_config = self.from_env()
            self.host = env_config.host
            self.port = env_config.port
            self.database = env_config.database
            self.username = env_config.username
            self.password = env_config.password
        else:
            # Use provided parameters or defaults
            self.host = host or "localhost"
            self.port = port or 5432
            self.database = database or "postgres"
            self.username = username or "postgres"
            self.password = password or ""
    
    def connection_string(self):
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"