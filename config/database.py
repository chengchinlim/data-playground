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
    
    def connection_string(self):
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"