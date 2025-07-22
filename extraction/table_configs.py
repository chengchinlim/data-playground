from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class TableConfig:
    name: str
    schema: str = "public"
    columns: Optional[List[str]] = None
    where_clause: Optional[str] = None
    order_by: Optional[str] = None
    limit: Optional[int] = None
    
    def get_query(self) -> str:
        columns_str = "*" if not self.columns else ", ".join(self.columns)
        
        query = f"SELECT {columns_str} FROM {self.schema}.{self.name}"
        
        if self.where_clause:
            query += f" WHERE {self.where_clause}"
        
        if self.order_by:
            query += f" ORDER BY {self.order_by}"
        
        if self.limit:
            query += f" LIMIT {self.limit}"
        
        return query


class TableConfigs:
    def __init__(self):
        self.tables = {}
    
    def add_table(self, table_config: TableConfig):
        self.tables[table_config.name] = table_config
    
    def get_table(self, table_name: str) -> TableConfig:
        return self.tables.get(table_name)
    
    def get_all_tables(self) -> Dict[str, TableConfig]:
        return self.tables
    
    def remove_table(self, table_name: str):
        if table_name in self.tables:
            del self.tables[table_name]


def get_default_table_configs() -> TableConfigs:
    configs = TableConfigs()
    
    configs.add_table(TableConfig(
        name="users",
        columns=["id", "first_name", "last_name", "created_at", "updated_at"],
        order_by="created_at DESC",
        limit=1000
    ))
    
    return configs