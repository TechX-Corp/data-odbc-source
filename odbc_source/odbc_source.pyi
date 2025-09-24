from typing import Dict, List, Optional

def read_to_parquet(
    connection_string: str,
    query: str,
    base_dir: str,
    table_dir: str,
    log_dir: str,
    total_size: int,
    datasource_type: str,
    batch_size: Optional[int],
    user_name: Optional[str],
    password: Optional[str],
    encryption_key=Optional[str],
    prefix=Optional[str],
) -> int: ...
def read_to_json(
    connection_string: str,
    query: str,
    datasource_type: str,
    user_name: Optional[str],
    password: Optional[str],
) -> List[Dict[str, str]]: ...
