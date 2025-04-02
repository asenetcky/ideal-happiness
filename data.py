import requests
import polars as pl
from io import StringIO

def grab(url: str = "https://data.ct.gov/resource/qhtt-czu2.json", page_size: int = 25000):
    offset = 0
    initial = True

    while True:
        uri = f"{url}?$limit={page_size}&$offset={offset}"

        response = requests.get(uri)
        response.raise_for_status()

        data = pl.read_json(StringIO(response.text))

        rows = data.select(pl.len()).item()

        if initial:
            result = data
            initial = False
        else:
            result = result.extend(data)   

        if rows < page_size:
            break

        offset += page_size
    
    return result

def write(data, path: str = "./output.parquet"):
    data.write_parquet(path)
    print(f"writing parquet file to {path}")
