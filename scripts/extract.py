import requests
import pandas as pd
from datetime import datetime

def extract():
    url = "https://fakestoreapi.com/carts"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    df = pd.json_normalize(data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"/opt/airflow/data/raw/orders_{timestamp}.csv"

    df.to_csv(path, index=False)
    return path