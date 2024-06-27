import time
import pandas as pd
import requests

def read_data(file_path):
    return pd.read_csv(file_path)

def send_to_nifi(data, nifi_url):
    headers = {"Content-Type": "application/json"}
    for _, row in data.iterrows():
        event = row.to_json()
        requests.post(nifi_url, headers=headers, data=event)
        time.sleep(0.01)  # Accelerated replay

def main():
    data = read_data('/data/hdd-smart-data.csv')
    nifi_url = "http://nifi:8080/nifi-api/data-transfer-endpoint"
    send_to_nifi(data, nifi_url)

if __name__ == '__main__':
    main()
