from datetime import datetime

def parse_row(row):
    preprocessed_row = {
        "date": row[0],
        "vault_id": int(row[4]),
        "temperature_celsius": float(row[25]) if row[25] else None
    }
    return preprocessed_row

def scale_interval(interval, scale_factor):
    return interval
