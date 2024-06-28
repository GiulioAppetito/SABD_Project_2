from datetime import datetime

def parse_row(row):
    preprocessed_row = {
        "date": row[0],
        "serial_number": row[1],
        "model": row[2],
        "failure": row[3],
        "vault_id": int(row[4]),
        "power_on_hours": int(row[9]),
        "temperature_celsius": int(row[32]) if row[32] else None
    }
    return preprocessed_row

def scale_interval(interval, scale_factor):
    return interval
