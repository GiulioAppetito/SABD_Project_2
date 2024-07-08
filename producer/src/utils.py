from datetime import datetime


def parse_row(row):
    preprocessed_row = {
        "date": row[0],
        "serial_number": row[1],
        "model": row[2],
        "failure": bool(int(row[3])),
        "vault_id": int(row[4]),
        "s9_power_on_hours": float(row[12]) if row[12] else None,
        "s194_temperature_celsius": float(row[25]) if row[25] else None
    }
    return preprocessed_row


def scale_interval(interval, scale_factor):
    return interval/scale_factor
