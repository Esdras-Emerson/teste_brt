from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import requests
from datetime import datetime, timedelta
import csv
import psycopg2
import os

schedule = IntervalSchedule(interval=timedelta(minutes=1))

@task
def fetch_gps_data():
    response = requests.get("https://dados.mobilidade.rio/gps/brt")
    response.raise_for_status()
    return response.json()["veiculos"]  # Supondo que a API retorne {"veiculos": [...]}

@task
def save_to_csv(data):
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"data/brt_gps_{timestamp}.csv"
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["bus_id", "latitude", "longitude", "speed", "captured_at"])
        for vehicle in data:
            writer.writerow([
                vehicle["id"],
                vehicle["latitude"],
                vehicle["longitude"],
                vehicle["velocidade"],
                datetime.now().isoformat()
            ])
    return filename

@task
def load_to_postgres(filename):
    conn = psycopg2.connect(
        dbname="brt_data",
        user="admin",
        password="admin",
        host="localhost"
    )
    cur = conn.cursor()
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Pular header
        for row in reader:
            cur.execute("""
                INSERT INTO raw_data (bus_id, latitude, longitude, speed, captured_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (bus_id, captured_at) DO NOTHING
            """, row)
    conn.commit()
    conn.close()

with Flow("BRT GPS Pipeline", schedule=schedule) as flow:
    data = fetch_gps_data()
    csv_file = save_to_csv(data)
    load_to_postgres(csv_file)

if __name__ == "__main__":
    flow.run()