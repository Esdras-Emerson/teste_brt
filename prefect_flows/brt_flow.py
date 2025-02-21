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
    """
    Busca dados de GPS da API do BRT.

    Retorna:
        list: Uma lista de dicionários contendo dados dos veículos.
    """
    response = requests.get("https://dados.mobilidade.rio/gps/brt", timeout=5)
    response.raise_for_status()
    return response.json()["veiculos"]  # Supondo que a API retorne {"veiculos": [...]}

@task
def save_to_csv(data):
    """
    Salva os dados de GPS buscados em um arquivo CSV.

    Args:
        data (list): Uma lista de dicionários contendo dados dos veículos.

    Retorna:
        str: O nome do arquivo CSV salvo.
    """
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"data/brt_gps_{timestamp}.csv"
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["bus_id", "latitude", "longitude", "speed", "captured_at"])
        for vehicle in data:
            writer.writerow([
                vehicle["codigo"],
                vehicle["latitude"],
                vehicle["longitude"],
                vehicle["velocidade"],
                datetime.now().isoformat()
            ])
    return filename

@task
def load_to_postgres(filename):
    """
    Carrega os dados do arquivo CSV para um banco de dados PostgreSQL.

    Args:
        filename (str): O nome do arquivo CSV a ser carregado.
    """
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
            try:
                cur.execute("""
                    INSERT INTO raw_data (bus_id, latitude, longitude, speed, captured_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, row)
                conn.commit()
            except psycopg2.IntegrityError:
                conn.rollback()
                cur.execute("""
                    UPDATE raw_data 
                    SET latitude = %s, longitude = %s, speed = %s
                    WHERE bus_id = %s AND captured_at = %s
                """, (row[1], row[2], row[3], row[0], row[4]))
                conn.commit()
    conn.close()

@task
def create_table():
    """
    Cria a tabela raw_data no banco de dados PostgreSQL se ela não existir.
    """
    conn = psycopg2.connect(
        dbname="brt_data",
        user="admin",
        password="admin",
        host="localhost"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_data (
            bus_id VARCHAR(50),
            latitude FLOAT,
            longitude FLOAT,
            speed FLOAT,
            captured_at TIMESTAMP,
            PRIMARY KEY (bus_id, captured_at)
        )
    """)
    conn.commit()
    conn.close()

with Flow("BRT GPS Pipeline", schedule=schedule) as flow:
create_table()  # Ensure the table exists before any data is loaded
    data = fetch_gps_data()
    csv_file = save_to_csv(data)
    load_to_postgres(csv_file)

if __name__ == "__main__":
    flow.run()
