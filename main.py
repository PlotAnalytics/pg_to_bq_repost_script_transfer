import os
import psycopg2
from google.cloud import bigquery
from flask import Flask, jsonify
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize Flask app
app = Flask(__name__)

# Environment variables
POSTGRES_HOST = '34.93.195.0'
POSTGRES_DB = 'postgres'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'Plotpointe!@3456'
BIGQUERY_PROJECT_ID = 'academic-oath-419411'
BIGQUERY_DATASET_ID = 'hamza_tiktok_scraper'
BIGQUERY_TABLE_ID = 'tiktok_videos_pg'

BIGQUERY_BATCH_SIZE = 50 

def fetch_postgres_data():
    try:
        logging.info("Connecting to PostgreSQL database.")
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()

        query = """
            SELECT s.video_id, 
                   convert_from(decode(encode(s.script, 'escape'), 'escape'), 'UTF8') AS script_text, 
                   v.url  
            FROM statistics s
            JOIN video v ON s.video_id = v.id
            WHERE v.sponsor_id IS NOT NULL 
              AND s.script IS NOT NULL
              AND length(encode(s.script, 'escape')) > 0
              AND encode(s.script, 'escape') NOT LIKE '%:%'
        """
        logging.info("Executing SQL query to fetch data from PostgreSQL.")
        cursor.execute(query)
        data = cursor.fetchall()

        logging.info(f"Fetched {len(data)} records from PostgreSQL.")
        return data
    except Exception as e:
        logging.error(f"Error fetching data from PostgreSQL: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("PostgreSQL connection closed.")

def insert_into_bigquery(rows):
    try:
        logging.info(f"Inserting {len(rows)} records into BigQuery.")
        client = bigquery.Client()

        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"

        for i in range(0, len(rows), BIGQUERY_BATCH_SIZE):
            batch = rows[i:i + BIGQUERY_BATCH_SIZE]
            logging.info(f"Inserting batch {i // BIGQUERY_BATCH_SIZE + 1}, size: {len(batch)}")

            errors = client.insert_rows_json(table_id, batch)
            if errors:
                logging.error(f"Encountered errors while inserting batch {i // BIGQUERY_BATCH_SIZE + 1}: {errors}")
            else:
                logging.info(f"Batch {i // BIGQUERY_BATCH_SIZE + 1} successfully inserted.")
                
    except Exception as e:
        logging.error(f"Error inserting data into BigQuery: {e}")

@app.route('/transfer-data', methods=['POST'])
def transfer_data():
    logging.info("Starting data transfer from PostgreSQL to BigQuery.")

    data = fetch_postgres_data()

    if not data:
        logging.warning("No data fetched from PostgreSQL. Exiting.")
        return jsonify({'message': 'No data fetched from PostgreSQL'}), 500

    rows_to_insert = [{"video_id": row[0], "script_text": row[1], "url": row[2]} for row in data]

    insert_into_bigquery(rows_to_insert)

    logging.info("Data transfer complete.")
    return jsonify({'message': 'Data transfer complete'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
