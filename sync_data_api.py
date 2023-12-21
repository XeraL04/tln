import psycopg2
import requests
import datetime
import logging
import configparser
from id_mapping import id_mapping
from api_helper import get_access_token

# read the configuration from the config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Local database configuration
db_config = {
    "host": config.get('Postgres','host'),
    "database": config.get('Postgres','database'),
    "user": config.get('Postgres','user'),
    "password": config.get('Postgres','password'),
    "port": config.get('Postgres','port'),
}

# Talenteo API configuration
api_url = config.get('TalenteoAPI','api_url')
client_id = config.get('TalenteoAPI','client_id')
client_secret = config.get('TalenteoAPI','client_secret')

# Configure logging
logging.basicConfig(filename='sync_data.log', level=logging.DEBUG)

def connect_to_database(config):
    try:
        connection = psycopg2.connect(**config)
        print("Connected to PostgreSQL database")
        return connection
    except Exception as e:
        print(f"Error: Unable to connect to the database - {e}")
        return None

def fetch_attendance_data(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM attendance WHERE synced = false;")
        attendance_data = cursor.fetchall()
        return attendance_data
    except Exception as e:
        print(f"Error: Unable to fetch attendance data - {e}")
        return None
    finally:
        cursor.close()

def send_data_to_api(api_url, access_token, employee_id, datetime, timezone, note):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Convert datetime object to a formatted string
    formatted_datetime = datetime.strftime("%Y-%m-%d %H:%M")

    data = {
        # "id": employee_id,
        "datetime": formatted_datetime,
        "timezone": timezone,
        "note": note,
    }

    try:
        response = requests.post(f"{api_url}/api/v1/employee/{employee_id}/punch", headers=headers, json=data)
        response.raise_for_status()
        # Log information about the synchronized data and payload
        print(response)
        logging.info(f"Synchronized data for employee ID {employee_id} with payload: {data}")
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Request Exception: {err}")
        # log error if synchronization fails
        logging.error(f"Error : Unable to send data to API - {err}")
    return None

def update_sync_status(connection, attendance_id):
    try:
        cursor = connection.cursor()
        cursor.execute("UPDATE attendance SET synced = true WHERE attendance_id = %s;", (attendance_id,))
        connection.commit()
        print(f"Sync status updated for attendance ID: {attendance_id}")
    except Exception as e:
        print(f"Error: Unable to update sync status - {e}")
    finally:
        cursor.close()

def main():
    # Get access token
    access_token = get_access_token(api_url, client_id, client_secret)
    if not access_token:
        print("Error: Unable to obtain access token.")
        return

    # Connect to the local database
    postgres_connection = connect_to_database(db_config)
    if not postgres_connection:
        return

    # Fetch unsynchronized attendance data
    attendance_data = fetch_attendance_data(postgres_connection)
    if not attendance_data:
        return

    # Iterate through the attendance data
    for row in attendance_data:
        attendance_id, employee_id, datetime, timezone, note, synced = row

        # Skip if the data is already synchronized
        if synced:
            print(f"Attendance ID {attendance_id} for employee ID {employee_id} is already synchronized. Skipping.")
            continue

        # Map the local database ID to the corresponding Talenteo employee ID
        talenteo_employee_id = id_mapping.get(employee_id)

        # Skip if there's no mapping for the employee
        if talenteo_employee_id is None:
            print(f"No mapping found for local DB ID: {employee_id}. Skipping.")
            continue

        # Send data to the Talenteo API
        api_response = send_data_to_api(api_url, access_token, talenteo_employee_id, datetime, timezone, note)

        # If API response is successful, update the sync status in the local database
        if api_response and api_response.get("success"):
            update_sync_status(postgres_connection, attendance_id)

    # Close the database connection
    postgres_connection.close()

if __name__ == "__main__":
    main()