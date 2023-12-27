import psycopg2
import requests
import datetime
import logging
import configparser
import time
from id_mapping import id_mapping
from api_helper import get_access_token

# Set up logging configuration at the beginning of the script
logging.basicConfig(level=logging.DEBUG)

# read the configuration from the config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Local database configuration
db_config = {
    "host": config.get('Postgres', 'host'),
    "database": config.get('Postgres', 'database'),
    "user": config.get('Postgres', 'user'),
    "password": config.get('Postgres', 'password'),
    "port": config.get('Postgres', 'port'),
}

# Talenteo API configuration
api_url = config.get('TalenteoAPI', 'api_url')
client_id = config.get('TalenteoAPI', 'client_id')
client_secret = config.get('TalenteoAPI', 'client_secret')

# Create a logger for business log
business_logger = logging.getLogger('business_log')
business_logger.setLevel(logging.INFO)
business_handler = logging.FileHandler('business_log.log')
business_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
business_handler.setFormatter(business_formatter)
business_logger.addHandler(business_handler)

# Create a logger for debug log
debug_logger = logging.getLogger('debug_log')
debug_logger.setLevel(logging.DEBUG)
debug_handler = logging.FileHandler('debug_log.log')
debug_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
debug_handler.setFormatter(debug_formatter)
debug_logger.addHandler(debug_handler)

def connect_to_database(config):
    try:
        connection = psycopg2.connect(**config)
        # business log
        logging.info("Connected to PostgreSQL database")
        return connection
    except Exception as e:
        # debug log
        logging.error(f"Error: Unable to connect to the database - {e}")
        return None

def fetch_attendance_data(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM attendance WHERE synced = false;")
            attendance_data = cursor.fetchall()
        return attendance_data
    except Exception as e:
        # debug log
        logging.error(f"Error: Unable to fetch attendance data - {e}")
        return None

def send_data_to_api(api_url, access_token, employee_id, datetime, timezone, note):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    formatted_datetime = datetime.strftime("%Y-%m-%d %H:%M")
    data = {
        "datetime": formatted_datetime,
        "timezone": timezone,
        "note": note,
    }

    try:
        response = requests.post(f"{api_url}/api/v1/employee/{employee_id}/punch", headers=headers, json=data)
        response.raise_for_status()
        # business log
        business_logger.info(f"Synchronized data for employee ID {employee_id}")
        # debug log
        debug_logger.debug(f"Synchronized data for employee ID {employee_id} with payload: {data}")
        return response.json()
    except requests.exceptions.RequestException as err:
        log_message = f"Error: Unable to send data to API for employee ID {employee_id} - {err}"
        debug_logger.debug(log_message)
        business_logger.error(log_message)
        return None

def update_sync_status(connection, attendance_id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE attendance SET synced = true WHERE attendance_id = %s;", (attendance_id,))
            connection.commit()
        logging.info(f"Sync status updated for attendance ID: {attendance_id}")
    except Exception as e:
        logging.error(f"Error: Unable to update sync status - {e}")

def check_sync_status(api_url, access_token, attendance_id):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content_Type": "application/json"
    }

    try:
        response = requests.get(f"{api_url}/api/v1/attendance/{attendance_id}", headers=headers)
        response.raise_for_status()

        data = response.json()
        if data.get("success") and data.get("data", {}).get("synced"):
            logging.info(f"Attendance ID {attendance_id} already synchronized on the API. Skipping")
            return True
        else:
            logging.info(f"Attendance ID {attendance_id} not synchronized on the API. Retrying sync.")
            return False
    except requests.exceptions.RequestException as err:
        log_message = f"Error when checking if the data is synchronized for Attendance ID {attendance_id}: {err}"
        logging.error(log_message)
        return False

def check_sync_status_after_sync(api_url, access_token, attendance_id):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content_Type": "application/json"
    }

    try:
        response = requests.get(f"{api_url}/api/v1/attendance/{attendance_id}", headers=headers)
        response.raise_for_status()

        data = response.json()

        if data.get("success") and data.get("data", {}).get("synced"):
            logging.info(f"Attendance ID {attendance_id} synchronized successfully on the API.")
            return True
        else:
            logging.error(f"Failed to synchronize Attendance ID {attendance_id} on the API.")
            return False
    except requests.exceptions.RequestException as err:
        log_message = f"Error when checking if the data is synchronized for Attendance ID {attendance_id}: {err}"
        logging.error(log_message)
        return False

def sync_data_in_date_range(start_date, end_date, connection, api_url, access_token):
    date_increment = datetime.timedelta
    current_date = start_date

    while current_date <= end_date:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM attendance WHERE date(datetime) = %s AND synced = false;", (current_date,))
                attendance_data = cursor.fetchall()

                for row in attendance_data:
                    attendance_id, employee_id, datetime_value, timezone, note, synced = row

                    talenteo_employee_id = id_mapping.get(employee_id)

                    if talenteo_employee_id is None:
                        logging.info(f"No mapping found for local DB ID: {employee_id}. Skipping.")
                        continue

                    api_response = send_data_to_api(api_url, access_token, talenteo_employee_id, datetime_value, timezone, note)

                    if api_response and api_response.get("success"):
                        update_sync_status(connection, attendance_id)
                        check_sync_status_after_sync(api_url, access_token, attendance_id)
                    else:
                        logging.error(f"Failed to sync data for attendance ID {attendance_id}. Retrying in 5 seconds...")
                        time.sleep(5)

                logging.info(f"All attendance data for the date {current_date} synchronized successfully.")

        except Exception as e:
            logging.error(f"Error: Unable to sync data for date {current_date} - {e}")
        finally:
            current_date += date_increment(days=1)

def main():
    access_token = get_access_token(api_url, client_id, client_secret)
    if not access_token:
        logging.error("Error: Unable to obtain access token.")
        print("Error: Unable to obtain access token.")
        return

    postgres_connection = connect_to_database(db_config)
    if not postgres_connection:
        return

    start_date = datetime.date(2023, 12, 27)
    end_date = datetime.date(2023, 12, 27)

    sync_data_in_date_range(start_date, end_date, postgres_connection, api_url, access_token)

    postgres_connection.close()

if __name__ == "__main__":
    main()
