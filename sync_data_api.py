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
buisness_logger = logging.getLogger('buisness_log')
buisness_logger.setLevel(logging.INFO)
buisness_handler = logging.FileHandler('buisness_log.log')
buisness_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
buisness_handler.setFormatter(buisness_formatter)
buisness_logger.addHandler(buisness_handler)

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
        print("Connected to PostgreSQL database")
        return connection
    except Exception as e:
        # debug log
        logging.error(f"Error: Unable to connect to the database - {e}")
        print(f"Error: Unable to connect to the database - {e}")
        return None

def fetch_attendance_data(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM attendance WHERE synced = false;")
        attendance_data = cursor.fetchall()
        return attendance_data
    except Exception as e:
        # debug log
        logging.error(f"Error: Unable to fetch attendance data - {e}")
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
        "datetime": formatted_datetime,
        "timezone": timezone,
        "note": note,
    }

    try:
        response = requests.post(f"{api_url}/api/v1/employee/{employee_id}/punch", headers=headers, json=data)
        response.raise_for_status()
        # Log information about the synchronized data and payload
        print(response)
        # business log
        buisness_logger.info(f"Synchronized data for employee ID {employee_id}")
        # debug log
        debug_logger.debug(f"Synchronized data for employee ID {employee_id} with payload: {data}")
        return response.json()
    except requests.exceptions.HTTPError as errh:
        # debug log
        debug_logger.debug(f"HTTP Error when sending data to the API : {errh}")
        print(f"HTTP Error when sending data to the API : {errh}")
        logging.error(f"HTTP Error when sending data to the API : {errh}")
    except requests.exceptions.ConnectionError as errc:
        debug_logger.debug(f"Error connecting when sending data to the API : {errc}")
        print(f"Error Connecting when sending data to the API : {errc}")
        logging.error(f"Error Connecting when sending data to the API : {errc}")
    except requests.exceptions.Timeout as errt:
        debug_logger.debug(f"Error connecting when sending data to the API : {errt}")
        print(f"Timeout Error when sending data to the API : {errt}")
        logging.error(f"Timeout Error when sending data to the API : {errt}")
    except requests.exceptions.RequestException as err:
        # log error if synchronization fails
        debug_logger.debug(f"Error : Unable to send data to API - {err}")
        print(f"Request Exception when sending data to the API : {err}")
        buisness_logger.error(f"Error : unable to send data to API - {err}")
        logging.error(f"Error : unable to send data to API - {err}")  
    return None

def update_sync_status(connection, attendance_id):
    try:
        cursor = connection.cursor()
        cursor.execute("UPDATE attendance SET synced = true WHERE attendance_id = %s;", (attendance_id,))
        connection.commit()
        logging.info(f"Sync status updates for attendance ID : {attendance_id}")
        print(f"Sync status updated for attendance ID: {attendance_id}")
    except Exception as e:
        logging.error(f"Error : unable to update sync status - {e}")
        print(f"Error: Unable to update sync status - {e}")
    finally:
        cursor.close()

# checking if the data is available on the api
def check_sync_status(api_url, access_token, attendance_id):
    headers = {
        "Authorization": f"Bearer{access_token}",
        "Content_Type": "application/json"
    }

    try:
        response = requests.get(f"{api_url}/api/v1/attendance/{attendance_id}", headers=headers)
        response.raise_for_status()

        data = response.json()
        if data.get("success") and data.get("data", {}).get("synced"):
            logging.info(f"Attendance ID {attendance_id} already synchronized on the API. Skipping")
            print(f"Attendance ID {attendance_id} already synchronized on the API. Skipping")
            return True
        else:
            logging.info(f"Attendance ID {attendance_id} not synchronized on the API. Retrying sync.")
            print(f"Attendance ID {attendance_id} not synchronized on the API. Retrying sync.")
            return False
    except requests.exceptions.HTTPError as errh:
        if errh.response.status_code == 404:
            logging.info(f"Attendance ID {attendance_id} not found on the API. Retrying sync.")
            print(f"Attendance ID {attendance_id} not found on the API. Retrying sync.")
            return False
        else:
            logging.error(f"HTTP Error when checking if the data is synchronized: {errh}")
            print(f"HTTP Error when checking if the data is synchronized: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting when checking if the data is synchronized: {errc}")
        print(f"Error Connecting when checking if the data is synchronized: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error when checking if the data is synchronized: {errt}")
        print(f"Timeout Error when checking if the data is synchronized: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Request Exception Error: {err}")
        print(f"Request Exception Error: {err}")
    # If reach this point, there was an issue with the api request or the status check
    logging.error(f"Unable to check synchronization status for attendance ID {attendance_id}")
    print(f"Unable to check synchronization status for attendance ID {attendance_id}")
    return False

# checking if the data is sync after the sync process
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
            print(f"Attendance ID {attendance_id} synchronized successfully on the API.")
            return True
        else:
            logging.error(f"Failed to synchronize Attendance ID {attendance_id} on the API.")
            print(f"Failed to synchronize Attendance ID {attendance_id} on the API.")
            return False
    except requests.exceptions.HTTPError as errh:
        if errh.response.status_code == 404:
            logging.info(f"Attendance ID {attendance_id} not found on the API. Retrying sync.")
            print(f"Attendance ID {attendance_id} not found on the API. Retrying sync.")
            return False
        else:
            logging.error(f"HTTP Error when checking if the data is synchronized: {errh}")
            print(f"HTTP Error when checking if the data is synchronized: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting when checking if the data is synchronized: {errc}")
        print(f"Error Connecting when checking if the data is synchronized: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error when checking if the data is synchronized: {errt}")
        print(f"Timeout Error when checking if the data is synchronized: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Request Exception Error: {err}")
        print(f"Request Exception Error: {err}")
    # If reach this point, there was an issue with the api request or the status check
    print(f"Unable to check synchronization status for attendance ID {attendance_id}")
    return False

def sync_data_in_date_range(start_date, end_date, connection, api_url, access_token):
    date_increment = datetime.timedelta
    current_date = start_date
    while current_date <= end_date:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM attendance WHERE date(datetime) = %s AND synced = false;", (current_date,))
            attendance_data = cursor.fetchall()

            for row in attendance_data:
                attendance_id, employee_id, datetime_value, timezone, note, synced = row

                # Map the local database ID to the corresponding Talenteo employee ID
                talenteo_employee_id = id_mapping.get(employee_id)

                # Skip if there's no mapping for the employee
                if talenteo_employee_id is None:
                    logging.info(f"No mapping found for local DB ID: {employee_id}. Skipping.")
                    print(f"No mapping found for local DB ID: {employee_id}. Skipping.")
                    continue

                # Send data to the Talenteo API
                api_response = send_data_to_api(api_url, access_token, talenteo_employee_id, datetime_value, timezone, note)

                # If API response is successful, check if the data is synchronized after the sync process
                if api_response and api_response.get("success"):
                    update_sync_status(connection, attendance_id)
                    # Check if the data is synchronized after the sync process
                    check_sync_status_after_sync(api_url, access_token, attendance_id)
                else:
                    # If syncing fails, log an error and retry after a delay
                    logging.error(
                        f"Failed to sync data for attendance ID {attendance_id}, Retrying in 5 seconds...")
                    print(
                        f"Failed to sync data for attendance ID {attendance_id}, Retrying in 5 seconds...")
                    time.sleep(5)

            logging.info(f"All attendance data for the date {current_date} synchronized successfully.")
            print(f"All attendance data for the date {current_date} synchronized successfully.")

        except Exception as e:
            logging.error(f"Error: Unable to sync data for date - {e}")
            print(f"Error: Unable to sync data for date - {e}")
        finally:
            cursor.close()
            current_date += date_increment(days=1)

def main():
    # Get access token
    access_token = get_access_token(api_url, client_id, client_secret)
    if not access_token:
        logging.error("Error: Unable to obtain access token.")
        print("Error: Unable to obtain access token.")
        return

    # Connect to the local database
    postgres_connection = connect_to_database(db_config)
    if not postgres_connection:
        return

    # Set the date range for synchronization
    start_date = datetime.date(2023, 12, 25)
    end_date = datetime.date(2023, 12, 25)

    # Sync data for the specified date range
    sync_data_in_date_range(start_date, end_date, postgres_connection, api_url, access_token)

    # Close the database connection
    postgres_connection.close()

if __name__ == "__main__":
    main()
