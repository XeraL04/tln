# WORKS
import psycopg2
import requests
import datetime
import logging
import configparser
import time #for sleep
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
        print(f"HTTP Error when sending data to the API : {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting when sending data to the API : {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error when sending data to the API : {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Request Exception when sending data to the API : {err}")
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

# cheking if the data is available on the api 
def check_sync_status(api_url, access_token, attendance_id):
    headers = {
        "Authorization": f"Bearer{access_token}",
        "Content_Type":"application/json"
    }

    try:
        response = requests.get(f"{api_url}/api/v1/attendance/{attendance_id}",headers=headers)
        response.raise_for_status()

        data = response.json()
        if data.get("success") and data.get("data",{}).get("synced"):
            print(f"Attendance ID {attendance_id} already synchronized on the API. Skipping")
            return True
        else:
            print(f"Attendance ID {attendance_id} not synchronized on the API. Retrying sync.")
            return False
    except requests.exceptions.HTTPError as errh:
        if errh.response.status_code == 404:
            print(f"Attendance ID {attendance_id} not found on the API. Retrying sync.")
            return False
        else:
            print (f"HTTP Error when checking if the data is synchronized: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print (f"Error Connecting when checking if the data is synchronized: {errc}")
    except requests.exceptions.Timeout as errt:
        print (f"Timeout Error when checking if the data is synchronized: {errt}")
    except requests.exceptions.RequestException as err:
        print (f"Request Exception Error: {err}")
    # If reach this point, there was an issue with the api request or the status check
    print(f"Unable to check synchronization status for attendance ID {attendance_id}")
    return False

# checking if the data is sync after the sync process
def check_sync_status_after_sync(api_url, access_token, attendance_id):
    headers = {
        "Authorization" : f"Bearer {access_token}",
        "Content_Type" : "application/json"
    }

    try : 
        response = requests.get(f"{api_url}/api/v1/attendance/{attendance_id}", headers=headers)
        response.raise_for_status()

        data = response.json()
        if data.get("succes") and data.get("data",{}).get("synced"):
            print(f"Attendance ID {attendance_id} synchronized successfully on the API.")
            return True
        else:
            print(f"Failed to synchronize Attendance ID {attendance_id} on the API.")
            return False
    except requests.exceptions.HTTPError as errh:
        if errh.response.status_code == 404:
            print(f"Attendance ID {attendance_id} not found on the API. Retrying sync.")
            return False
        else:
            print(f"HTTP Error when checking if the data is synchronized: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting when checking if the data is synchronized: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error when checking if the data is synchronized: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Request Exception Error: {err}")
    # If reach this point, there was an issue with the api request or the status check
    print(f"Unable to check synchronization status for attendance ID {attendance_id}")
    return False

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
        attendance_id, employee_id, datetime, timezone, note, synced = row  # Adjust the order if needed

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

        # Check if the data is already synchronized on the API
        if check_sync_status(api_url, access_token, attendance_id):
            # if synchronized Update the sync status on the local db and continue to the next eteration
            update_sync_status(postgres_connection, attendance_id)
            # check if the data is synchronized after the sync process
            check_sync_status_after_sync(api_url, access_token, attendance_id)
            continue

        # Send data to the Talenteo API
        api_response = send_data_to_api(api_url, access_token, talenteo_employee_id, datetime, timezone, note)

        # If API response is successful, update the sync status in the local database
        if api_response and api_response.get("success"):
            update_sync_status(postgres_connection, attendance_id)

            # Check if the data is synchronized after the sync process
            check_sync_status_after_sync(api_url, access_token, attendance_id)
        else:
            # if syncing fails, retry after a delay 
            print(f"Failed to sync data for attendace ID {attendance_id}, Retrying in 5 seconds...")
            time.sleep(5) #sleep for 5 secs before retrying 

    # Close the database connection
    postgres_connection.close()

    # Print a message after synchronizing all data
    print("All attendance data synchronized successfully.")

if __name__ == "__main__":
    main()