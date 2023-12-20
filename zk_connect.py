# WORKS

import psycopg2
from zk import ZK
import configparser

# The Configuration
def read_config(file_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def get_device_config():
    config = read_config()
    
    # Debug print to see available sections
    print("Sections in config:", config.sections())

    # Case-insensitive search for 'Device' section
    for section in config.sections():
        if section.lower() == 'device1':
            return config[section]

    # If 'Device' section is not found
    raise KeyError('Device section not found in the configuration file')

def get_postgres_config():
    config = read_config()
    return config['Postgres']

def connect_to_postgresql(postgres_config):
    try:
        connection = psycopg2.connect(
            database=postgres_config['database'],
            user=postgres_config['user'],
            password=postgres_config['password'],
            host=postgres_config['host'],
            port=postgres_config['port']
        )
        print("Connected to PostgreSQL database")
        return connection
    except Exception as e:
        print(f"Error: Unable to connect to the database - {e}")
        return None

def register_attendance_to_database(connection, talenteo_id, datetime, timezone, note, is_sync):
    try:
        cursor = connection.cursor()
        # check if the attendance log alredy exists
        cursor.execute("""
            SELECT * FROM attendance
            WHERE employee_id = %s AND datetime = %s;
        """, (talenteo_id, datetime))
        existing_log = cursor.fetchone()

        if existing_log:
            print("Attendance log already exists. Skipping registration.")
        else:
            cursor.execute("""
                INSERT INTO attendance (
                    employee_id, 
                    datetime, 
                    timezone, 
                    note, 
                    synced
                ) 
                VALUES (%s, %s, %s, %s, %s)
            """, (
                talenteo_id, datetime, timezone, note, is_sync
            ))
            connection.commit()
            print("Attendance log registered successfully")
    except Exception as e:
        print(f"Error: Unable to register attendance - {e}")
    finally:
        cursor.close()

def main():
    # Get PostgreSQL Database Configuration
    postgres_config = get_postgres_config()

    # Connect to PostgreSQL
    postgres_connection = connect_to_postgresql(postgres_config)

    if not postgres_connection:
        return

    # Get device configuration from the configuration file
    try:
        device_config = get_device_config()
    except KeyError as e:
        print(f"Error: {e}")
        return

    device_ip = device_config.get('device_ip', 'localhost')
    device_port = int(device_config.get('device_port', 4370))

    try:
        # Connect to ZKTeco Device
        zk = ZK(device_ip, port=device_port, timeout=5)
        zk.connect()
        zk.enable_device()

        # Get attendance logs from ZKTeco Device
        attendance_logs = zk.get_attendance()

        # Register attendance logs to PostgreSQL
        for log in attendance_logs:
            talenteo_id = log.user_id
            timezone = "Europe/London" 
            note = "Punch note" 
            datetime = log.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            is_sync = False

            register_attendance_to_database(
                postgres_connection,
                talenteo_id,
                datetime,
                timezone,
                note,
                is_sync
            )

    finally:
        # Disconnect from ZKTeco Device
        zk.disconnect()

        # Close PostgreSQL connection
        postgres_connection.close()

if __name__ == "__main__":
    main()
