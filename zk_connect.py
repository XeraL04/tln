import psycopg2
import configparser
import logging
from datetime import datetime
from zk import ZK

# Set up logging configuration at the beginning of the script
logging.basicConfig(level=logging.DEBUG)

# Function to configure and get a logger
def setup_logger(name, level, file_name):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(file_name)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

# Create loggers
business_logger = setup_logger('business_log_zk', logging.INFO, 'business_log.log')
debug_logger = setup_logger('debug_log_zk', logging.DEBUG, 'debug_log.log')

# Function to read the configuration
def read_config(file_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

# Function to get device configuration
def get_device_config():
    config = read_config()
    logging.debug('Sections in config: %s', config.sections())
    for section in config.sections():
        if section.lower() == 'device1':
            return config[section]
    raise KeyError('Device section not found in the configuration file')

# Function to get PostgreSQL configuration
def get_postgres_config():
    config = read_config()
    return config['Postgres']

# Function to connect to PostgreSQL
def connect_to_postgresql(postgres_config):
    try:
        connection = psycopg2.connect(**postgres_config)
        logging.info("Connected to Database")
        return connection
    except Exception as e:
        logging.error("Error: Unable to connect to the database - %s", e)
        return None

# Function to register attendance to the database
def register_attendance_to_database(connection, talenteo_id, datetime, timezone, note, is_sync):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM attendance
                WHERE employee_id = %s AND datetime = %s;
            """, (talenteo_id, datetime))
            existing_log = cursor.fetchone()

            if existing_log:
                business_logger.info("Attendance log already exists. Skipping registration.")
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
                """, (talenteo_id, datetime, timezone, note, is_sync))
                connection.commit()
                business_logger.info("Attendance log registered successfully")
    except Exception as e:
        logging.error("Error: Unable to register attendance - %s", e)

# Main function
def main():
    postgres_config = get_postgres_config()
    postgres_connection = connect_to_postgresql(postgres_config)

    if not postgres_connection:
        return

    try:
        device_config = get_device_config()
    except KeyError as e:
        logging.error("Error: %s", e)
        return

    device_ip = device_config.get('device_ip', 'localhost')
    device_port = int(device_config.get('device_port', 4370))

    try:
        zk = ZK(device_ip, port=device_port, timeout=5)
        zk.connect()
        zk.enable_device()

        attendance_logs = zk.get_attendance()

        for log in attendance_logs:
            talenteo_id = log.user_id
            timezone = "Europe/London" 
            note = "Punch note" 
            log_datetime = log.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            is_sync = False

            register_attendance_to_database(
                postgres_connection,
                talenteo_id,
                log_datetime,
                timezone,
                note,
                is_sync
            )

    finally:
        zk.disconnect()
        postgres_connection.close()

if __name__ == "__main__":
    main()
