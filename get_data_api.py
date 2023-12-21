import requests
import configparser
from datetime import date
from api_helper import get_access_token

# read the configuration from the config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Talenteo API Configuration
api_url = config.get('TalenteoAPI', 'api_url')
client_id = config.get('TalenteoAPI', 'client_id')
client_secret = config.get('TalenteoAPI', 'client_secret')

def get_employee_attendance(employee_id, attendance_date):
    attendance_url = f"{api_url}/api/v1/employee/{employee_id}/punch"
    headers = {
        'Authorization': f"Bearer {get_access_token(api_url, client_id, client_secret)}",
        'Content-Type': 'application/json',
    }
    attendance_payload = {
        'datetime': attendance_date,
    }

    try:
        response = requests.get(attendance_url, headers=headers, json=attendance_payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting employee attendance: {e}")
        return None

# Example usage:
employee_id = "1001"

attendance_date = "2023-12-20"

attendance_data = get_employee_attendance(employee_id, attendance_date)
if attendance_data and 'data' in attendance_data:
    employee_attendance = attendance_data['data']
    print(f"Employee Attendance for ID {employee_id} on {attendance_date}:")
    print(employee_attendance)
else:
    print(f"Error getting employee attendance: {attendance_data}")