# WORKS

import requests
from datetime import date

# Talenteo API Configuration
talenteo_api_config = {
    'base_url': 'https://reporting.dev.talenteo.com',
    'client_id': 'spintechs',
    'client_secret': 'RXb141ioB1ry2vbTZvEB',
}

def get_access_token():
    token_url = f"{talenteo_api_config['base_url']}/oauth/issueToken"
    token_payload = {
        'grant_type': 'client_credentials',
        'client_id': talenteo_api_config['client_id'],
        'client_secret': talenteo_api_config['client_secret'],
    }

    try:
        response = requests.post(token_url, json=token_payload)
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.RequestException as e:
        print(f"Error obtaining access token: {e}")
        return None

def get_employee_attendance(employee_id, attendance_date):
    attendance_url = f"{talenteo_api_config['base_url']}/api/v1/employee/{employee_id}/punch"
    headers = {
        'Authorization': f"Bearer {get_access_token()}",
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
# employee_id = "1000" #tlnteo ID = 
employee_id = "1001" #tlnteo ID = 9
# employee_id = "1002" #tlnteo ID = 8
# employee_id = "1003" #tlnteo ID = 
# employee_id = "1004" #tlnteo ID = 
# employee_id = "1005" #tlnteo ID = 
# employee_id = "1006" #tlnteo ID = 6
# employee_id = "1007" #tlnteo ID = 
# employee_id = "1008" #tlnteo ID = 
# attendance_date = "2023-11-22"  # Replace with the desired date in the format 'YYYY-MM-DD'
attendance_date = "2023-12-20"  # Replace with the desired date in the format 'YYYY-MM-DD'

attendance_data = get_employee_attendance(employee_id, attendance_date)
if attendance_data and 'data' in attendance_data:
    employee_attendance = attendance_data['data']
    print(f"Employee Attendance for ID {employee_id} on {attendance_date}:")
    print(employee_attendance)
else:
    print(f"Error getting employee attendance: {attendance_data}")