import requests

def get_access_token(api_url, client_id, client_secret):
    token_url = f"{api_url}/oauth/issueToken"

    payload = {
        "grant_type":"client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    try:
        response = requests.post(token_url, data=payload)
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.exceptions.HTTPError as errh : 
        print(f"HTTP Error : {errh}")
    except requests.exceptions.ConnectionError as errc : 
        print(f"HTTP Error : {errc}")
    except requests.exceptions.Timeout as errt : 
        print(f"HTTP Error : {errt}")
    except requests.exceptions.RequestException as err : 
        print(f"HTTP Error : {err}")