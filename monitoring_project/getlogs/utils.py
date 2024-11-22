import requests

def fetch_data_from_api():
    url = "http://127.0.0.1:8888/log"  # Replace with your API endpoint
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP error codes
        response_json = response.json()
        
        # Ensure we have the expected structure
        if response_json.get("status") == "success" and "data" in response_json:
            return response_json["data"]  # Extract the list of job entries
        else:
            return []  # Return an empty list if structure is unexpected
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return []  # Return an empty list on error
