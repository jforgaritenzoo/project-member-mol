import requests, pika, os, logging


def fetch_api_logs():
    url = "http://127.0.0.1:8888/log"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP error codes
        response_json = response.json()

        if response_json.get("status") == "success" and "data" in response_json:
            return response_json["data"]
        else:
            return []
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return []
    
def runs_script(job_name):
    url = f"http://127.0.0.1:8888/runscript?job_name={job_name}"  # Update with your API endpoint
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP error codes
        response_json = response.json()

        if response_json.get("status") == "success" and "logs" in response_json:
            return {
                "message": response_json["message"],
                "logs": response_json["logs"],
            }
        else:
            # Return an error message if the status is not success or logs are missing
            return {
                "message": "Failed to fetch logs or logs are missing",
                "logs": [],
            }
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return {
            "message": "Error fetching data from API",
            "logs": [],
        }



