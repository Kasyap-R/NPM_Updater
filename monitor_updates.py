import requests
import time
import json
from concurrent.futures import ProcessPoolExecutor, as_completed

# Constants to manage retries and packages to process
MAX_RETRIES = 3
RETRY_DELAY = 0.1
MAX_PACKAGES = 50

# Global variable to keep track of the current package count
curr_package = 0

# This function is used to get package details and clone the package
def update_package(package_name):
    global curr_package
    shared_link = "https://registry.npmjs.org/"
    url = f"{shared_link}{package_name}"
    time.sleep(5)  # Delay to respect rate limits
    
    # Try to retrieve the package details with retries for failures
    for attempt in range(MAX_RETRIES):
        try:
            with requests.get(url) as response:
                if response.status_code != 200:
                    raise Exception(f"HTTP error {response.status_code}")
                package_data = json.loads(response.text)
                # Update Data Here
                return
        except Exception as e:
            # If not the last attempt, retry after a delay
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"Error: {package_name} {e}")
                with open('./Error_Messages/package_retrieval_errors.txt', 'a') as f:
                    f.write(f"Error: {package_name} {e}\n")
                return

# Main function to manage the overall operation
def main(since):
    global curr_package
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = []
        
        # Continuous loop to keep checking for package updates
        while True:
            try:
                # Fetch the package changes feed
                response = requests.get(
                    f'https://replicate.npmjs.com/_changes?since={since}&feed=continuous&timeout=10000', stream=True)
                response.raise_for_status()  # Raise an exception if invalid response
                
                # Process each line in the feed
                for line in response.iter_lines():
                    if line:
                        # If maximum package limit is reached, shutdown executor and return
                        if curr_package > MAX_PACKAGES:
                            print("MAX PACKAGE COUNT EXCEEDED")
                            executor.shutdown(wait=True)
                            return
                            
                        change = json.loads(line)
                        if 'deleted' in change and change['deleted'] is True:
                            # add code to deal with deletion here
                            continue
                        if 'id' in change:
                            # Fetch details of the new package and submit the task to the executor
                            since = change['seq']
                            print(change['id'], flush=True)
                            future = executor.submit(update_package, change["id"])
                            futures.append(future)

                # Check if any of the futures have completed
                for future in as_completed(futures):
                    if future.result():
                        curr_package += 1
                    futures.remove(future)
                    
            except requests.exceptions.RequestException as e:
                print(f'Error: {e}. Retrying in 5 seconds.')
                time.sleep(5)  # Wait for 5 seconds before retrying

if __name__ == '__main__':
    main("now")
