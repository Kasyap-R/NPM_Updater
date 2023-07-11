import requests
import time
import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

MAX_RETRIES = 3
RETRY_DELAY = 0.1 

MAX_PACKAGES = 50
curr_package = 0


def update_and_clone_package(package_name):
    global curr_package
    shared_link = "https://registry.npmjs.org/"
    url = f"{shared_link}{package_name}"
    time.sleep(5)
    for attempt in range(MAX_RETRIES):
        try:
            with requests.get(url) as response:
                if response.status_code != 200:
                    raise Exception(f"HTTP error {response.status_code}")
                package_data = json.loads(response.text)
                # Update Data Here
                return
        except Exception as e:
            if attempt < MAX_RETRIES - 1:  # i.e. if not the last attempt
                time.sleep(RETRY_DELAY)
            else:
                print(f"Error: {package_name} {e}")
                with open('./Error_Messages/package_retrieval_errors.txt', 'a') as f:
                    f.write(f"Error: {package_name} {e}\n")
                return
            
def main(since):
    global curr_package
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = []
        while True:
            try:
                response = requests.get(
                    f'https://replicate.npmjs.com/_changes?since={since}&feed=continuous&timeout=10000', stream=True)
                response.raise_for_status()  # raise exception if invalid response
                for line in response.iter_lines():
                    if line:
                        if curr_package > MAX_PACKAGES:
                            print("MAX PACKAGE COUNT EXCEEDED")
                            executor.shutdown(wait=True)
                            return
                        change = json.loads(line)
                        if 'deleted' in change and change['deleted'] is True:
                            # add code to deal with deletion here
                            continue
                        if 'id' in change:
                            since = change['seq']
                            print(change['id'], flush=True)
                            future = executor.submit(update_and_clone_package, change["id"])
                            futures.append(future)
                            curr_package += 1

                # # Check if any of the futures have completed
                for future in as_completed(futures):
                    if future.result():
                        curr_package += 1
                    futures.remove(future)
            except requests.exceptions.RequestException as e:
                print(f'Error: {e}. Retrying in 5 seconds.')
                time.sleep(5)  # wait for 5 seconds before retrying

if __name__ == '__main__':
    main("now")