import json
import requests
from queue import Queue
from threading import Thread
from retrying import retry

base_url = "https://prod-api.kosetto.com"

num_threads = 32
queue_size = 200000

download_queue = Queue(queue_size)

        
# fetch base_url/users/by-id/[ID]
@retry(stop_max_attempt_number=3,wait_random_min=1000, wait_random_max=2000)
def retry_fetch_item(item):
    url = base_url + "/users/by-id/" + str(item)
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        res = response.json()
        with open('out.csv', 'a+') as f:
            # {"id": 17, "address": "0xa46d765e2410450ff08a9a99317281c462f27120", "twitterUsername": "Crypt0Panda", "twitterName": "Busy Panda (On Chain Idiot) \u269b\ufe0f\u26a1\ufe0f", "twitterPfpUrl": "https://pbs.twimg.com/profile_images/1672531058091515905/pXp1A98s.jpg", "twitterUserId": "1371362738937556995", "lastOnline": 1691754149290, "lifetimeFeesCollectedInWei": "0"}
            f.write(f"{res['id']},{res['address']},{res['twitterUsername']},{res['twitterName']},{res['twitterPfpUrl']},{res['twitterUserId']},{res['lastOnline']},{res['lifetimeFeesCollectedInWei']}\n")
    # elif response.status_code == 404:
    #     print(f"Error fetching: {url} - {response.status_code}")
    else:
      print(f"retry fetching: {url} - {response.status_code}")
      raise Exception(f"Error fetching: {url} - {response.status_code}")

def download_worker():
    while True:
        item = download_queue.get()
        try:
            retry_fetch_item(item)
        except Exception as e:
            print(e)

        download_queue.task_done()


for i in range(num_threads):
    t = Thread(target=download_worker)
    t.daemon = True
    t.start()

with open('out.csv', 'w+') as f:
   f.write(f"id,address,twitterUsername,twitterName,twitterPfpUrl,twitterUserId,lastOnline,lifetimeFeesCollectedInWei\n")
  
for item in range(11,queue_size):
    download_queue.put(item)

download_queue.join()

print("Finish.")