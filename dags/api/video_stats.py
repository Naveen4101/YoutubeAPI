import requests
import json

from datetime import date
from airflow.decorators import task
from airflow.models import Variable
#import os
#from dotenv import load_dotenv
#load_dotenv(dotenv_path="./.env")
#API_KEY=os.getenv('API_KEY')
#CHANNEL_HANDLE="MrBeast"
API_KEY=Variable.get('API_KEY')
CHANNEL_HANDLE=Variable.get('CHANNEL_HANDLE')
max_results=50
@task
def get_playlistID():
    try:
        url=f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response=requests.get(url)
        response.raise_for_status()
        data=response.json()
        #print(json.dumps(data,indent=4))
        channel_items=data["items"][0]
        channel_playlists=channel_items["contentDetails"]["relatedPlaylists"]['uploads']
        #print(channel_playlists)
        return channel_playlists
    except requests.exceptions.RequestException as e:
        raise e
@task
def get_video_id(playlistid):
    try:
        base_url=f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlistid}&key={API_KEY}"
        video_ids=[]
        Page_token=None
        while True:
            url=base_url
            if Page_token:
                url+= f"&pageToken={Page_token}"
            response=requests.get(base_url)
            response.raise_for_status()
            data=response.json()
            for item in data.get('items',[]):
                video_id=item['contentDetails']['videoId']
                video_ids.append(video_id)
            Page_token=data.get('nextPageToken')
            if not Page_token:
                break
            return video_ids

    except requests.exceptions.RequestException as e:
        raise e
@task
def extract_video_data(video_ids):
    extracted_data=[]
    def batch_list(video_ids_lst,batch_size):
            for video_id in range(0,len(video_ids_lst),batch_size):
                yield video_ids_lst[video_id : video_id + batch_size ]
    try:
        for batch in batch_list(video_ids,max_results):
            video_ids_str= ",".join(batch)
            url=f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            response=requests.get(url)
            response.raise_for_status()
            data=response.json()
            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                }
                extracted_data.append(video_data)
        return extracted_data
    except requests.exceptions.RequestException as e:
        raise e
@task
def save_to_json(video_data):
    file_path=f"./data/Youtube_data_{date.today()}.json"
    with open(file_path,"w",encoding='utf-8') as json_output_file:
        json.dump(video_data,json_output_file,indent=4,ensure_ascii=False)


if __name__=='__main__':
    playlistid=get_playlistID()
    video_ids=get_video_id(playlistid)
    video_data=extract_video_data(video_ids)
    save_to_json(video_data)
