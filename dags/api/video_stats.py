import requests
import json
from datetime import date

# import os
# from dotenv import load_dotenv
# load_dotenv(dotenv_path='./.env')

from airflow.decorators import task
from airflow.models import Variable


API_KEY = Variable.get('API_KEY')

CHANNEL_HANDLE=Variable.get('CHANNEL_HANDLE')

maxResults =50

nextPageToken = None

@task
def get_playlist_id():

    try:

        url=f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}' \


        response = requests.get(url)

        response.raise_for_status()  # Check if the request was successful

        data = response.json()

        # print(json.dumps(data, indent=4))

        channel_items = data['items'][0]

        channelPlaylistId= channel_items['contentDetails']['relatedPlaylists']['uploads']

        print(channelPlaylistId)

        return channelPlaylistId
    
    except requests.exceptions.RequestException as e:
        raise e
    


@task
def get_video_ids(playlistId):

    video_ids = []

    pageToken = None

    base_url= f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}'

    try:
        while True:
            url = base_url

            if pageToken:
                url += f'&pageToken={pageToken}'
            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful
            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            pageToken = data.get('nextPageToken')
            if not pageToken:
                break
        return video_ids

    except requests.exceptions.RequestException as e:
        raise e 

    
@task
def extract_video_data(video_ids):

    extracted_data = []
    
    def batch_lists(video_ids, batch_size):
        for i in range(0, len(video_ids), batch_size):
            yield video_ids[i:i + batch_size]

    try:
        for batch in batch_lists(video_ids, maxResults):
            video_ids_strng = ','.join(batch)

            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_strng}&key={API_KEY}'
            
            response = requests.get(url)
            
            response.raise_for_status()  # Check if the request was successful
            
            data = response.json()
            
            for item in data.get('items', []):
                video_id= item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    'video_id': video_id,
                    'title': snippet['title'],
                    'publishedAt': snippet['publishedAt'],
                    'duration': contentDetails['duration'],
                    'viewCount': statistics.get('viewCount', 0),
                    'likeCount': statistics.get('likeCount', 0),
                    'commentCount': statistics.get('commentCount', 0)
                            }
                extracted_data.append(video_data)
        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

@task   
def save_to_json(extracted_data):
    file_path = f'./data/yt_data_{date.today()}.json'

    with open(file_path, 'w', encoding='utf-8') as json_outfile:
        json.dump(extracted_data, json_outfile, ensure_ascii=False, indent=4)

    
if __name__ == "__main__":
    playlistId=get_playlist_id()
    video_ids=get_video_ids(playlistId)
    video_data=extract_video_data(video_ids)
    save_to_json(video_data)