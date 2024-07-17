from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import random
import pyodbc
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime,timedelta
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def pull_trending_video() : 
    api_key = os.getenv('api_key')
    if not api_key:
        raise ValueError("API key not found. Please set the API key in the .env file.")
    youtube = build('youtube','v3',developerKey= api_key)
    trending_videos = []
    total_video = 0
    count = 0
    try :
        next_page_token = None
        while True : 
            trendingVideo = youtube.videos().list(
                part='contentDetails,statistics,topicDetails,snippet',
                chart='mostPopular',
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            trending_videos.extend(trendingVideo['items'])
            total_video += len(trendingVideo['items'])

            next_page_token = trendingVideo.get('nextPageToken', None)
            count += 1
            print(f"Page {count}: Processed {total_video} videos")

            if next_page_token is None:
                break

            time.sleep(random.randint(5, 10))
    except HttpError as e:
        print(f"An HTTP error occurred: {e}")

    return trending_videos


def extract_details(ti) : 
    videos = ti.xcom_pull(task_ids='pull_trending_video')
    extracted_videos = []
    for video in videos : 
        video_info = {}
        video_info['id'] = video.get('id',None)
        
        video_info['publishedAt'] = video.get('snippet').get('publishedAt')
        video_info['channelId'] = video.get('snippet').get('channelId')
        video_info['title'] = video.get('snippet').get('title')
        video_info['description'] = video.get('snippet').get('description')
        video_info['channelTitle'] = video.get('snippet').get('channelTitle')
        video_info['tags'] = video.get('snippet').get('tags')
        video_info['categoryId'] = video.get('snippet').get('categoryId')

        video_info['licensedContent'] = video.get('contentDetails').get('licensedContent')
        video_info['blockRegion'] = video.get('contentDetails').get('regionRestriction')
        video_info['caption'] = video.get('contentDetails').get('caption')
        video_info['definition'] = video.get('contentDetails').get('definition')


        video_info['viewCount'] = video.get('statistics').get('viewCount')
        video_info['likeCount'] = video.get('statistics').get('likeCount')
        video_info['favoriteCount'] = video.get('statistics').get('favoriteCount')
        video_info['commentCount'] = video.get('statistics').get('commentCount')

        extracted_videos.append(video_info)
    
    return extracted_videos

def fillna(ti) : 
    df = pd.DataFrame(ti.xcom_pull(task_ids='extract_details'))
    df['viewCount'] = df['viewCount'].fillna('0')
    df['likeCount'] = df['likeCount'].fillna('0')
    df['favoriteCount'] = df['favoriteCount'].fillna('0')
    df['commentCount'] = df['commentCount'].fillna('0')
    return df


def preprocess(ti) : 
    df = ti.xcom_pull(task_ids='fillna')
    df['viewCount'] = df['viewCount'].astype(int)
    df['likeCount'] = df['likeCount'].astype(int)
    df['favoriteCount'] = df['favoriteCount'].astype(int)
    df['commentCount'] = df['commentCount'].astype(int)
    df['tags'] = df['tags'].apply(lambda row: ' '.join(row) if row is not None else '')
    df['blockRegion'] = df['blockRegion'].apply(lambda row : row.get('block') if row is not None else None)
    dict_category = {1 : 'Film & Animation', 2 : 'Autos & Vehicles', 10 : 'Music', 15 : 'Pets & Animals', 17 : 'Sports', 18 : 'Short Movies', 19 : 'Travel & Events',
                        20 : 'Gaming', 21 : 'Videoblogging', 22 : 'People & Blogs', 23 : 'Comedy', 24 : 'Entertainment', 25 : 'News & Politics', 26 : 'Howto & Style', 27 : 'Education',
                        28 : 'Science & Technology', 29 : 'Nonprofits & Activism', 30 : 'Movies', 31 : 'Anime/Animation', 32 : 'Action/Adventure', 33 : 'Classics', 34 : 'Comedy',
                        35 : 'Documentary', 36 : 'Drama', 37 : 'Family', 38 : 'Foreign', 39 : 'Horror', 40 : 'Sci-Fi/Fantasy', 41 : 'Thriller', 42 : 'Shorts', 43 : 'Shows', 44 : 'Trailers'}
    df['categoryId'] = df['categoryId'].astype(int)
    df['category'] = df['categoryId'].map(dict_category)
    return df
    

def write_to_sql_server(ti) : 
    df = ti.xcom_pull(task_ids='preprocess')
    connection_string = (
        f"mssql+pyodbc://@DESKTOP-301A075\\DATA_WAREHOUSE,1433;/data_warehouse?driver=SQL+Server&trusted_connection=yes"
    )

    engine = create_engine(connection_string)

    table_name = 'trending_video' 
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"DataFrame written to table {table_name} successfully.")
    except Exception as e:
        print("Error writing DataFrame to SQL Server:", e)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 9),
    'retries': 1,
}
dag = DAG(
    'youtube_trending_dag',
    default_args=default_args,
    description='A DAG to pull trending videos from YouTube and store them in SQL Server',
    schedule_interval=timedelta(hours=6),
)

pull_trending_video_task = PythonOperator(
    task_id='pull_trending_video',
    python_callable=pull_trending_video,
    provide_context=True,
    dag=dag,
)

extract_details_task = PythonOperator(
    task_id='extract_details',
    python_callable=extract_details,
    provide_context=True,
    dag=dag,
)
fillna_task = PythonOperator(
    task_id='fillna',
    python_callable=fillna,
    provide_context=True,
    dag=dag,
)
preprocess_task = PythonOperator(
    task_id='preprocess',
    python_callable=preprocess,
    provide_context=True,
    dag=dag,
)

write_to_sql_server_task = PythonOperator(
    task_id='write_to_sql_server',
    python_callable=write_to_sql_server,
    provide_context=True,
    dag=dag,
)

pull_trending_video_task >> extract_details_task >> fillna_task >> preprocess_task >> write_to_sql_server_task
