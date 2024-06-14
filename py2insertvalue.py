# %%
import googleapiclient.discovery
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

api_key = "AIzaSyBNJObaoci8Grdmf1V1EfLwtpotae8P_-o"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

engine = sqlalchemy.create_engine('mysql://root:root@localhost:3306/mydb10jun24_2', pool_pre_ping=True)

# %%
def time_duration(t):
    a = pd.Timedelta(t)
    b = str(a).split()[-1]
    return b

# %%
def channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id = channel_id
        #id="UCduIoIMfD8tT3KoU0-zBRgQ"  -->GUVI
        
    )
    response = request.execute()

    data = {
        "channel_id":channel_id,
        "channel_name": response['items'][0]['snippet']['title'],
        "channel_des": response['items'][0]['snippet']['description'],
        "channel_pid":response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        "channel_sub":response['items'][0]['statistics']['subscriberCount'],
        "channel_viewc":response['items'][0]['statistics']['viewCount'],
        "channel_videoC":response['items'][0]['statistics']['videoCount'],
        "channel_pat":response['items'][0]['snippet']['publishedAt']
    }
    print(data)
    channeldata = pd.DataFrame([data])
    return channeldata

# %%
def video_data(video_id):
    request = youtube.videos().list(
        part="contentDetails,snippet,statistics",
        id = video_id,
        maxResults = 5
        
    )
    response = request.execute()

    data = {
            'video_id': response['items'][0]['id'],
            'video_name': response['items'][0]['snippet']['title'],
            'video_description': response['items'][0]['snippet']['description'],
            #'tags': response['items'][0]['snippet'].get('tags', []),
            'published_date': response['items'][0]['snippet']['publishedAt'][0:10],
            'published_time': response['items'][0]['snippet']['publishedAt'][11:19],
            'view_count': response['items'][0]['statistics']['viewCount'],
            'like_count': response['items'][0]['statistics'].get('likeCount', 0),
            'favourite_count': response['items'][0]['statistics'].get('favoriteCount',0),
            'comment_count': response['items'][0]['statistics'].get('commentCount', 0),
            'duration': time_duration(response['items'][0]['contentDetails']['duration']),
            'thumbnail': response['items'][0]['snippet']['thumbnails']['default']['url'],
            'caption_status': [response['items'][0]['contentDetails']['caption']],
            'channel_id': response['items'][0]['snippet']['channelId']
                }
    return data

# %%
def playlist_data(channel_id):
    request = youtube.playlists().list(
        part="snippet",
        channelId = channel_id,
        maxResults = 5
        
    )
    response = request.execute()

    playlistdata = pd.DataFrame()
    #playlistdata={}

    for i in range (len(response['items'])):
        data = {
            'playlistid': response['items'][i]['id'],
            'playlistname': response['items'][i]['snippet']['title'],
            'channelid': response['items'][i]['snippet']['channelId']
                }
        #print(data)
        bufferdata = pd.DataFrame([data])
        playlistdata = pd.concat([playlistdata,bufferdata])
        #playlistdata.update(data)
        
    return playlistdata

# %%
def comment_data(video_id):
    request = youtube.commentThreads().list(
    part="id,snippet,replies",
    videoId = video_id,
    maxResults = 5
        
    )

    try:
        response = request.execute()
        commentdata={}

        for i in range (len(response['items'])):
            data = {
                'comment_id': response['items'][i]['id'],
                'video_id': response['items'][i]['snippet']['videoId'],
                'comment_text': response['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                'comment_author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment_pdate' : response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                'channel_id': response['items'][i]['snippet']['channelId']
                    }
            commentdata.update(data)

        return commentdata
    
    except googleapiclient.errors.HttpError as e:
        #print(e.error_details[0]["reason"])
        if e.error_details[0]["reason"] == 'commentsDisabled':
            print("Comments are Disabled for the video Id")
            return None
        
    

# %%
def search_data(channel_id):
    request = youtube.search().list(
    part="id",
    channelId= channel_id,
    maxResults = 5
    )
    response = request.execute()

    videodata = pd.DataFrame()
    commentdata = pd.DataFrame()
    
    for i in range(len(response['items'])):
      for j in response['items']:
          if 'videoId' in j['id'].keys():
            data = video_data(j['id']['videoId'])
            #print(data)
            bufferdata = pd.DataFrame([data])
            videodata = pd.concat([videodata,bufferdata])

            cmnt_data = comment_data(j['id']['videoId'])
            #print(cmnt_data)
            bufferdata2 = pd.DataFrame([cmnt_data])
            #print(bufferdata2)
            commentdata = pd.concat([commentdata,bufferdata2])

    return videodata, commentdata

# %%
exitmode = 'append'

def extract_channelData(channel_id):
    channeldf = channel_data(channel_id)
    channeldf.to_sql(name='channel', con=engine, if_exists=exitmode, index=False)

def extract_videocommentData(channel_id):

    videodf, commentdf = search_data(channel_id)
    videodf.to_sql(name='video', con=engine, if_exists=exitmode, index=False)
    commentdf.to_sql(name='comment', con=engine, if_exists=exitmode, index=False)

def extract_playlistData(channel_id):
    playlistdf = playlist_data(channel_id)
    playlistdf.to_sql(name='playlist', con=engine, if_exists=exitmode, index=False)


