import mysql.connector
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text


engine = sqlalchemy.create_engine('mysql://root:root@localhost:3306/mydb10jun24_2', pool_pre_ping=True,pool_size=20, max_overflow=0)

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root"
)

mycursor = mydb.cursor()

mycursor.execute( """USE mydb10jun24_2""")



def getChannelname():
    query = "select * from channel"
    data = pd.read_sql(query, engine)
    return data[['channel_name']]

def channelname(channel_name):
    query = "select * from channel where channel_name = '{}'".format(channel_name)
    #data = pd.read_sql(query, engine)
    #query = "select * from channel"
    cdata = pd.read_sql(query, engine)
    return cdata.T

def Query1():
    query = """select channel.channel_name AS CHANNEL_NAME,  video.video_name AS VIDEO_NAME
            FROM VIDEO join channel on channel.channel_id = video.channel_id"""
    Q1data = pd.read_sql(query, engine)
    return Q1data

def Query2():
    query = """SELECT channel_name AS CHANNEL_NAME, channel_videoC AS VIDEO_COUNT FROM CHANNEL
                ORDER BY CAST(channel_videoC AS SIGNED) DESC"""
    Q2data = pd.read_sql(query, engine)
    return Q2data

def Query3():
    query = """
                SELECT ANY_VALUE(channel.channel_name) AS CHANNEL_NAME, video.video_name, ANY_VALUE(video.view_Count) AS VIDEO_COUNT
                FROM video
                LEFT JOIN channel
                ON channel.channel_id = video.channel_id
				        GROUP BY video_name
                ORDER BY CAST(ANY_VALUE(video.view_Count) AS SIGNED) DESC
                LIMIT 10
                """
    Q3data = pd.read_sql(query, engine)
    return Q3data

def Query4():
    query = """
                SELECT video_name as VIDEO_NAME, ANY_VALUE(comment_count) AS COMMENT_COUNT from video
                GROUP BY video_name
                ORDER BY CAST(ANY_VALUE(comment_count) AS SIGNED) DESC
                """
    Q4data = pd.read_sql(query, engine)
    return Q4data

def Query5():
    query = """
                SELECT ANY_VALUE(channel.channel_name) as CHANNEL_NAME, video.video_name as VIDEO_NAME, ANY_VALUE(like_count) AS COMMENT_COUNT
                FROM video
                RIGHT JOIN channel
                ON channel.channel_id = video.channel_id
                GROUP BY video_name
                ORDER BY CAST(ANY_VALUE(like_count) AS SIGNED) DESC
                """
    Q5data = pd.read_sql(query, engine)
    return Q5data

def Query6():
    query = """
                SELECT video_name as VIDEO_NAME, like_count as LIKE_COUNT 
                FROM video
                """
    Q6data = pd.read_sql(query, engine)
    return Q6data

def Query7():
    query = """
                SELECT channel_name as CHANNEL_NAME, channel_viewc as CHANNEL_VIEW_COUNT
                FROM channel
                ORDER BY CAST(channel_viewc AS SIGNED) DESC
                """
    Q7data = pd.read_sql(query, engine)
    return Q7data

def Query8():
    query = """
                SELECT ANY_VALUE(channel.channel_name) as CHANNEL_NAME, video.video_name AS VIDEO_NAME, 
                ANY_VALUE(video.published_Date) AS PUBLISHED_DATE
                FROM video
                INNER JOIN channel
                ON channel.channel_id = video.channel_id
                WHERE published_Date like '%2022%'
                GROUP BY video_name;
                """
    Q8data = pd.read_sql(text(query), engine)
    return Q8data

def Query9():
    query = """
                SELECT channel.channel_name, ANY_VALUE(video.video_name), AVG(TIME_TO_SEC(video.duration))
                FROM video
                LEFT JOIN channel
                ON channel.channel_id = video.channel_id
                GROUP BY channel_name
                """
    Q9data = pd.read_sql(query, engine)
    return Q9data

def Query10():
    query = """
                SELECT ANY_VALUE(channel.channel_name) as CHANNEL_NAME, video_name as VIDEO_NAME, ANY_VALUE(comment_count) AS COMMENT_COUNT 
                FROM video
                INNER JOIN channel
                ON channel.channel_id = video.channel_id
                GROUP BY video_name
                ORDER BY CAST(ANY_VALUE(comment_count) AS SIGNED) DESC
                """
    Q10data = pd.read_sql(query, engine)
    return Q10data
    


mydb.commit()
mycursor.close()
mydb.close()
