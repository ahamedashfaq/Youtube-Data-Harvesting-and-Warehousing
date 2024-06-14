# %%
import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root"
)

mycursor = mydb.cursor()

mycursor.execute(
    """CREATE DATABASE mydb10jun24_3"""
)

mycursor.execute(
    """USE mydb10jun24_3"""
)


channeltb_query = """CREATE TABLE IF NOT EXISTS channel (channel_id VARCHAR(255), channel_name VARCHAR(255), channel_des TEXT, 
    channel_pid VARCHAR(255), channel_sub INTEGER,channel_viewCount INTEGER, channel_videoCount INTEGER, channel_pat DATETIME)"""
mycursor.execute(channeltb_query)


videotb_query = """CREATE TABLE IF NOT EXISTS video (video_id VARCHAR(255), video_name VARCHAR(255), 
    video_description TEXT, published_date DATETIME, published_time DATETIME, view_count INT, like_count INT,  favorite_count INT,
    comment_count INT, duration INT, thumbnail VARCHAR(255), caption_status VARCHAR(255),channel_id VARCHAR(255))"""
mycursor.execute(videotb_query)

playlisttb_query = 'CREATE TABLE IF NOT EXISTS playlist (playlist_id VARCHAR(255), channel_id VARCHAR(255), playlist_name VARCHAR(255))'
mycursor.execute(playlisttb_query)

commenttb_query = """CREATE TABLE IF NOT EXISTS comment (comment_id VARCHAR(255), video_id VARCHAR(255), comment_text TEXT, comment_author VARCHAR(255),
    comment_published_date DATETIME, channel_id VARCHAR(255))"""
mycursor.execute(commenttb_query)

mydb.commit()
mycursor.close()
mydb.close()
