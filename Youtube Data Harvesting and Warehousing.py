#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from googleapiclient. discovery import build
from pprint import pprint
import mysql.connector
import streamlit as st
import pandas as pd

api_key='AIzaSyDjVfG-8etno7yz-pJzRoOwRshdEHjxFDM'

api_service_name = "youtube"
api_version = "v3"
youtube = build(
        api_service_name, api_version, developerKey=api_key)
# Getting channel details
def get_channel_info(channel_address):
    request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_address
            )
    response = request.execute()
        
    for i in response["items"]:
                data=dict(Channel_Name= i["snippet"]["title"],
                          Channel_id=i["id"],
                          Subscription_Count=i["statistics"]["subscriberCount"],
                          Channel_views=i["statistics"]["viewCount"],
                          Channel_Description=i["snippet"]["description"],
                          Video_Count=i["statistics"]["videoCount"],
                          playlist_ID=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


#getting Video details
def get_videos_ids(channel_address):
    video_ids=[]
    response = youtube.channels().list(id=channel_address,
                                      part="contentDetails").execute()
    Playlist_ID=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    
    while True:
        response1=youtube.playlistItems().list(
                                           part='snippet',
                                           playlistId=Playlist_ID,
                                            maxResults=50,
                                            pageToken= next_page_token).execute()
    
    
        for i in range (len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

#getting video details
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
            request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=video_id
            )
            response=request.execute()
            for i in response['items']:
                data=dict(channel_id=i['snippet']['channelId'],
                      channel_Name=i['snippet']['channelTitle'],
                      video_id=i['id'],
                      video_name=i['snippet']['title'],
                      video_description=i['snippet'].get('description'),
                      thumbnail=i['snippet']['thumbnails']['default']['url'],
                      tags=i['snippet'].get('tags'),
                      publishedAt=i['snippet']['publishedAt'],
                      view_count=i['statistics'].get('viewCount'), 
                      like_count=i['statistics'].get('likeCount'),
                      fav_count=i['statistics']['favoriteCount'],
                      comment_count=i['statistics'].get('commentCount'),
                      duration=i['contentDetails']['duration'],
                      video_definition=i['contentDetails']['definition'],
                      caption=i['contentDetails']['caption'])
            video_data.append(data)
    return video_data

#getting comments details
def get_comment_info(video_ids):
    comments_data=[]
    try:
        for video_id in video_ids:
                request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=50
                )
                response=request.execute()
            
                for i in response['items']:
                      data=dict(Comment_Id=i['snippet']['topLevelComment']['id'],
                                Video_Id=i['snippet']['topLevelComment']['snippet']['videoId'],
                                Comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_Author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_PublishedAt=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                      comments_data.append(data)
    except:
        pass
    return comments_data

import pymongo
client=pymongo.MongoClient('mongodb://127.0.0.1:27017/')
db=client["Youtube_data"]

def channel_details(channel_address):
    channel_info=get_channel_info(channel_address)
    vid_id=get_videos_ids(channel_address)
    vid_info=get_video_info(vid_id)
    comment_info=get_comment_info(vid_id)

    coll1=db["channel_details"]
    coll1.insert_one({"Channel_Information":channel_info,"Video_Id_Information":vid_id, 
                          "Video_Information":vid_info,"Comment_information":comment_info})
    
    return "Upload completed successfully"

#create channel table
import mysql.connector
def insert_channel_data():
    connection=mysql.connector.connect(
         host='localhost',
         user='root',
         password='12345678',
         database='youtube_data'
    )
    cursor=connection.cursor()
    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    
    query="""create table if not exists channels(Channel_Name varchar(255) primary key,
                                    Channel_id varchar(255),
                                    Subscription_Count bigint ,
                                    Channel_views bigint,
                                    Channel_Description text,
                                    Video_Count int,
                                    playlist_ID varchar(255))
                                                """
    
    cursor.execute(query)
    connection.commit()
    ch_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for ch_data in collection.find({},{"_id":0,"Channel_Information":1}):
        ch_list.append(ch_data["Channel_Information"])
    df=pd.DataFrame(ch_list)
    for index,row in df.iterrows():
        insert_query="""insert into channels(Channel_Name,
                                           Channel_id,
                                           Subscription_Count,
                                           Channel_views,
                                           Channel_Description,
                                           Video_Count,
                                           playlist_ID)
                                            values(%s,%s,%s,%s,%s,%s,%s)"""
        values=(row['Channel_Name'],
                row['Channel_id'],
                row['Subscription_Count'],
                row['Channel_views'],
                row['Channel_Description'],
                row['Video_Count'],
                row['playlist_ID'])
    
        try:
            cursor.execute(insert_query,values)
            connection.commit()
            print(f"Inserted data for channel: {row['Channel_Name']}")
        except:
            print(f"Channel values are already inserted")
    cursor.close()
    connection.close()

#create video table
import mysql.connector
from datetime import datetime, timedelta
import pandas as pd
import re
def insert_video_data():
    connection=mysql.connector.connect(
             host='localhost',
             user='root',
             password='12345678',
             database='youtube_data'
        )
    cursor=connection.cursor()
    
    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    connection.commit()
        
    query="""create table if not exists videos(channel_id varchar(255) primary key,
                                                  channel_Name varchar(255),
                                                  video_id varchar(255),
                                                  video_name varchar(255),
                                                  video_description text,
                                                  thumbnail varchar(255),
                                                  tags text,
                                                  publishedAt timestamp,
                                                  view_count int, 
                                                  like_count int,
                                                  fav_count int,
                                                  comment_count int,
                                                  duration time,
                                                  video_definition varchar(10),
                                                  caption varchar(255))
                                                    """
        
    cursor.execute(query)
    connection.commit()
    vid_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for vid_data in collection.find({},{"_id":0,"Video_Information":1}):
        for i in range(len(vid_data['Video_Information'])):
                    vid_list.append(vid_data["Video_Information"][i])
    df1=pd.DataFrame(vid_list)
    for index,row in df1.iterrows():
            tags_str = ",".join(row['tags']) if isinstance(row['tags'], list) else str(row['tags'])
                
            published_at = datetime.strptime(row['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
            duration_iso = row['duration']
            duration_obj = timedelta()
            if 'H' in duration_iso:
                duration_obj += timedelta(hours=int(re.search(r'\d+', duration_iso.split('H')[0]).group()))
            if 'M' in duration_iso:
                duration_obj += timedelta(minutes=int(re.search(r'\d+', duration_iso.split('M')[0]).group()))
            if 'S' in duration_iso:
                duration_obj += timedelta(seconds=int(re.search(r'\d+', duration_iso.split('S')[0]).group()))
            
            duration_mysql = str(duration_obj)
            insert_query="""insert into videos(channel_id,
                                                      channel_Name,
                                                      video_id,
                                                      video_name,
                                                      video_description,
                                                      thumbnail,
                                                      tags,
                                                      publishedAt,
                                                      view_count, 
                                                      like_count,
                                                      fav_count,
                                                      comment_count,
                                                      duration,
                                                      video_definition,
                                                      caption)
                                                      
                                                      values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            
                
            values=(row['channel_id'],
                        row['channel_Name'],
                        row['video_id'],
                        row['video_name'],
                        row['video_description'],
                        row['thumbnail'],
                        tags_str,
                        published_at,
                        row['view_count'],
                        row['like_count'],
                        row['fav_count'],
                        row['comment_count'],
                        duration_mysql,
                        row['video_definition'],
                        row['caption']
                       )
            
            try:
                cursor.execute(insert_query,values)
                connection.commit()
                print(f"Inserted data for video: {row['channel_id']}")
            except:
                print(f"videos values already inserted in the table")
    cursor.close()
    connection.close()

#create comments table

import mysql.connector
def insert_comments_data():
    connection=mysql.connector.connect(
         host='localhost',
         user='root',
         password='12345678',
         database='youtube_data'
    )
    cursor=connection.cursor()
    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    connection.commit()
    
    query="""create table if not exists comments(Comment_Id varchar(255) primary key,
                                                 Video_Id varchar(255),
                                                 Comment_text text,
                                                 Comment_Author varchar(255),
                                                 Comment_PublishedAt timestamp)
                                                         """
    
    cursor.execute(query)
    connection.commit()
    comment_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for comment_data in collection.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(comment_data['Comment_information'])):
            comment_list.append(comment_data["Comment_information"][i])
    df2=pd.DataFrame(comment_list)
    for index,row in df2.iterrows():
        com_published_at = datetime.strptime(row['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        insert_query="""insert into comments(Comment_Id,
                                                     Video_Id,
                                                     Comment_text,
                                                     Comment_Author,
                                                     Comment_PublishedAt)
                                                     
                                                     values(%s,%s,%s,%s,%s)"""
                
        values=(row['Comment_Id'],
                            row['Video_Id'],
                            row['Comment_text'],
                            row['Comment_Author'],
                           com_published_at,
                           )
                
        try:
            cursor.execute(insert_query,values)
            connection.commit()
            print(f"Inserted data for comment: {row['Video_Id']}")
        except:
            print(f"comments values already inserted in the table")
    cursor.close()
    connection.close()

def tables():
    insert_channel_data()
    insert_video_data()
    insert_comments_data()

    return "Table created successfully"

#show channel details
def show_channel_table():
    ch_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for ch_data in collection.find({},{"_id":0,"Channel_Information":1}):
        ch_list.append(ch_data["Channel_Information"])
    df=st.dataframe(ch_list)
    return df

#show vieo details
def show_video_table():
    vid_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for vid_data in collection.find({},{"_id":0,"Video_Information":1}):
        for i in range(len(vid_data['Video_Information'])):
                    vid_list.append(vid_data["Video_Information"][i])
    df1=st.dataframe(vid_list)
    return df1

#show comment details
def show_comment_table():
    comment_list=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for comment_data in collection.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(comment_data['Comment_information'])):
            comment_list.append(comment_data["Comment_information"][i])
    df2=st.dataframe(comment_list)
    return df2

#STREAM LIT
with st.sidebar:
    st.title(":red [YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python scripting")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
channel_id=st.text_input("Enter the channel ID")
if st.button("Collect and Store data"):
    ch_id=[]
    db=client["Youtube_data"]
    collection=db["channel_details"]
    for ch_data in collection.find({},{"_id":0,"Channel_Information":1}):
        ch_id.append(ch_data["Channel_Information"]["Channel_id"])
    if channel_id in ch_id:
        st.success("Channel details of the given channel ID already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Table=tables()
    st.success(tables)
show_table=st.radio("SELECT THE TABLE TO VIEW",("CHANNELS","VIDEOS","COMMENTS"))
if show_table=="CHANNELS":
    show_channel_table()
elif show_table=="VIDEOS":
    show_video_table()
elif show_table=="COMMENTS":
    show_comment_table()

import mysql.connector
connection=mysql.connector.connect(
         host='localhost',
         user='root',
         password='12345678',
         database='youtube_data'
    )
cursor=connection.cursor()
question=st.selectbox("Select your question",("1.All the videos and their corresponding channels",
                                              "2.Channels have the most number of videos",
                                              "3.Top 10 most viewed videos and their respective channels",
                                              "4.Comments were made on each video",
                                              "5.Videos having the highest number of likes",
                                              "6.Total number of likes and dislikes for each video",
                                              "7.Total number of views for each channel",
                                              "8.Channels that have published videos in the year 2022",
                                              "9.Average duration of all videos in each channel",
                                              "10.Videos have the highest number of comments"))

import mysql.connector
connection=mysql.connector.connect(
         host='localhost',
         user='root',
         password='12345678',
         database='youtube_data'
)
cursor=connection.cursor()
if question=="1.All the videos and their corresponding channels":
    query1='''select video_name as videos, channel_Name as channelname from videos'''
    cursor.execute(query1)
    t1=cursor.fetchall()
    df4=pd.DataFrame(t1,columns=["video_title","channel_name"])
    connection.commit()
    st.write(df4)
elif question=="2.Channels have the most number of videos":
    query2='''select channel_Name as channelname,Video_Count as videocount from channels
             order by Video_Count desc '''
    cursor.execute(query2)
    t1=cursor.fetchall()
    df5=pd.DataFrame(t1,columns=["Channel Name","Total videos"])
    connection.commit()
    st.write(df5)
elif question=="3.Top 10 most viewed videos and their respective channels":
    query3='''select view_count as views, channel_Name as channelname, video_name as videos from videos
                   where view_count is not null order by view_count desc limit 10'''
    cursor.execute(query3)
    t3=cursor.fetchall()
    df6=pd.DataFrame(t3,columns=["Most Viewed","Channel Name","Video_title"])
    connection.commit()
    st.write(df6)

elif question=="4.Comments were made on each video":
    query4='''select comment_count as comment, channel_Name as channelname, video_name as videos from videos
                   where comment_count is not null '''
    cursor.execute(query4)
    t4=cursor.fetchall()
    df7=pd.DataFrame(t4,columns=["Number of comments","Channel Name","Video_title"])
    connection.commit()
    st.write(df7)
elif question=="5.Videos having the highest number of likes":
    query5='''select channel_Name as channelname, video_name as videos, like_count as likes from videos
                       where like_count is not null order by like_count desc '''
    cursor.execute(query5)
    t5=cursor.fetchall()
    df8=pd.DataFrame(t5,columns=["Channel Name","Video_title", "Number of Likes",])
    connection.commit()
    st.write(df8)
elif question=="6.Total number of likes and dislikes for each video":
    query6='''select video_name as videos, like_count as likes from videos'''
    cursor.execute(query6)
    t6=cursor.fetchall()
    df9=pd.DataFrame(t6,columns=["Video_title", "Number of Likes",])
    connection.commit()
    st.write(df9) 
elif question=="7.Total number of views for each channel":
    query7='''select Channel_Name as channelname, Channel_views as channelviews, Channel_id as id from channels'''
    cursor.execute(query7)
    t7=cursor.fetchall()
    df10=pd.DataFrame(t7,columns=["Channel Name","Channel views", "Channel ID",])
    connection.commit()
    st.write(df10) 
    
elif question=="8.Channels that have published videos in the year 2022":
    query8='''select video_name as videos, publishedAt as videorelease, Channel_Name as channelname from videos
                     where extract(year from publishedAt)=2022 '''
    cursor.execute(query8)
    t8=cursor.fetchall()
    df11=pd.DataFrame(t8,columns=["Video Name","Published Date", "Channel Name",])
    connection.commit()
    st.write(df11)
elif question=="9.Average duration of all videos in each channel": 
    query9 =  """SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"""
    cursor.execute(query9)
    t9=cursor.fetchall()
    df13 = pd.DataFrame(t9, columns=["ChannelTitle", "Average Duration"])
    connection.commit()
    T9=[]
    for index, row in df13.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question=="10.Videos have the highest number of comments": 
    query10=  """SELECT channel_Name as channelname, video_name as videos, comment_count as comments from videos 
                where comment_count is not null order by comment_count desc"""
    cursor.execute(query10)
    t10=cursor.fetchall()
    df14 = pd.DataFrame(t10, columns=["Channel Name", "Video Title","Total Comments"])
    connection.commit() 
    st.write(df14)