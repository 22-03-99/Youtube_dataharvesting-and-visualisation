from googleapiclient.discovery import build
import pymongo
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import streamlit as st
from streamlit_option_menu import option_menu
import pymongo


def Api_connect():
    Api_Id="AIzaSyCASHb_jeu54lQENPoMG3fHaQro7DfjJc4"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#get channel info
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()
    for i in response['items']:
        data=dict(channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_Videos=i["statistics"]["videoCount"],
                channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

def get_Videos_ids(channel_id):
        Video_ids=[]
        response=youtube.channels().list(id=channel_id,
                                                part='contentDetails').execute()
        Playlist_Id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        next_page_token=None

        while True:
                response1=youtube.playlistItems().list(
                                                        part='snippet',
                                                        playlistId=Playlist_Id,
                                                        maxResults=50,
                                                        pageToken=next_page_token).execute()
                for i in range(len(response1["items"])):
                        Video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                next_page_token=response1.get('nextPageToken')

                if next_page_token is None:
                        break
        return Video_ids

#get video info
def get_video_info(Video_Ids):
    video_data=[]
    for video_id in Video_Ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnails=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favourite_count=item['statistics'].get('favouriteCount'),
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data

#get comment info
def get_comment_info(Video_ids):
    comment_data=[]
    try:
        for video_id in Video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(
                    comment_id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )

                comment_data.append(data)

    except:
        pass
    return comment_data

#get_playlist_details
def get_playlist_details(channel_id):

        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Playlist_title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data

client=pymongo.MongoClient("mongodb+srv://prabhakaran20999:pra123@cluster0.1tkcpbi.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]
collection=db['channel_details']

def channel_details(channel_id):
    api=Api_connect()
    ch_details=get_channel_info(channel_id)
    id=ch_details['Channel_Id']
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_Videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)


    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    return "upload completed successfully"

def show_channels_table():
  ch_list=[]
  db=client["youtube_data"]
  col=db['channel_details']
  for ch_dtls in col.find({},{"_id":0,"channel_information":1}):
    ch_list.append(ch_dtls["channel_information"])
  df=st.table(ch_list)
  return df

def show_playlists_table():
    pl_list=[]
    db=client["youtube_data"]
    col=db['channel_details']
    for pl_dtls in col.find({},{"_id":0,"playlist_information":1}):
      for i in range(len(pl_dtls['playlist_information'])):
        pl_list.append(pl_dtls['playlist_information'][i])
    df1=st.table(pl_list)
    return df1

def show_videos_table():
  vi_list=[]
  db=client["youtube_data"]
  col=db['channel_details']
  for vi_dtls in col.find({},{"_id":0,"video_information":1}):
    for i in range(len(vi_dtls['video_information'])):
      vi_list.append(vi_dtls['video_information'][i])
  df2=st.table(vi_list)
  df2=df2.fillna("none")
  df2['Tags']=df2['Tags'].apply(str)
  return df2

def show_comments_table():
  cm_list=[]
  db=client["youtube_data"]
  col=db['channel_details']
  for cm_dtls in col.find({},{"_id":0,"comment_information":1}):
    for i in range(len(cm_dtls['comment_information'])):
      cm_list.append(cm_dtls['comment_information'][i])
  df3=st.table(cm_list)
  return df3

#table creation
def channels_table(channel_name_s):
  import sqlite3
  con = sqlite3.connect('youtube.db')
  cursor=con.cursor()

  try:
      create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                          Channel_Id varchar(80) primary key,
                                                          Subscribers bigint,
                                                          Views bigint,
                                                          Total_Videos int,
                                                          Channel_Description text,
                                                          Playlist_Id varchar(80))'''
      cursor.execute(create_query)
      mydb.commit()

  except:
      print("Channels table already created")

  query_1= "SELECT * FROM channels"
  cursor.execute(query_1)
  table= cursor.fetchall()
  con.commit()

  chann_list= []
  chann_list2= []
  df_all_channels= pd.DataFrame(table)

  chann_list.append(df_all_channels[0])
  for i in chann_list[0]:
      chann_list2.append(i)


  if channel_name_s in chann_list2:
      news= f"Your Provided Channel {channel_name_s} is Already exists"
      return news

  else:

      single_channel_details= []
      coll1=db["channel_details"]
      for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
          single_channel_details.append(ch_data["channel_information"])

      df_single_channel= pd.DataFrame(single_channel_details)



      for index,row in df_single_channel.iterrows():
          insert_query='''insert into channels(Channel_Name ,
                                              Channel_Id,
                                              Subscribers,
                                              Views,
                                              Total_Videos,
                                              Channel_Description,
                                              Playlist_Id)

                                              values(%s,%s,%s,%s,%s,%s,%s)'''
          values=(row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscribers'],
                  row['Views'],
                  row['Total_Videos'],
                  row['Channel_Description'],
                  row['Playlist_Id'])

          try:
              cursor.execute(insert_query,values)
              con.commit()

          except:
              print("Channel values are already inserted")

def playlist_table(channel_name_s):
  import sqlite3
  con = sqlite3.connect('youtube.db')
  cursor=con.cursor()

  create_query2='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                              Playlist_title varchar(100),
                                                              Channel_Id varchar(100),
                                                              Channel_Name varchar(100),
                                                              PublishedAt timestamp,
                                                              Video_Count int)'''
  cursor.execute(create_query2)
  con.commit()

  s_playlist_info=[]
  db=client["youtube_data"]
  col=db['channel_details']
  for ch_dtls in col.find({'channel_information.channel_Name':channel_name_s},{"_id":0}):
    s_playlist_info.append(ch_dtls['playlist_information'])
  df_playlist_dtls=pd.DataFrame(s_playlist_info[0])
  for index,row in df_playlist_dtls.iterrows():
          insert_query='''insert into playlists(Playlist_Id,
                                              Playlist_title,
                                              Channel_Id,
                                              Channel_Name,
                                              PublishedAt,
                                              Video_Count
                                              )

                                              values(?,?,?,?,?,?)'''

          values=(row['Playlist_Id'],
                  row['Playlist_title'],
                  row['Channel_Id'],
                  row['Channel_name'],
                  row['PublishedAt'],
                  row['Video_Count']
                  )


          cursor.execute(insert_query,values)
          con.commit()

def videos_table(channel_name_s):
  import sqlite3
  con = sqlite3.connect('youtube.db')
  cursor=con.cursor()

  create_query3='''create table if not exists videos(Channel_Name varchar(1000),
                                                  Channel_Id varchar(1000),
                                                  Video_Id varchar(300) primary key,
                                                  Title text,
                                                  Tags text,
                                                  Thumbnails varchar(2000),
                                                  Description text,
                                                  Published_Date timestamp,
                                                  Duration interval,
                                                  Views bigint,
                                                  Likes bigint,
                                                  Comments int,
                                                  Favourite_count int,
                                                  Definition varchar(1000),
                                                  Caption_Status varchar(5000)
                                                      )'''

  cursor.execute(create_query3)
  con.commit()


  s_video_info=[]
  db=client["youtube_data"]
  col=db['channel_details']
  for vi_dtls in col.find({'channel_information.channel_Name':channel_name_s},{"_id":0}):
    s_video_info.append(vi_dtls['video_information'])
  df_video_dtls=pd.DataFrame(s_video_info[0])
  df_video_dtls['Tags']=df_video_dtls['Tags'].fillna ("none")
  df_video_dtls['Tags']=df_video_dtls['Tags'].apply(str)
  for index,row in df_video_dtls.iterrows():
          insert_query='''insert into videos(Channel_Name,
                                                  Channel_Id,
                                                  Video_Id,
                                                  Title,
                                                  Tags,
                                                  Thumbnails,
                                                  Description,
                                                  Published_Date,
                                                  Duration,
                                                  Views,
                                                  Likes,
                                                  Comments,
                                                  Favourite_count,
                                                  Definition,
                                                  Caption_Status
                                              )
                                              values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

          values=(row['Channel_Name'],
                  row['Channel_Id'],
                  row['Video_Id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnails'],
                  row['Description'],
                  row['Published_Date'],
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favourite_count'],
                  row['Definition'],
                  row['Caption_Status']
                  )

          cursor.execute(insert_query,values)
          con.commit()

def comments_table(channel_name_s):
  import sqlite3
  con = sqlite3.connect('youtube.db')
  cursor=con.cursor()


  create_query4='''create table if not exists comments(comment_id varchar(100) primary key,
                                                      Video_Id varchar(50),
                                                      Comment_text text,
                                                      Comment_Author varchar(150),
                                                      Comment_Published timestamp
                                                      )'''

  cursor.execute(create_query4)
  con.commit()

  s_comment_info=[]
  db=client["youtube_data"]
  col=db['channel_details']
  for cm_dtls in col.find({'channel_information.channel_Name':channel_name_s},{"_id":0}):
    s_comment_info.append(cm_dtls['comment_information'])
  df_comment_dtls=pd.DataFrame(s_comment_info[0])

  for index,row in df_comment_dtls.iterrows():
          insert_query='''insert into comments(comment_id,
                                                  Video_Id,
                                                  Comment_text,
                                                  Comment_Author,
                                                  Comment_Published
                                              )

                                              values(?,?,?,?,?)'''


          values=(row['comment_id'],
                  row['Video_Id'],
                  row['Comment_text'],
                  row['comment_author'],
                  row['comment_published']
                  )

          cursor.execute(insert_query,values)
          con.commit()

def tables(channel_name):

    news= channels_table(channel_name)
    if news:
        st.write(news)
    else:
        playlist_table(channel_name)
        videos_table(channel_name)
        comments_table(channel_name)

        return "Tables Created Successfully"

import pandas as pd
import re

# Function to convert duration string into seconds
def duration_to_seconds(duration_str):
    hours_match = re.search(r'(\d+)H', duration_str)
    minutes_match = re.search(r'(\d+)M', duration_str)
    seconds_match = re.search(r'(\d+)S', duration_str)

    hours = int(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0
    seconds = int(seconds_match.group(1)) if seconds_match else 0

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

def main():



  st.title("YOUTUBE DATA HAVERSTING AND WAREHOUSING")
  st.write("")

  with st.sidebar:
      st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")

  client=pymongo.MongoClient("mongodb+srv://prabhakaran20999:pra123@cluster0.1tkcpbi.mongodb.net/?retryWrites=true&w=majority")
  db=client["youtube_data"]
  collection=db['channel_details']

  channel_id=st.text_input("Enter the channel ID")
  if st.button("collect and store data in mongodb"):
      ch_list=[]
      db=client["youtube_data"]
      col=db['channel_details']
      for ch_dtls in col.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_dtls["channel_information"]['Channel_Id'])

      if channel_id in ch_list:
          st.success("Channel Details of the given channel id already exists")

      else:
          insert=channel_details(channel_id)
          st.success(insert)

  all_channels=[]
  db=client["youtube_data"]
  col=db['channel_details']
  for ch_dtls in col.find({},{"_id":0,"channel_information":1}):
    all_channels.append(ch_dtls['channel_information']['channel_Name'])

  unique_channel=st.selectbox("select the channel",all_channels)

  if st.button("Migrate to Sql"):
    Table=tables(unique_channel)
    st.success(Table)

  show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))



  if show_table=="CHANNELS":
      show_channels_table()

  elif show_table=="PLAYLISTS":
      show_playlists_table()

  elif show_table=="VIDEOS":
      show_videos_table()

  elif show_table=="COMMENTS":
      show_comments_table()

  import sqlite3
  con = sqlite3.connect('youtube.db')
  cursor=con.cursor()


  question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                                "2. channels with most number of videos",
                                                "3. 10 most viewed videos",
                                                "4. comments in each videos",
                                                "5. Videos with higest likes",
                                                "6. likes of all videos",
                                                "7. views of each channel",
                                                "8. videos published in the year of 2022",
                                                "9. average duration of all videos in each channel",
                                                "10. videos with highest number of comments"))


  if question=="1. All the videos and the channel name":
      query1='''select title as videos,channel_name as channelname from videos'''
      cursor.execute(query1)
      con.commit()
      t1=cursor.fetchall()
      df=pd.DataFrame(t1,columns=["video title","channel name"])
      df.index=df.index+1
      st.write(df)

  elif question=="2. channels with most number of videos":
      query2='''select channel_name as channelname,total_videos as no_videos from channels
                  order by total_videos desc'''
      cursor.execute(query2)
      con.commit()
      t2=cursor.fetchall()
      df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
      df2.index=df2.index+1
      st.write(df2)

  elif question=="3. 10 most viewed videos":
      query3='''select views as views,channel_name as channelname,title as videotitle from videos
                  where views is not NULL
                  order by views desc limit 10'''
      cursor.execute(query3)
      con.commit()
      t3=cursor.fetchall()
      df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
      df3.index=df3.index+1
      st.write(df3)

  elif question=="4. comments in each videos":
      query4='''select comments as no_comments,title as videotitle
                from videos where comments is not null and comments not in ('none')'''
      cursor.execute(query4)
      con.commit()
      t4=cursor.fetchall()
      df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
      df4.index=df4.index+1
      st.write(df4)

  elif question=="5. Videos with higest likes":
      query5='''select title as videotitle,channel_name as channelname,likes as likecount
                  from videos where likes is not null and likes not in ('none')
                  order by likes desc'''
      cursor.execute(query5)
      con.commit()
      t5=cursor.fetchall()
      df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
      df5.index=df5.index+1
      st.write(df5)

  elif question=="6. likes of all videos":
      query6='''select likes as likecount,title as videotitle from videos'''
      cursor.execute(query6)
      con.commit()
      t6=cursor.fetchall()
      df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
      df6.index=df6.index+1
      st.write(df6)

  elif question=="7. views of each channel":
      query7='''select channel_name as channelname ,views as totalviews from channels'''
      cursor.execute(query7)
      con.commit()
      t7=cursor.fetchall()
      df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
      df7.index=df7.index+1
      st.write(df7)

  elif question=="8. videos published in the year of 2022":
      query8='''select title as video_title,published_date as videorelease, channel_name as channelname from videos
                  where strftime('%Y', published_date)='2022'
                  '''
      cursor.execute(query8)
      con.commit()
      t8=cursor.fetchall()
      df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
      df8.index=df8.index+1
      st.write(df8)

  elif question=="9. average duration of all videos in each channel":
      query9='''select channel_name as channelname,duration as averageduration from videos'''
      cursor.execute(query9)
      con.commit()
      t9=cursor.fetchall()
      df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
      df9.index=df9.index+1
      df9['averageduration_seconds'] = df9['averageduration'].apply(lambda x: duration_to_seconds(x) if x else None)
      average_duration_grouped = df9.groupby('channelname')['averageduration_seconds'].mean()
      average_duration_df = average_duration_grouped.reset_index()

      T9=[]
      for index,row in average_duration_df.iterrows():
          channel_title=row["channelname"]
          average_duration=row['averageduration_seconds']
          average_duration_str=str(average_duration)
          T9.append({"channeltitle" : channel_title, "avgduration" : average_duration_str + " seconds"})
          df11=pd.DataFrame(T9)
          df11.index=df11.index+1
      st.write(df11)

  elif question=="10. videos with highest number of comments":
      query10='''select title as videotitle, channel_name as channelname,comments as comments
                  from videos where comments is not null and comments not in ('none')
                  order by comments desc'''
      cursor.execute(query10)
      con .commit()
      t10=cursor.fetchall()
      df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
      df10.index=df10.index+1
      st.write(df10)





if __name__== '__main__':
  main()
