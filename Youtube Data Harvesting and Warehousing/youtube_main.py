from googleapiclient.discovery import build
import pymongo
import certifi
import mysql.connector
import pandas as pd
import streamlit as st
import uuid
import re
import streamlit as st
from streamlit_option_menu import option_menu 

# API KEY CONNECTION

def api_connect():
    api_id = "AIzaSyBRajxkiPhLGX4jHU0D_0GHTZTKxYnSbKw"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=api_id)
    return youtube

youtube=api_connect()

# GET CHANNEL INFO

def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subscription_Count=i['statistics']['subscriberCount'],
                Channel_Views=i['statistics']['viewCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'],
                Total_Videos=i['statistics']['videoCount'])
    return data

# GET PLAYLIST INFO

def get_playlist_info(channel_id):
    next_page_token = None
    playlists_info=[]
    while True:
        response = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for item in response['items']:
            data=dict(
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                Playlist_Id=item['id'],
                Playlist_Title=item['snippet']['title'],
                Playlist_Published=item['snippet']['publishedAt'],    
                Video_Count=item['contentDetails']['itemCount'])
            playlists_info.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break                        
    return playlists_info

# GET VIDEO IDS INFO

def get_video_ids(channel_id):
    # Get Playlist ID
    response=youtube.channels().list(
        part="contentDetails",
        id=channel_id).execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # Get Video IDs
    video_ids = []
    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,                                              
            pageToken=next_page_token).execute()                       
        for i in response1['items']:
            video_ids.append(i['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')                  
        if next_page_token is None :
            break
    return video_ids

# GET VIDEO INFO

def get_video_info(video_Ids):
    videos_info = []
    for video_id in video_Ids:
        response=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id).execute()
        data = dict(Channel_Name=response['items'][0]['snippet']['channelTitle'],
                    Channel_Id=response['items'][0]['snippet']['channelId'],
                    Video_Id=response['items'][0]['id'],
                    Video_Name=response['items'][0]['snippet']['title'],
                    Video_Desc=response['items'][0]['snippet']['description'],
                    Tags=response['items'][0]['snippet'].get('tags'),                       
                    Published_Date=response['items'][0]['snippet']['publishedAt'],
                    View_Count=response['items'][0]['statistics'].get('viewCount'),         
                    Like_Count=response['items'][0]['statistics'].get('likeCount'),         
                    Fav_Count=response['items'][0]['statistics'].get('favoriteCount'),      
                    Comment_Count=response['items'][0]['statistics'].get('commentCount'),   
                    Duration=response['items'][0]['contentDetails']['duration'],
                    Thumbnails=response['items'][0]['snippet']['thumbnails']['default']['url'],
                    Caption_Status=response['items'][0]['contentDetails']['caption'],
                    Definition=response['items'][0]['contentDetails']['definition'])
        videos_info.append(data)            
    return videos_info

# GET COMMENT INFO 

def get_comment_info(video_Ids):
    comments_info=[]
    try:
        for video_id in video_Ids:
            response=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50).execute()       
            for item in response['items']:     
                data=dict(Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Id=item['snippet']['topLevelComment']['id'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comments_info.append(data)
    except:
        pass  
    return comments_info

# CONNECTION TO MongoDB 
# DATABASE - "Youtube_Data" GETS CREATED

client=pymongo.MongoClient("mongodb+srv://sowmiyaamgs:1234@cluster0.hwjrmfq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",tlsCAFile=certifi.where())
db=client["Youtube_Data"]

# UPLOAD TO MongoDB
# Insert all the details of a particular channel as one document (one json/ one dictionary) inside MongoDB

def get_upload_channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    record=db["Channel_details"]
    record.insert_one({"Channel_Information":ch_details,"Playlists_Information":pl_details,"Videos_Information":vi_details,"Comments_Information":com_details})

    return "Uploaded Successfully"

# CONNECTION TO MySQL

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="Youtube_Data")

mycursor = mydb.cursor(buffered=True)

# TABLE CREATION - channels_info

def import_migrate_channel_details(select_channel):
    
    create_query='''CREATE TABLE IF NOT EXISTS channels_info(channel_name varchar(100),
                                                        channel_id varchar(80),
                                                        subcription_count bigint,
                                                        channel_views bigint,
                                                        channel_description text,
                                                        playlist_id varchar(80),
                                                        total_videos int)'''
    mycursor.execute(create_query)
    mydb.commit()

    ch_list=[]
    db=client["Youtube_Data"]
    record=db["Channel_details"]
    for ch_data in record.find({"Channel_Information.Channel_Name":{"$in":select_channel}},{'_id':0,'Channel_Information':1}):
        ch_list.append(ch_data["Channel_Information"])
    df=pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channels_info(channel_name,
                                                    channel_id,
                                                    subcription_count,
                                                    channel_views,
                                                    channel_description,
                                                    playlist_id,
                                                    total_videos)
                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Channel_Description'],
                row['Playlist_Id'],
                row['Total_Videos'])
        
        mycursor.execute(insert_query,values)
        mydb.commit()

# TABLE CREATION - playlists_info

def import_migrate_playlists_details(select_channel):
   
    create_query='''CREATE TABLE IF NOT EXISTS playlists_info(channel_id varchar(80),
                                                        channel_name varchar(100),
                                                        playlist_id varchar(80) primary key,
                                                        playlist_name varchar(100),
                                                        playlist_published timestamp,
                                                        video_count int)'''
    mycursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_Data"]
    record=db["Channel_details"]
    for pl_data in record.find({"Channel_Information.Channel_Name":{"$in":select_channel}},{'_id':0,'Playlists_Information':1}):
        for i in range(len(pl_data['Playlists_Information'])):
            pl_list.append(pl_data["Playlists_Information"][i])
    df=pd.DataFrame(pl_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO playlists_info(channel_id,
                                                    channel_name,
                                                    playlist_id,
                                                    playlist_name,
                                                    playlist_published,
                                                    video_count)
                                                    values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Id'],
                row['Channel_Name'],
                row['Playlist_Id'],
                row['Playlist_Title'],
                row['Playlist_Published'],
                row['Video_Count'])
        
        mycursor.execute(insert_query,values)
        mydb.commit()
                   
# TABLE CREATION - videos_info

# Duration to Minutes Convertion
def duration_to_minutes(duration_str):
    pattern = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(pattern, duration_str)
    if match:
        hours = int(match.group(1)[:-1]) if match.group(1) else 0
        minutes = int(match.group(2)[:-1]) if match.group(2) else 0
        seconds = int(match.group(3)[:-1]) if match.group(3) else 0
        
        total_minutes = round((hours*60) + minutes + seconds / 60 ,2)
        
        return total_minutes
    else:
        return 0
    
# Table Creation     
def import_migrate_videos_details(select_channel):

    create_query='''CREATE TABLE IF NOT EXISTS videos_info(channel_name varchar(100),
                                                        channel_id varchar(80),
                                                        video_id varchar(80) primary key,
                                                        video_name varchar(150),
                                                        video_desc text,
                                                        tags text,
                                                        published_date timestamp,
                                                        view_count bigint,
                                                        like_count bigint,
                                                        fav_count int,
                                                        comment_count int,
                                                        duration_in_minutes float,
                                                        thumbnails varchar(200),
                                                        caption_status varchar(50),
                                                        definition varchar(10))'''
    mycursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_Data"]
    record=db["Channel_details"]
    for vi_data in record.find({"Channel_Information.Channel_Name":{"$in":select_channel}},{'_id':0,'Videos_Information':1}):
        for i in range(len(vi_data['Videos_Information'])):
            vi_list.append(vi_data["Videos_Information"][i])
    df=pd.DataFrame(vi_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO videos_info(channel_name,
                                                channel_id,
                                                video_id,
                                                video_name,
                                                video_desc,
                                                tags,
                                                published_date,
                                                view_count,
                                                like_count,
                                                fav_count,
                                                comment_count,
                                                duration_in_minutes,
                                                thumbnails,
                                                caption_status,
                                                definition)
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        Tags_str = ','.join(row['Tags']) if isinstance(row['Tags'],list) else None
        Duration_in_Minutes = duration_to_minutes(row['Duration'])
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Video_Name'],
                row['Video_Desc'],
                Tags_str,
                row['Published_Date'],
                row['View_Count'],
                row['Like_Count'],
                row['Fav_Count'],
                row['Comment_Count'],
                Duration_in_Minutes,
                row['Thumbnails'],
                row['Caption_Status'],
                row['Definition'])
        
        mycursor.execute(insert_query,values)
        mydb.commit()

# TABLE CREATION - comments_info

def import_migrate_comment_details(select_channel):
    create_query='''CREATE TABLE IF NOT EXISTS comments_info(video_id varchar(80),
                                                        comment_id varchar(80) primary key,
                                                        comment_text text,
                                                        comment_author varchar(150),
                                                        comment_published timestamp)'''
    mycursor.execute(create_query)
    mydb.commit()
    
    com_list=[]
    db=client["Youtube_Data"]
    record=db["Channel_details"]
    for com_data in record.find({"Channel_Information.Channel_Name":{"$in":select_channel}},{'_id':0,'Comments_Information':1}):
        for i in range(len(com_data['Comments_Information'])):
            com_list.append(com_data["Comments_Information"][i])
    df=pd.DataFrame(com_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO comments_info(video_id,
                                                    comment_id,
                                                    comment_text,
                                                    comment_author,
                                                    comment_published)
                                                    values(%s,%s,%s,%s,%s)'''
        values=(row['Video_Id'],
                row['Comment_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published'])
        mycursor.execute(insert_query,values)
        mydb.commit()

# MySQL - 4 TABLES CREATION

def tables(select_channel):
    import_migrate_channel_details(select_channel)
    import_migrate_playlists_details(select_channel)
    import_migrate_videos_details(select_channel)
    import_migrate_comment_details(select_channel)

    return "Tables Updated Successfully"

# SHOW TABLES - STREAMLIT 

def show_channel_info():
    ch_list=[]
    db=client["Youtube_Data"]
    record=db["Channel_details"]
    for ch_data in record.find({},{'_id':0,'Channel_Information':1}):
        ch_list.append(ch_data["Channel_Information"])
    df=st.dataframe(ch_list)

def show_playlists_info():
    pl_list=[]
    db=client["Youtube_Data"]
    record=db["Channel_details"]
    for pl_data in record.find({},{'_id':0,'Playlists_Information':1}):
        for i in range(len(pl_data['Playlists_Information'])):
            pl_list.append(pl_data["Playlists_Information"][i])
    df=st.dataframe(pl_list)

def show_videos_info():
    vi_list=[]
    db=client["Youtube_Data"]
    record=db["Channel_details"]
    for vi_data in record.find({},{'_id':0,'Videos_Information':1}):
        for i in range(len(vi_data['Videos_Information'])):
            vi_list.append(vi_data["Videos_Information"][i])
    df=st.dataframe(vi_list)

def show_comments_info():
    com_list=[]
    db=client["Youtube_Data"]
    record=db["Channel_details"]
    for com_data in record.find({},{'_id':0,'Comments_Information':1}):
        for i in range(len(com_data['Comments_Information'])):
            com_list.append(com_data["Comments_Information"][i])
    df=st.dataframe(com_list)

# STREAMLIT CODE

st.set_page_config(layout="wide")
st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

with st.sidebar:
    select = option_menu("Main Menu", ["HOME", "EXTRACT AND LOAD", "VIEW","INSIGHTS"])

if select=="HOME":
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("")
        st.markdown("")
        st.image(r"E:\AMS Docs\Courses\Guvi\Data Science\Projects\01_Youtube Project\Youtube-logo.jpg",width=400)
    with col2:
        st.markdown("")
        st.markdown("")
        st.title("Tasks Involved")
        st.write("- Youtube Data API Integration")
        st.write("- Data Storage in NoSQL Database - MongoDB")
        st.write("- Data Migration to MySQL")
        st.write("- Develop Streamit Application to fetch data from MySQL and display Insights")
    
elif select=="EXTRACT AND LOAD":
    channel_id = st.text_input("Enter the Channel ID ")
    if st.button("Extract Data and Upload to MongoDB"):
        ch_ids=[]
        db=client["Youtube_Data"]
        record=db["Channel_details"]
        for ch_data in record.find({},{"_id":0,"Channel_Information":1}):
            ch_ids.append(ch_data["Channel_Information"]["Channel_Id"])
        if channel_id in ch_ids:
            st.success("Channel Details of given Channel ID already exists")
        else:
            insert=get_upload_channel_details(channel_id)
            st.success(insert)

    # To get the list of channels uploaded to MongoDB 
    uploaded_channels = []
    db=client["Youtube_Data"]
    record=db["Channel_details"]
    for ch_data in record.find({},{'_id':0,'Channel_Information':1}):
        uploaded_channels.append(ch_data["Channel_Information"]["Channel_Name"])

    # To display the appropriate list of channels to Migrate to MySQL
    table_query='''SHOW tables from youtube_data'''
    mycursor.execute(table_query)
    existing_ch = []
    channel_options = []
    for x in mycursor:
            if x[0]=="channels_info":
                query='''SELECT channel_name FROM channels_info'''
                mycursor.execute(query)
                out=mycursor.fetchall()
                for i in range(len(out)):
                    existing_ch.append(out[i][0])
                st.write("Existing Channels in MySQL :", ", ".join(existing_ch))
    channel_options = list(set(uploaded_channels)-set(existing_ch))
    select_channel = st.multiselect("Select Channel to Migrate",channel_options)
    if st.button("Migrate to MySQL"):
        Tables=tables(select_channel)
        st.success(Tables)
        
elif select=="VIEW":
    st.markdown(":blue[View the details of Channels uploaded in MongoDB]")
    show_table = st.radio("Select the Table for View",("Channels","Playlists","Videos","Comments"))
    if show_table=="Channels":
        show_channel_info()
    elif show_table=="Playlists":
        show_playlists_info()
    elif show_table=="Videos":
        show_videos_info()
    elif show_table=="Comments":
        show_comments_info()

elif select=="INSIGHTS":
    query='''SELECT channel_name FROM channels_info'''
    mycursor.execute(query)
    out=mycursor.fetchall()
    existing_ch = []
    for i in range(len(out)):
        existing_ch.append(out[i][0])
    st.write(":red[Existing Channels in MySQL :]",", ".join(existing_ch))

    question=st.selectbox("Select any Question to get Insights ",("1.	What are the names of all the videos and their corresponding channels?",
                                                                "2.	Which channels have the most number of videos, and how many videos do they have?",
                                                                "3.	What are the top 10 most viewed videos and their respective channels?",
                                                                "4.	How many comments were made on each video, and what are their corresponding video names?",
                                                                "5.	Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                                "6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                                "7.	What is the total number of views for each channel, and what are their corresponding channel names?",
                                                                "8.	What are the names of all the channels that have published videos in the year 2022?",
                                                                "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                                "10.	Which videos have the highest number of comments, and what are their corresponding channel names?"))

    # Query Answers

    if question=="1.	What are the names of all the videos and their corresponding channels?":
        query1 = '''SELECT video_name, channel_name 
                    FROM videos_info'''
        mycursor.execute(query1)
        a1=mycursor.fetchall()
        df1=pd.DataFrame(a1,columns=['Video Name','Channel Name'])
        st.write(df1)
    elif question=="2.	Which channels have the most number of videos, and how many videos do they have?":
        query2 = '''SELECT channel_name, total_videos 
                    FROM channels_info 
                    ORDER BY total_videos DESC'''
        mycursor.execute(query2)
        a2=mycursor.fetchall()
        df2=pd.DataFrame(a2,columns=['Channel Name','Total Videos'])
        st.write(df2)
    elif question=="3.	What are the top 10 most viewed videos and their respective channels?":
        query3 = '''SELECT video_name, channel_name, view_count
                    FROM videos_info
                    ORDER BY view_count DESC limit 10'''
        mycursor.execute(query3)
        a3=mycursor.fetchall()
        df3=pd.DataFrame(a3,columns=['Video Name','Channel Name','Views'])
        st.write(df3)
    elif question=="4.	How many comments were made on each video, and what are their corresponding video names?":
        query4 = '''SELECT video_name, comment_count
                    FROM videos_info'''
        mycursor.execute(query4)
        a4=mycursor.fetchall()
        df4=pd.DataFrame(a4,columns=['Video Name','Comments Count'])
        st.write(df4)   
    elif question=="5.	Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5 = '''SELECT video_name, like_count, channel_name
                    FROM videos_info
                    WHERE NOT like_count=0
                    ORDER BY like_count DESC'''
        mycursor.execute(query5)
        a5=mycursor.fetchall()
        df5=pd.DataFrame(a5,columns=['Video Name','Likes','Channel Name'])
        st.write(df5)
    elif question=="6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query6 = '''SELECT video_name, like_count
                    FROM videos_info'''
        mycursor.execute(query6)
        a6=mycursor.fetchall()
        df6=pd.DataFrame(a6,columns=['Video Name','Likes'])
        st.write(df6)
    elif question=="7.	What is the total number of views for each channel, and what are their corresponding channel names?":
        query7 = '''SELECT channel_name, channel_views
                    FROM channels_info'''
        mycursor.execute(query7)
        a7=mycursor.fetchall()
        df7=pd.DataFrame(a7,columns=['Channel Name','Channel Views'])
        st.write(df7)
    elif question=="8.	What are the names of all the channels that have published videos in the year 2022?":
        query8 = '''SELECT DISTINCT channel_name
                    FROM videos_info
                    WHERE YEAR(published_date)=2022'''
        mycursor.execute(query8)
        a8=mycursor.fetchall()
        df8=pd.DataFrame(a8,columns=['Channel Name'])
        st.write(df8)
    elif question=="9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = '''SELECT channel_name,AVG(duration_in_minutes)
                    FROM videos_info
                    GROUP BY channel_name'''
        mycursor.execute(query9)
        a9=mycursor.fetchall()
        df9=pd.DataFrame(a9,columns=['Channel Name','Average Duration (Mins)'])
        st.write(df9)
    elif question=="10.	Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10 = '''SELECT video_name, comment_count, channel_name
                    FROM videos_info
                    ORDER BY comment_count DESC'''
        mycursor.execute(query10)
        a10=mycursor.fetchall()
        df10=pd.DataFrame(a10,columns=['Video Name','Comment Count','Channel Name'])
        st.write(df10)


