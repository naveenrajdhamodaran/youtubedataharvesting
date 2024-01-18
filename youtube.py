from googleapiclient.discovery import build
import pymongo
import pandas as pd
import streamlit as st

#Api Key Connection
def Api_connect():
    Api_id="AIzaSyDZhxHxzVvUOX0C9OLAsFKYvKeBVkjZqYE"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_id)
    return youtube
youtube=Api_connect()


#get channel details using channel id
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()
    for i in response['items']:
        data=dict(channel_name=i["snippet"]["title"],
                channel_id=i["id"],
                subscribers=i["statistics"]["subscriberCount"],
                views=i["statistics"]["viewCount"],
                total_videos=i["statistics"]["videoCount"],
                channel_description=i["snippet"]["description"],
                playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


#get video id
def get_videosid(channel_id):
    video_id=[]
    response=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    paylist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=paylist_id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()       
        for i in range(len(response1['items'])):
            video_id.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break  
    return video_id          


#get the video details
def get_video_info(Video_IDS):
    video_data=[]
    for videos_id in Video_IDS:
        request=youtube.videos().list(
                                    part="snippet,contentDetails,statistics",
                                    id=videos_id
        )
        response=request.execute()
        for item in response['items']:
            data=dict(channel_name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    video_id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    comments=item['statistics'].get('commentCount'),
                    favorite_count=item.get('favoriteCount'),
                    definition=item['contentDetails']['definition'],
                    caption_status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data

        
#get comment information
def comment_info(Video_IDS):
    comment_data=[]
    try:
        for video_id in Video_IDS:
            request=youtube.commentThreads().list(
                                                    part="snippet",
                                                    videoId=video_id,
                                                    maxResults=50)
            response=request.execute()

            for item in response['items']:
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                        video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_published_date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass
    return comment_data



#get playlist details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
                                        part="snippet,contentDetails",
                                        channelId=channel_id,
                                        maxResults=50,
                                        pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_id=item['snippet']['channelId'],
                    Channel_name=item['snippet']['title'],
                    PublistedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount'])
                    
            All_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data



#MongoDB connection

client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client['youtube_data_details']


#Calling all the function like channels,playlist,videodetails,playlist and uploading the data in MongoDB

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    Video_IDS=get_videosid(channel_id)
    vi_details=get_video_info(Video_IDS)
    com_details=comment_info(Video_IDS)
    col5=db['channel_details']
    col5.insert_one({"channel_information":ch_details,"playlist_details":pl_details,
                     "video_information":vi_details,"comment_information":com_details})
    return "upload completed successfully in MongoDB"




#MYSQL CONNECTION DETAILS

import mysql.connector
mydb = mysql.connector.connect(host='localhost',user='root',password='naveenraj',database='youtube_data')
mycursor = mydb.cursor()


#Channel table creation in Mysql
def channels_tables():
    import mysql.connector
    mydb = mysql.connector.connect(host='localhost',user='root',password='naveenraj',database='youtube_data')
    mycursor = mydb.cursor()

    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        sql="create table channels(channel_name varchar(100),channel_id varchar(80) primary key,subscribers bigint,views bigint,total_videos int,channel_description text,playlist_id varchar(80))"
        mycursor.execute(sql)
        mydb.commit()
    except:
        print("channels table already created")

    ch_list=[]
    db=client['youtube_data_details']
    col5=db['channel_details']
    for ch_data in col5.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        sql='''insert into channels(channel_name,
                                    channel_id,
                                    subscribers,
                                    views,
                                    total_videos,
                                    channel_description,
                                    playlist_id) 
                                    values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row["channel_id"],
                row["subscribers"],
                row["views"],
                row["total_videos"],
                row["channel_description"],
                row["playlist_id"])
        try:
            mycursor.execute(sql,values)
            mydb.commit()
        except:
            print("channel values are already inserted")

    #return "channels tables uploaded successfully"

    
#playlist tables created in Mysql and data's inserted from dataframe to mysql playlist table:
#playlist tables:
from datetime import datetime
import mysql.connector
def playlist_tables():
    import mysql.connector
    mydb = mysql.connector.connect(host='localhost',user='root',password='naveenraj',database='youtube_data')
    mycursor = mydb.cursor()

    drop_query='''drop table if exists playlists'''
    mycursor.execute(drop_query)
    mydb.commit()

    
    sql="create table playlists(Playlist_id varchar(100) primary key,Title varchar(100),Channel_id varchar(100),Channel_name varchar(100),PublistedAt timestamp,Video_Count int)"
    mycursor.execute(sql)
    mydb.commit()
    
        

    pl_list=[]
    db=client['youtube_data_details']
    col5=db['channel_details']
    for pl_data in col5.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_data['playlist_details'])):
            pl_list.append(pl_data['playlist_details'][i])
    df1=pd.DataFrame(pl_list) 

    for index,row in df1.iterrows():
        publisted_at = datetime.strptime(row['PublistedAt'], '%Y-%m-%dT%H:%M:%SZ')
        sql='''insert into playlists(Playlist_id,
                                    Title,
                                    Channel_id,
                                    Channel_name,
                                    PublistedAt,
                                    Video_Count) 
                                    values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_id'],
                row["Title"],
                row["Channel_id"],
                row["Channel_name"],
                publisted_at,
                row["Video_Count"])
        mycursor.execute(sql,values)
        mydb.commit()
        

    #return "playlist tables uploaded successfully"
        



#insert videos details from dataframe to  video table of mysql db 
#insert videos details in mysql
client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client['youtube_data_details']
from datetime import datetime
import mysql.connector
def videos_tables():
    import mysql.connector
    mydb = mysql.connector.connect(host='localhost',user='root',password='naveenraj',database='youtube_data')
    mycursor = mydb.cursor()

    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()

    
    sql='''create table videos(channel_name varchar(100),
                                channel_id varchar(100),
                                video_id varchar(100) primary key,
                                Title varchar(150),
                                Tags text,
                                Thumbnail varchar(200),
                                Description text,
                                Published_date timestamp,
                                Duration int,
                                Views bigint,
                                Likes bigint,
                                comments int,
                                favorite_count int,
                                definition varchar(100),
                                caption_status varchar(100))'''
    mycursor.execute(sql)
    mydb.commit()
    
        

    vi_list=[]
    db=client['youtube_data_details']
    col5=db['channel_details']
    for vi_data in col5.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list) 

    for index,row in df2.iterrows():
        Published_date = datetime.strptime(row['Published_date'], '%Y-%m-%dT%H:%M:%SZ')
        tags_str = ', '.join(row['Tags']) if row['Tags'] else None
        duration_str = row["Duration"][2:]  # Remove 'PT' prefix
        duration_seconds = 0
        if 'H' in duration_str:
            duration_seconds += int(duration_str.split('H')[0]) * 3600
            duration_str = duration_str.split('H')[1]
        if 'M' in duration_str:
            duration_seconds += int(duration_str.split('M')[0]) * 60
            duration_str = duration_str.split('M')[1]
        if 'S' in duration_str:
            duration_seconds += int(duration_str.split('S')[0])
        sql='''insert into videos(channel_name,
                                    channel_id,
                                    video_id,
                                    Title,
                                    Tags,
                                    Thumbnail,
                                    Description,
                                    Published_date,
                                    Duration,
                                    Views,
                                    Likes,
                                    comments,
                                    favorite_count,
                                    definition,
                                    caption_status) 
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row["channel_id"],
                row["video_id"],
                row["Title"],
                tags_str,
                row["Thumbnail"],
                row["Description"],
                Published_date,
                duration_seconds,
                row["Views"],
                row["Likes"],
                row["comments"],
                row["favorite_count"],
                row["definition"],
                row["caption_status"],)
        mycursor.execute(sql,values)
        mydb.commit()
        

    #return "videos tables uploaded successfully"


        


#insert comment details from dataframe to  comment table of mysql db 
#insert comments details in mysql
client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client['youtube_data_details']
from datetime import datetime
import mysql.connector
def comments_tables():
    import mysql.connector
    mydb = mysql.connector.connect(host='localhost',user='root',password='naveenraj',database='youtube_data')
    mycursor = mydb.cursor()

    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()

    
    sql='''create table comments(comment_id varchar(100) primary key,
                                video_id varchar(100),
                                comment_text text,
                                comment_author varchar(150),
                                comment_published_date timestamp)'''
    mycursor.execute(sql)
    mydb.commit()
    
        

    com_list=[]
    db=client['youtube_data_details']
    col5=db['channel_details']
    for com_data in col5.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3=pd.DataFrame(com_list) 

    for index,row in df3.iterrows():
        comment_published_date = datetime.strptime(row['comment_published_date'], '%Y-%m-%dT%H:%M:%SZ')
        sql='''insert into comments(comment_id,
                                    video_id,
                                    comment_text,
                                    comment_author,
                                    comment_published_date) 
                                    values(%s,%s,%s,%s,%s)'''
        values=(row['comment_id'],
                row["video_id"],
                row["comment_text"],
                row["comment_author"],
                comment_published_date)
        mycursor.execute(sql,values)
        mydb.commit()
        

    #return "comments tables uploaded successfully"

#create function for complete insertion of channel details in mysql
def tables():
    channels_tables()
    videos_tables()
    playlist_tables()
    comments_tables()

    return "All tables created successfully"



# streamlit dataframe of channel table
def show_channel_table():
    ch_list=[]
    db=client['youtube_data_details']
    col5=db['channel_details']
    for ch_data in col5.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


# streamlit dataframe of playlist table
def show_playlist_table():
    pl_list=[]
    db=client['youtube_data_details']
    col5=db['channel_details']
    for pl_data in col5.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_data['playlist_details'])):
            pl_list.append(pl_data['playlist_details'][i])
    df1=st.dataframe(pl_list) 

    return def1


# streamlit dataframe of video table
def show_video_table():
    vi_list=[]
    db=client['youtube_data_details']
    col5=db['channel_details']
    for vi_data in col5.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list) 

    return df2


# streamlit dataframe of video table
def show_comments_table():
    com_list=[]
    db=client['youtube_data_details']
    col5=db['channel_details']
    for com_data in col5.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3=st.dataframe(com_list) 

    return df3


#streamlit connectivity

background_image = "C:\Guvi\DW74\photos\backround.jpg"  # Replace with the path to your image file

# Custom CSS to set the background image

with st.sidebar:
    st.title(":BLUE[YOUTUBE DATAHARVESTING AND WAREHOUSING]")
    st.header("Module's undertaken")
    st.caption("python sripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API integration")
    st.caption("DataManagement using MongoDB and SQL")

channel_id=st.text_input("Enter The ChannelId")

if st.button("collect and store data"):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    ch_ids = []
    db = client['youtube_data_details']
    col5 = db['channel_details']
    for ch_data in col5.find({}, {"_id": 0, "channel_information": 1}):
        ch_ids.append(ch_data['channel_information']['channel_id'])

    if channel_id in ch_ids:
        st.success("channel detail of given channel id already exist")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to Mysql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR REVIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_video_table()

elif show_table=="COMMENTS":
    show_comments_table()

    

#sql connection
import mysql.connector
mydb = mysql.connector.connect(host='localhost',user='root',password='naveenraj',database='youtube_data')
mycursor = mydb.cursor()

question=st.selectbox("select your question",("1. All the video name and their channel name",
                                                "2. channels with most number of videos",
                                                "3. Top 10 most viewed video's and their channel",
                                                "4. comments in each videos",
                                                "5. videos with highest likes",
                                                "6. Likes of all videos",
                                                "7. views of each channel",
                                                "8. video published in the year of 2022",
                                                "9. average duration of all videos in each channels",
                                                "10. Videos with highest number of comments"))


if question=="1. All the video name and their channel name":
    sql_query = '''SELECT Title AS VideoName, channel_name AS ChannelName
                    FROM videos'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df1 = pd.DataFrame(results, columns=["video_title", "channel_name"])
    st.write(df1)


elif question=="2. channels with most number of videos":
    sql_query = '''SELECT channel_name AS channelname, total_videos AS video_count
                    FROM channels order by video_count DESC'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df2 = pd.DataFrame(results, columns=["channelname", "video_count"])
    st.write(df2)


elif question=="3. Top 10 most viewed video's and their channel":
    sql_query = '''SELECT Views as views,channel_name as channelname,Title as videoname 
                    from videos 
                    where views is not null
                    order by views desc
                    limit 10'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df3 = pd.DataFrame(results, columns=["views", "channelname","videoname"])
    st.write(df3)

elif question=="4. comments in each videos":
    sql_query = '''SELECT comments as comment_counts, Title as videoname from videos where comments is not null order by comments desc'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df4 = pd.DataFrame(results, columns=["comment_counts", "videoname"])
    st.write(df4)


elif question=="5. videos with highest likes":
    sql_query = '''SELECT  Title as videoname, Likes as like_count from videos where Likes is not null order by Likes desc'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df5 = pd.DataFrame(results, columns=["videoname", "like_count"])
    st.write(df5)


elif question=="5. videos with highest likes":
    sql_query = '''SELECT  channel_name as channelname,Title as videoname, Likes as like_count from videos where Likes is not null order by Likes desc'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df5 = pd.DataFrame(results, columns=["channelname","videoname", "like_count"])
    st.write(df5)


elif question=="6. Likes of all videos":
    sql_query = '''SELECT Title as videoname, Likes as like_count from videos where Likes is not null order by Likes desc'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df6 = pd.DataFrame(results, columns=["videoname", "like_count"])
    st.write(df6)


elif question== "7. views of each channel":
    sql_query = '''SELECT channel_name as channelname, Views as total_views from channels where Views is not null order by Views desc'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df7 = pd.DataFrame(results, columns=["channelname", "total_views"])
    st.write(df7)


elif question== "8. video published in the year of 2022":
    sql_query = '''SELECT channel_name as channelname, Title as videoname,Published_date as publishedAt from videos where extract(year from Published_date)=2022'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df8 = pd.DataFrame(results, columns=["channelname", "videoname","publishedAt"])
    st.write(df8)


elif question== "9. average duration of all videos in each channels":
    sql_query = '''SELECT channel_name as channelname, AVG(duration) as Averageduration_in_sec from videos 
                    group by channel_name'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df9 = pd.DataFrame(results, columns=["channelname", "Averageduration_in_sec"])
    st.write(df9)


elif question== "10. Videos with highest number of comments":
    sql_query = '''SELECT channel_name as channelname,Title as videoname, comments as comments_count
                    from videos where comments is not null order by comments desc'''
    mycursor.execute(sql_query)

    results = mycursor.fetchall()
    mydb.commit()
    df10 = pd.DataFrame(results, columns=["channelname", "videoname","comments_count"])
    st.write(df10)




                                               
        

    








