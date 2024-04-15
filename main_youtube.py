#importing needed packages for this project
from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
import mysql.connector as sql
import sqlalchemy
import re
from sqlalchemy import create_engine
import pymysql as pymysql

#to create global variable
ss = st.session_state

#create connection to youtube using api
def api_connect():

    api_key = 'Enter The Your API Key'
    connect = build('youtube', 'v3', developerKey=api_key)
    return connect

#craating connection to MYSQL
def get_cursor(query):
    mydb = sql.connect(host="localhost",
                       user="root",
                       password="root",
                       database="youtube_data"
                       )
    mycursor = mydb.cursor()
    mycursor.execute(query)
    return mycursor.fetchall()



# irfan- UChL9x-Q75LCuYHIFRDT_T1A
# my_ch - UC0w0qaUXKBcyCkyDwjgvsyw
# ral_lop - UCULSrCo_sFQcz8r__iaLBJA
# shakthivel - UCO-qbU5R7zRF268mBqSPKoA
#sazarunes - UCTuow0QI7JuLWw647V40rww
#cycle - UCQN6WKa4bLj6hjtM9WNXghA
#channel_id = 'UCULSrCo_sFQcz8r__iaLBJA'  # example channel_ID

#Define variable for youtube connection
youtube = api_connect()

#to get channel information
def get_channel_info(channel_id, youtube):
    data = []

    response = youtube.channels().list(
        id=channel_id,
        part='snippet,statistics,contentDetails'
    )

    channel_data = response.execute()

    for i in channel_data['items']:
        data.append(dict(Channel_Name=i["snippet"]["title"],
                         Channel_Id=i["id"],
                         Subscribers=i['statistics']['subscriberCount'],
                         Views=i["statistics"]["viewCount"],
                         Total_Videos=int(i["statistics"]["videoCount"]),
                         Channel_Description=i["snippet"]["description"],
                         Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]))

    return data


#get all the video ids from the channel
def get_videos_ids(playlist_id, youtube):

    video_id=[]
    next_page_token = None
    while True:
        video_request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response_video = video_request.execute()

        for item in response_video['items']:
            video_id.append(item['contentDetails']['videoId'])

        next_page_token = response_video.get('nextPageToken')
        if next_page_token is None:
            break

    return video_id

#get all video information
def get_video_info(video_id, youtube):
    video_data=[]
    for video_id in video_id:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=duration_formatting(item['contentDetails']['duration']),
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Dislikes=item['statistics'].get('dislikeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data

#to conver video duration to a proper format
def duration_formatting(duration):
    regex = r'PT(\d*H)?(\d*M)?(\d*S)?'
    match = re.match(regex, duration)
    if not match:
        return '00:00:00'
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else '00'
    minutes = int(minutes[:-1]) if minutes else '00'
    seconds = int(seconds[:-1]) if seconds else '00'
    # total_seconds = hours * 3600 + minutes * 60 + seconds
    time = f'{hours}:{minutes}:{seconds}'
    return time

#get all comment information
def get_comment_info(video_id,youtube,channel_id):
    Comment_data=[]
    for video_id in video_id:
        try:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()
        except:
            continue

        for item in response['items']:
            data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                    Channel_Id=channel_id,
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

            Comment_data.append(data)

    return Comment_data

#get all playlist information
def get_playlist_details(channel_id,youtube):
    next_page_token=None
    All_data=[]
    while True:
        request = youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

#this is to check if the channel is already existing in the database
def existing_channels_in_Database():
    query = "SELECT Channel_Id,Channel_Name FROM channel_info"
    existing_channels = get_cursor(query)
    return existing_channels


#this is to clear the database
def clear_database():
    query1 = "SET FOREIGN_KEY_CHECKS = 0;"
    query2 = "truncate channel_info;"
    query3 = "truncate video_info;"
    query4 = "truncate comment_info;"
    query5 = "truncate playlist_info;"
    query6 = "SET FOREIGN_KEY_CHECKS = 1;"
    mydb = sql.connect(host="localhost",
                       user="root",
                       password="root",
                       database="youtube_data"
                       )
    mycursor = mydb.cursor()
    try:
        mycursor.execute(query1)
        mydb.commit()
        mycursor.execute(query2)
        mycursor.execute(query3)
        mycursor.execute(query4)
        mycursor.execute(query5)
        mycursor.execute(query6)
        mydb.commit()
        mydb.close()
        return st.success('Database Is Cleared Successfully')
    except Exception as e:
        return st.error(e)


#This is to temporarily save all the information in DataFrames.
def data_grabbing_process(channel_id, youtube):
    with st.spinner("Getting Channel Info"):
        ss.chanel_info = get_channel_info(channel_id, youtube)
        ss.df_channel_info = pd.DataFrame.from_dict(ss.chanel_info, orient='columns')
        if int(ss.chanel_info[0]['Total_Videos']) > 300:
            add_sidebar.write(f'Total videos of this channel is {ss.chanel_info[0]['Total_Videos']}.\n'
                     f'It may take a while to grab data be patient')
        else:
            add_sidebar.write(f'Total videos of this channel is {ss.chanel_info[0]['Total_Videos']}.\n'
                     f'Data Grabbing will finish soon')
    add_sidebar.success("Done!")
    with st.spinner("Getting Video Info"):
        ss.playlist_id = ss.chanel_info[0]['Playlist_Id']
        ss.video_id = get_videos_ids(ss.playlist_id, youtube)
        ss.video_info = pd.DataFrame.from_dict(get_video_info(ss.video_id, youtube), orient='columns')
    add_sidebar.success("Done!")
    with st.spinner("Getting Comment Info"):
        ss.comment_info = pd.DataFrame.from_dict(get_comment_info(ss.video_id, youtube, channel_id), orient='columns')
    add_sidebar.success("Done!")
    with st.spinner("Getting Playlist Info"):
        ss.playlist_info = pd.DataFrame.from_dict(get_playlist_details(channel_id, youtube), orient='columns')
    add_sidebar.success("Done!")
    ss.all_channel_info = pd.concat([ss.all_channel_info, ss.df_channel_info])
    ss.all_video_info = pd.concat([ss.all_video_info, ss.video_info])
    ss.all_comment_info = pd.concat([ss.all_comment_info, ss.comment_info])
    ss.all_playlist_info = pd.concat([ss.all_playlist_info, ss.playlist_info])

#This is to save all the informaton to MYSQL DATABASE
def insert_data_to_mysql(chl_info, vid_info, cmnt_inf, play_info):
    #create connection with mysql using sqlalchamy
    engine = create_engine('mysql+mysqlconnector://root:root@localhost/youtube_data', echo=False)

    #transfer the channel info to mysql
    chl_info.to_sql('channel_info', con=engine, if_exists='append', index=False,
                    dtype = {"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                             "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                             "Subscribers": sqlalchemy.types.BigInteger,
                             "Views": sqlalchemy.types.BigInteger,
                             "Total_Videos": sqlalchemy.types.INT,
                             "Channel_Description": sqlalchemy.types.TEXT,
                             "Playlist_Id": sqlalchemy.types.VARCHAR(length=225), }
                    )
    # transfer the video info to mysql
    vid_info.to_sql('video_info', con=engine, if_exists='append', index=False,
                    dtype = {'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                             'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                             'Title': sqlalchemy.types.VARCHAR(length=225),
                             'Thumbnail': sqlalchemy.types.VARCHAR(length=225),
                             'Description': sqlalchemy.types.TEXT,
                             'Published_Date': sqlalchemy.types.VARCHAR(length=50),
                             'Duration': sqlalchemy.types.VARCHAR(length=50),
                             'Views': sqlalchemy.types.BigInteger,
                             'Likes': sqlalchemy.types.BigInteger,
                             'Dislikes': sqlalchemy.types.BigInteger,
                             'Comments': sqlalchemy.types.BigInteger,
                             'Favorite_Count': sqlalchemy.types.INT,
                             'Caption_Status': sqlalchemy.types.VARCHAR(length=225), }
                    )
    # transfer the comment info to mysql
    cmnt_inf.to_sql('comment_info', con=engine, if_exists='append', index=False,
                    dtype={'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                           'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                           'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                           'Comment_Text': sqlalchemy.types.TEXT,
                           'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                           'Comment_Published': sqlalchemy.types.String(length=50), }
                    )

    # transfer the playlist info to mysql
    play_info.to_sql('playlist_info', con=engine, if_exists='append', index=False,
                     dtype={"Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                            'Title': sqlalchemy.types.VARCHAR(length=225),
                            "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                            'PublishedAt': sqlalchemy.types.VARCHAR(length=50),
                            'Video_count': sqlalchemy.types.INT}
                     )


#this is to inform the interpretor where to start the programe
if __name__ == "__main__":
    existing_ch = []
    existing_channels = existing_channels_in_Database()
    for channels in existing_channels:
        existing_ch.append(channels)
    add_sidebar = st.sidebar

    #adding side-bar
    with add_sidebar:

        add_sidebar.title('Data Harvesting & Warehousing')
        add_sidebar.text('By Rafadh Rafeek')
        st.write('Existing Channels in Database')
        st.dataframe(pd.DataFrame(existing_ch, columns=['Channel Id','Channel Name']))
        clear_data_btn = st.button('Clear Database')
        if clear_data_btn:
            clear_database()
        channel_id = add_sidebar.text_input('Enter The Channel ID Below: ')
        side_col1, side_col2 = add_sidebar.columns(2)
        if 'channel_id_list' not in ss:
            ss.channel_id_list = []
        if 'all_channel_info' not in ss:
            ss.all_channel_info = pd.DataFrame()
        if 'all_video_info' not in ss:
            ss.all_video_info = pd.DataFrame()
        if 'all_comment_info' not in ss:
            ss.all_comment_info = pd.DataFrame()
        if 'all_playlist_info' not in ss:
            ss.all_playlist_info = pd.DataFrame()

        col1, col2 = st.columns(2)
        # button to grab data from youtube using channel id
        grab_data_btn = side_col1.button('Grab Data')
        if grab_data_btn:
            if channel_id not in [channel[0] for channel in existing_ch]:
                try:
                    if len(ss.channel_id_list) <= 10 - len(existing_ch):
                        if str(channel_id) not in ss.channel_id_list:
                            data_grabbing_process(channel_id, youtube)
                            ss.channel_id_list.append(channel_id)
                            if ss.all_channel_info.empty:
                                add_sidebar.error('There is no Data to show. Grab data and try again')
                            else:
                                add_sidebar.dataframe(ss.all_channel_info)
                        else:
                            add_sidebar.error('Channel Id Already Existing')
                    else:
                        add_sidebar.error('This App is Limited Only For 10 Channels.')
                except:
                    add_sidebar.error('Please ensure that you have entered a valid Channel ID. If the issue persists, it may be due to the exhaustion of today’s API quota. In this case, we kindly request you to try again tomorrow. Thank you for your understanding.')
            else:
                st.error('Channel Id Already Existing in the Database')
        # button to save data to sql
        save_analyse_btn = side_col2.button('Save To Analyse Data')
        if save_analyse_btn:
            if ss.all_channel_info.empty:
                add_sidebar.error('There is no Data to Save. Grab data and try again')
            else:
                insert_data_to_mysql(ss.all_channel_info,ss.all_video_info,ss.all_comment_info,ss.all_playlist_info)
                ss.all_channel_info = pd.DataFrame()
                ss.all_video_info = pd.DataFrame()
                ss.all_comment_info = pd.DataFrame()
                ss.all_playlist_info = pd.DataFrame()
    # question part
    selected_option = st.selectbox('**Select your Question**',
                                  ('Select your Question',
                                    '1. What are the names of all the videos and their corresponding channels?',
                                   '2. Which channels have the most number of videos, and how many videos do they have?',
                                   '3. What are the top 10 most viewed videos and their respective channels?',
                                   '4. How many comments were made on each video, and what are their corresponding video names?',
                                   '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                   '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                   '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                   '8. What are the names of all the channels that have published videos in the year 2022?',
                                   '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                   '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                                   key='collection_question')

    if selected_option == '1. What are the names of all the videos and their corresponding channels?':
        query = "SELECT video_info.Title,channel_info.Channel_Name FROM channel_info JOIN video_info ON channel_info.Channel_Id = video_info.Channel_Id;"
        result_1 = get_cursor(query)
        df1 = pd.DataFrame(result_1, columns=['Video Name','Channel Name']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    if selected_option == '2. Which channels have the most number of videos, and how many videos do they have?':
        query = "SELECT Channel_Name, Total_Videos FROM channel_info ORDER BY Total_Videos desc;"
        result_2 = get_cursor(query)
        df2 = pd.DataFrame(result_2, columns=['Channel Name','Video Count']).reset_index(drop=True)
        df2.index += 1
        st.dataframe(df2)

    if selected_option == '3. What are the top 10 most viewed videos and their respective channels?':
        query = "SELECT video_info.Views as Views, video_info.Title, channel_info.Channel_Name FROM video_info join channel_info on channel_info.Channel_Id = video_info.Channel_Id ORDER BY Views desc limit 10;"
        result_3 = get_cursor(query)
        df3 = pd.DataFrame(result_3, columns=['Video Views','Video Name','Channel Name']).reset_index(drop=True)
        df3.index += 1
        st.dataframe(df3)

    if selected_option == '4. How many comments were made on each video, and what are their corresponding video names?':
        query = "SELECT video_info.Comments as comments, video_info.Title FROM video_info ORDER BY comments desc;"
        result_4 = get_cursor(query)
        df4 = pd.DataFrame(result_4, columns=['Comments Count','Video Name']).reset_index(drop=True)
        df4.index += 1
        st.dataframe(df4)

    if selected_option == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query = "select video_info.likes, video_info.Title, channel_info.Channel_Name from video_info join channel_info on video_info.Channel_id = channel_info.Channel_ID order by likes desc limit 10;"
        result_5 = get_cursor(query)
        df5 = pd.DataFrame(result_5, columns=['Likes Count','Video Name','Channel Name']).reset_index(drop=True)
        df5.index += 1
        st.dataframe(df5)

    if selected_option == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        query = "select Likes, Dislikes, Title from video_info ;"
        result_6 = get_cursor(query)
        df6 = pd.DataFrame(result_6, columns=['Likes Count','Dislikes Count','Video Name']).reset_index(drop=True)
        df6.index += 1
        st.dataframe(df6)

    if selected_option == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        query = "select Views,Channel_Name from channel_info order by Views desc;"
        result_7 = get_cursor(query)
        df7 = pd.DataFrame(result_7, columns=['Total Views','Channel Name']).reset_index(drop=True)
        df7.index += 1
        st.dataframe(df7)

    if selected_option == '8. What are the names of all the channels that have published videos in the year 2022?':
        query = ("select Channel_Name, date from (select str_to_date(video_info.Published_Date, '%Y-%m-%dT') as date, channel_info.Channel_Name as Channel_Name from video_info join channel_info on video_info.Channel_id = channel_info.Channel_ID ) as e where year(e.date)=2022;")
        result_8 = get_cursor(query)
        df8 = pd.DataFrame(result_8, columns=['Channel Name', 'Date Published']).reset_index(drop=True)
        df8.index += 1
        st.dataframe(df8)

    if selected_option == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query = ("select time_format(SEC_TO_TIME(avg(TIME_TO_SEC(e.Duration))), '%H:%i:%s'),e.name from (select video_info.Duration as Duration, video_info.Channel_Id, channel_info.Channel_Name as name from video_info join channel_info on video_info.Channel_id = channel_info.Channel_ID) as e group by name")
        result_9 = get_cursor(query)
        df9 = pd.DataFrame(result_9, columns=['Average Duration', 'Channel Name']).reset_index(drop=True)
        df9.index += 1
        st.dataframe(df9)

    if selected_option ==  '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        query = ("select video_info.Comments as cmnt,video_info.Title, channel_info.Channel_name from video_info join channel_info on video_info.Channel_id = channel_info.Channel_ID order by cmnt desc limit 10;")
        result_10 = get_cursor(query)
        df10 = pd.DataFrame(result_10, columns=['Comment Count', 'Video Name','Channel Name']).reset_index(drop=True)
        df10.index += 1
        st.dataframe(df10)
