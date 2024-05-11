from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
import re
import json
from datetime import datetime
import numpy as np
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns


#SQL Connection

mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    password="root",
    database="youtube"
)

cursor = mydb.cursor()

def api_connection():
    api_key = "AIzaSyDDRdxgcrw2woSS09qJ5H4UEr32zgokw9g"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = api_connection()


#Get Channel Information

def Channel_Info(channel_ids):
    for channel_id in channel_ids:
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS channel_info (
                                channel_name VARCHAR(255),
                                channel_id VARCHAR(255) PRIMARY KEY,
                                subscribe INT,
                                views INT,
                                total_videos INT,
                                channel_description TEXT,
                                playlist_id VARCHAR(255)
                            )""")
            
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id
            )
            response = request.execute()
            
            for item in response.get('items',[]):
                details = dict(Channel_Name= item['snippet']['title'],
                    Channel_Id= item['id'],
                    Subscribers= item['statistics']['subscriberCount'],
                    Views= item['statistics']['viewCount'],
                    Total_Videos= item['statistics']['videoCount'],
                    Channel_Description= item['snippet']['description'],
                    Playlist_Id=item['contentDetails']['relatedPlaylists']['uploads']
                )
                
                # Check if the channel already exists in the database
                cursor.execute("SELECT channel_id FROM channel_info WHERE channel_id = %s", (details['Channel_Id'],))
                existing_channel = cursor.fetchone()
                
                if not existing_channel:
                    cursor.execute("INSERT INTO channel_info (channel_name, channel_id, subscribe, views, total_videos, channel_description, playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                   (details['Channel_Name'], details['Channel_Id'], details['Subscribers'], details['Views'], details['Total_Videos'], details['Channel_Description'], details['Playlist_Id']))
                    mydb.commit()
                else:
                    print(f"Channel with ID {details['Channel_Id']} already exists in the database.")

        except Exception as e:
            print(f"Error processing channel ID {channel_id}: {e}")

channel_ids = ["UClHGyN8q-_GmFxjMpBdjL0g","UCWbowecFn2dqdJSVAA2CRDw","UCcDwnq9FPdY22uFbFJBkdRw"]

Channel_Info(channel_ids)


# List of channel IDs
channel_ids = ["UClHGyN8q-_GmFxjMpBdjL0g","UCWbowecFn2dqdJSVAA2CRDw","UCcDwnq9FPdY22uFbFJBkdRw"]

# Function to fetch video IDs from a channel
def Get_Video_Id(channel_id):
    video_ids = []
    next_page_token = None
    while True:
        request = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=50,  # Adjust as per your requirements, max is 50
            pageToken=next_page_token
        )
        response = request.execute()
        items = response.get('items', [])
        video_ids.extend([item['id']['videoId'] for item in items if item['id']['kind'] == 'youtube#video'])
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    return video_ids

# Fetch video IDs for each channel
all_video_ids = []
for channel_id in channel_ids:
    video_ids = Get_Video_Id(channel_id)
    all_video_ids.extend(video_ids)

print(all_video_ids)


# Function to parse duration
def parse_duration(duration_str):
    try:
        # Check if the duration string is in the 'HH:MM:SS' format
        if ':' in duration_str:
            hours, minutes, seconds = map(int, duration_str.split(':'))
            total_seconds = hours * 3600 + minutes * 60 + seconds
        else:
            match = re.match(r'PT((\d+)H)?((\d+)M)?(\d+S)?', duration_str)
            hours = int(match.group(2)) if match.group(2) else 0
            minutes = int(match.group(4)) if match.group(4) else 0
            seconds = int(match.group(5)[:-1]) if match.group(5) else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds
    except (ValueError, AttributeError):
        return None
    
# Function to convert seconds to HH:MM:SS format
def seconds_to_hhmmss(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '{:02d}:{:02d}:{:02d}'.format(int(hours), int(minutes), int(seconds))

# Function to create video_details table
def create_video_details_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS video_details (
        id INT AUTO_INCREMENT PRIMARY KEY,
        channel_name VARCHAR(255),
        channel_id VARCHAR(255),
        video_id VARCHAR(255),
        title VARCHAR(255),
        tags JSON,
        thumbnail JSON,
        description TEXT,
        published_date DATETIME,
        duration VARCHAR(8),  -- Duration in HH:MM:SS format
        views INT,
        likes INT,
        dislikes INT,
        comments INT
    )
    """)

# Function to get video details
def Get_Video_Details(video_ids, cursor, mydb):
    video_list = []
    try:
        for video_id in video_ids:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response = request.execute()

            for item in response['items']:
                data = dict(
                    channel_name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    video_id=item['id'],
                    title=item['snippet']['title'],
                    tags=json.dumps(item.get('tags')),
                    thumbnail=json.dumps(item['snippet']['thumbnails']),
                    description=item['snippet'].get('description', ''),
                    published_date=item['snippet']['publishedAt'],
                    duration=item['contentDetails']['duration'],
                    views=item['statistics'].get('viewCount', 0),
                    likes=item['statistics'].get('likeCount', 0),
                    dislikes=item['statistics'].get('dislikeCount', 0),
                    comments=item['statistics'].get('commentCount', 0)
                )

                # Check if the video already exists in the database
                cursor.execute("SELECT video_id FROM video_details WHERE video_id = %s", (data['video_id'],))
                existing_video = cursor.fetchone()

                if not existing_video:
                    # Print duration string for debugging
                    print("Duration String:", data['duration'])

                    # Parse duration
                    duration_seconds = parse_duration(data['duration'])
                    if duration_seconds is not None:
                        duration = duration_seconds
                    else:
                        duration = 0

                    # Convert duration to HH:MM:SS format
                    duration_hhmmss = seconds_to_hhmmss(duration)

                    # Print duration for debugging
                    print("Duration (HH:MM:SS):", duration_hhmmss)

                    # Parse published date
                    iso_datetime = data['published_date']
                    parsed_datetime = datetime.fromisoformat(iso_datetime.replace('Z', '+00:00'))
                    mysql_published_date = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')

                    # Append to video list
                    video_list.append(data)

                    # Insert into database
                    cursor.execute("INSERT INTO video_details(channel_name, channel_id, video_id, title, tags, thumbnail, description, published_date, duration, views, likes, dislikes, comments) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                   (data['channel_name'], data['channel_id'], data['video_id'], data['title'], data['tags'], data['thumbnail'], data['description'], mysql_published_date, duration_hhmmss, data['views'], data['likes'], data['dislikes'], data['comments']))

                    mydb.commit()
                else:
                    print(f"Video with ID {data['video_id']} already exists in the database.")

    except Exception as e:
        print(f"Error: {e}")

    return video_list

# Get video details for all video IDs
create_video_details_table(cursor)
all_video_details = Get_Video_Details(all_video_ids, cursor, mydb)

print(all_video_details)


# Create MySQL table for comment details
try:
    cursor.execute("""CREATE TABLE IF NOT EXISTS comment_details (
                        comment_id VARCHAR(255),
                        video_id VARCHAR(255),
                        comment_text TEXT,
                        author VARCHAR(255),
                        published_date DATETIME
                    )""")
except Exception as e:
    print(f"Error creating table: {e}")

# Function to fetch comment details for a video ID
def get_comment_Details(video_id):
    comment_list = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
        )
        response = request.execute()

        # Check if comments are disabled
        if 'items' not in response:
            print(f"Comments are disabled for video ID {video_id}")
            return comment_list

        for item in response['items']:
            comment_det = {
                'comment_id': item['snippet']['topLevelComment']['id'],
                'video_id': item['snippet']['topLevelComment']['snippet']['videoId'],
                'comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'published_date': item['snippet']['topLevelComment']['snippet']['publishedAt']
            }

            # Check if the comment already exists in the database
            cursor.execute("SELECT comment_id FROM comment_details WHERE comment_id = %s", (comment_det['comment_id'],))
            existing_comment = cursor.fetchone()
            if not existing_comment:
                # Insert the comment into the database
                iso_datetime = comment_det['published_date']
                parsed_datetime = datetime.datetime.fromisoformat(iso_datetime.replace('Z', '+00:00'))
                mysql_published_dates = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')

                cursor.execute("INSERT INTO comment_details (comment_id, video_id, comment_text, author, published_date) VALUES (%s, %s, %s, %s, %s)",
                               (comment_det['comment_id'], comment_det['video_id'], comment_det['comment_text'], comment_det['author'], mysql_published_dates))
                mydb.commit()

            comment_list.append(comment_det)

    except Exception as e:
        print(f"Error fetching comments for video ID {video_id}: {e}")

    return comment_list

# Function to fetch comment details for all video IDs
def get_all_video_comments(all_video_ids):
    all_comments = []
    for video_id in all_video_ids:
        video_comments = get_comment_Details(video_id)
        all_comments.extend(video_comments)
    return all_comments

# Call the function to get all video comment details
all_video_comments = get_all_video_comments(all_video_ids)
print(all_video_comments)


def get_playlist_details(channel_ids):
    all_playlists = []
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS playlist_details (
                            playlist_id VARCHAR(255) PRIMARY KEY,
                            title VARCHAR(255),
                            channel_id VARCHAR(255),
                            published_date DATETIME,
                            video_count INT
                        )""")
        
        for channel_id in channel_ids:
            Next_Page_Token = None
            while True:
                request = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=Next_Page_Token
                )
                response = request.execute()

                for item in response.get('items', []):
                    playlist_det = {
                        'playlist_id': item['id'],
                        'title': item['snippet']['title'],
                        'channel_id': item['snippet']['channelId'],
                        'published_date': item['snippet']['publishedAt'],
                        'video_count': item['contentDetails']['itemCount']
                    }
                    all_playlists.append(playlist_det)

                    iso_datetime = playlist_det['published_date']
                    parsed_datetime = datetime.datetime.fromisoformat(iso_datetime.replace('Z', '+00:00'))
                    mysql_published_date = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')

                    # Check if the playlist already exists in the database
                    cursor.execute("SELECT playlist_id FROM playlist_details WHERE playlist_id = %s", (playlist_det['playlist_id'],))
                    existing_playlist = cursor.fetchone()

                    if not existing_playlist:
                        cursor.execute("INSERT INTO playlist_details (playlist_id, title, channel_id, published_date, video_count) VALUES (%s, %s, %s, %s, %s)",
                                       (playlist_det['playlist_id'], playlist_det['title'], playlist_det['channel_id'], mysql_published_date, playlist_det['video_count']))
                        mydb.commit()
                    else:
                        print(f"Playlist with ID {playlist_det['playlist_id']} already exists in the database.")

                Next_Page_Token = response.get('nextPageToken')
                if Next_Page_Token is None:
                    break

    except Exception as e:
        print(f"Error: {e}")

    return all_playlists

# Example usage:
channel_ids = ["UClHGyN8q-_GmFxjMpBdjL0g", "UCWbowecFn2dqdJSVAA2CRDw", "UCcDwnq9FPdY22uFbFJBkdRw"]
all_playlists = get_playlist_details(channel_ids)
print(all_playlists)


#Overall Function get detils

def fetch_all_data(channel_id):
    channel_info = Channel_Info(channel_id)
    video_ids = Get_Video_Id(channel_id)
    cursor = mydb.cursor()
    video_details = Get_Video_Details(video_ids, cursor, mydb)
    cursor.close()
    playlist_details = get_playlist_details(channel_id)
    comment_details = get_comment_Details(video_ids[0])
    
    # Convert dictionaries to DataFrames
    channel_df = pd.DataFrame([channel_info])
    video_df = pd.DataFrame(video_ids, columns=['video_id'])
    playlist_df = pd.DataFrame(playlist_details)
    video_detail_df = pd.DataFrame(video_details)
    comment_df = pd.DataFrame(comment_details)
    
    return {
        "channel_details": channel_df,
        "video_details": video_df,
        "comment_details": comment_df,
        "playlist_details": playlist_df,
        "video_data": video_detail_df
    }


def main():
    
    st.sidebar.header('Menu')
    option=st.sidebar.radio("Select Option",['Home','Queries'])
    if option=="Home":
            st.header(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]', divider='rainbow')
            channel_id = st.text_input("Enter Channel ID")
            
            if st.button("Get Channel Details"):
                details = fetch_all_data(channel_id)
                
                st.subheader('Channel Details')
                st.write(details["channel_details"])

                st.subheader('Video Details')
                st.write(details["video_data"])

                st.subheader('Comment Details')
                st.write(details["comment_details"])

                st.subheader('Playlist Details')
                st.write(details["playlist_details"])
            
    elif option == "Queries":
        st.header("Queries")

        questions = [
                   "1. What are the names of all the videos and their corresponding channels?",
                   "2. Which channels have the most number of videos, and how many videos do they have?",
                   "3. What are the top 10 most viewed videos and their respective channels?",
                   "4. How many comments were made on each video, and what are their corresponding video names?",
                   "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                   "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                   "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                   "8. What are the names of all the channels that have published videos in the year 2022?",
                   "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                   "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
                   ]

        selected_questions = st.multiselect("Select questions to execute", questions)
        if st.button("Run Selected Queries"):

            for selected_question in selected_questions:
        
                if selected_question == questions[0]:
                    cursor.execute("SELECT channel_name,title FROM video_details")
                    data = cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel Name', 'Title'])
                    st.write(df)
                    st.subheader('Bar Plot of Video Counts by Channel')
                    st.set_option('deprecation.showPyplotGlobalUse', False)
                    plt.figure(figsize=(8, 6))
                    sns.countplot(x='Channel Name', data=df)
                    plt.xticks(rotation=90)
                    st.pyplot()
                     
                elif selected_question == questions[1]:
                    cursor.execute("SELECT channel_name, COUNT(*) as video_count FROM video_details GROUP BY channel_name ORDER BY video_count DESC")
                    data=cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel Name', 'Counts'])
                    st.write(df)
                    st.subheader('Bar Plot of Video Counts by Channel')
                    st.set_option('deprecation.showPyplotGlobalUse', False)
                    plt.figure(figsize=(8, 6))
                    sns.barplot(x='Channel Name', y='Counts', data=df, palette='viridis')
                    plt.xticks(rotation=90)
                    plt.xlabel('Channel Name')
                    plt.ylabel('Counts')
                    plt.title('Video Counts by Channel')
                    st.pyplot()

                elif selected_question == questions[2]:
                    cursor.execute("SELECT channel_name,title,views FROM video_details ORDER BY views DESC LIMIT 10")
                    data=cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel Name', 'Title', 'Views'])
                    st.write(df)
                    st.subheader('Bar Chart of Views for Top 10 Videos and Channels')
                    plt.figure(figsize=(10, 6))
                    sns.barplot(data=df, x='Title', y='Views', hue='Channel Name', dodge=False, palette='viridis')
                    plt.xticks(rotation=90)
                    plt.xlabel('Video Title')
                    plt.ylabel('Views')
                    plt.title('Views for Top 10 Videos and Channels')
                    plt.legend(title='Channel Name', bbox_to_anchor=(1, 1))
                    st.pyplot()

                elif selected_question == questions[3]:
                    cursor.execute("SELECT title,comments FROM video_details")
                    data=cursor.fetchall()
                    df=df=pd.DataFrame(data, columns=['Title','Comments'])
                    st.write(df)

                elif selected_question == questions[4]:
                    cursor.execute("SELECT channel_name,MAX(likes) as max_likes FROM video_details GROUP BY channel_name")
                    data=cursor.fetchall()
                    df=pd.DataFrame(data, columns=['Channel_Name','Likes'])
                    st.write(df)
                    st.subheader('Horizondal bar chart for getting likes based on channel name')
                    st.set_option('deprecation.showPyplotGlobalUse', False)
                    plt.figure(figsize=(10,6))
                    sns.barplot(data=df,x='Likes',y='Channel_Name',palette='viridis')
                    plt.xticks(rotation=90)
                    plt.xlabel('Likes')
                    plt.ylabel('Channel_Name')
                    plt.title('Max_Likes from various channels')
                    plt.legend(title='Channel_Name',bbox_to_anchor=(1,1))
                    st.pyplot()

                elif selected_question == questions[5]:
                    cursor.execute("SELECT title, SUM(likes) as total_likes, SUM(dislikes) as total_dislikes FROM video_details GROUP BY title")
                    data=cursor.fetchall()
                    df=pd.DataFrame(data, columns=['Title','Likes','Dislikes'])
                    st.write(df)

                elif selected_question == questions[6]:
                    cursor.execute("SELECT channel_name, SUM(views) as total_views FROM video_details GROUP BY channel_name")
                    data=cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel_Name', 'Views'])
                    st.write(df)
                    st.subheader('Pie chart for total views based on channel name')
                    st.set_option('deprecation.showPyplotGlobalUse', False)
                    plt.figure(figsize=(10, 8))
                    plt.pie(df['Views'], labels=df['Channel_Name'], autopct='%1.1f%%', startangle=140)
                    plt.title('Total Views per Channel')
                    plt.axis('equal')  
                    st.pyplot()
                    
                elif selected_question == questions[7]:
                    cursor.execute("SELECT DISTINCT channel_name FROM video_details WHERE YEAR(published_date) = 2022;")
                    data=cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel_Name'])
                    df['Count'] = 1
                    st.write(df)
                    st.subheader('Bar chart for distinct channel names for videos published in 2022')
                    plt.figure(figsize=(10, 6))
                    plt.bar(df['Channel_Name'], df['Count'], color='skyblue')
                    plt.xlabel('Channel Name')
                    plt.ylabel('Count')
                    plt.title('Distinct Channel Names for Videos Published in 2022')
                    plt.xticks(rotation=90)
                    st.pyplot()

                elif selected_question == questions[8]:
                        try:
                            cursor.execute("""
                                SELECT 
                                    channel_name, 
                                    AVG(
                                        TIME_TO_SEC(
                                            TIMEDIFF(
                                                TIME(CONVERT(duration, TIME)), 
                                                TIME('00:00:00')
                                            )
                                        )
                                    ) AS avg_duration_seconds
                                FROM 
                                    video_details 
                                WHERE 
                                    duration != '00:00:00' -- Exclude zero durations
                                GROUP BY 
                                    channel_name
                            """)
                            data = cursor.fetchall()
                            
                            if not data:
                                st.warning("No data found.")
                            else:
                                df = pd.DataFrame(data, columns=['Channel_Name', 'Avg_Duration_Seconds'])
                                df['Avg_Duration'] = df['Avg_Duration_Seconds'].apply(seconds_to_hhmmss)
                                st.write(df)
                                plt.figure(figsize=(10, 6))
                                plt.scatter(df['Channel_Name'], df['Avg_Duration_Seconds'], color='skyblue')
                                plt.xlabel('Channel')
                                plt.ylabel('Average Duration (Seconds)')
                                plt.title('Average Duration of Videos for Each Channel')
                                plt.xticks(rotation=90)
                                plt.tight_layout()
                                st.pyplot()
                        except Exception as e:
                            st.error(f"Error occurred: {e}")

                elif selected_question == questions[9]:
                    cursor.execute("""SELECT title, channel_name, SUM(comments) as comments
                            FROM video_details 
                            GROUP BY title, channel_name 
                            ORDER BY comments DESC 
                            LIMIT 1
                        """)
                    data = cursor.fetchall()
                    df=pd.DataFrame(data,columns=['Title','Channel_Name','Comments'])
                    st.write(df)
               
if __name__ == "__main__":
    main()

    mydb.close()
