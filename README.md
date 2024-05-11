Introduction

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in SQL database, visualized the details using Matplotlib,Seaborn and plotly.

Table of Contents

Key Technologies and Skills, Installation, Usage, Features, Retrieving data from the YouTube API, Storing data in MySQL, Visualizing Datas Using Matplotlib,Seaborn and Plotly, Data Analysis, User InterFace, Contact

Key Technologies and Skills

Python scripting Data Collection API integration Streamlit Matplotlib Seaborn Plotly Data Management using MySQL

Installation To run this project, you need to install the following packages:

from googleapiclient.discovery import build import mysql.connector import json import pandas as pd from datetime import datetime,timedelta import streamlit as st import matplotlib.pyplot as plt import seaborn as sns

Usage To use this project, follow these steps:

Clone the repository: git clone https://github.com/Abdulsalam-47/Youtube-Harvesting-and-Warehousing.git Run the Streamlit app: streamlit run youtube.py Access the app in your browser at http://localhost:8501/

Features

Retrieve data from the YouTube API, including channel information, playlists, videos, and comments. Store the retrieved data in a MySQL database. Viewing fetched datas using Pandas. Analyze and visualize data using Matplotlib,Seaborn and Plotly. Perform queries on the SQL data warehouse. Streamlit using for User Interface.

Retrieving data from the YouTube API

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and merge it into a Dictionary file.

Storing data in MySQL

Database created based on Youtube API Reference.Created Table based on requirment, and the retrived data is stored in a SQL database with a structured format.Pandas DataFrame is used to viewing all over details.

Visualizing Datas Using Matplotlib,Seaborn and Plotly

With the integrated Matplotlib,Seaborn and Plotly library, users can create interactive and visually appealing charts and graphs to gain insights from the collected data, Utilizing the power of Plots, users can create various types of charts, including line charts, bar charts, scatter plots, pie charts, and more, These visualizations enhance the understanding of the data and make it easier to identify patterns, trends, and correlations.

Data Analysis

Channel Analysis: Channel analysis includes insights on playlists, videos, subscribers, views, likes, comments, and durations. Gain a deep understanding of the channel's performance and audience engagement through detailed visualizations and summaries.

Video Analysis: Video analysis focuses on views, likes, comments, and durations, enabling both an overall channel and specific channel perspectives. Leverage visual representations and metrics to extract valuable insights from individual videos.

User Interface

The Streamlit app provides an user friendly interface to interact with the charts and explore the data visually. Users can customize the visualizations, filter data, and zoom in or out to focus on specific aspects of the analysis. With the combined capabilities of Plots and Streamlit, the Data Analysis section empowers users to uncover valuable insights and make data-driven decisions.

Contact

Email: abdulsalam47492@gmail.com Linkedin: https://www.linkedin.com/in/abdul-salam-m-02801716b/

This project is licensed under the MIT License. Please review the LICENSE file for more details.
