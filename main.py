from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from requests import post, get
from dotenv import load_dotenv
import os
import base64
import json
import pandas as pd


#load the environment variables
load_dotenv()
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

def get_token():
    auth_str = client_id + ':' + client_secret
    auth_bytes = auth_str.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization" : "Basic " + auth_base64,
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    data = {"grant_type" : "client_credentials"}
    result = post(url, headers=headers, data=data)
    print(result)
    json_result = json.loads(result.content)
    print(json_result)
    token = json_result["access_token"]
    return token

def get_auth_header(token):
    return {"Authorization" : "Bearer " + token}

#search for an artist and get their top tracks
def search_for_artist(token, artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token=token)
    query = f"?q={artist_name}&type=artist&limit=1"
    query_url = url+query
    result = get(query_url, headers=headers)
    json_results = json.loads(result.content)["artists"]["items"]
    if len(json_results) == 0:
        print("No artist found")
        return None
    return json_results[0]

def get_songs_by_artist_US(token, artist_id):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US"
    #url = "https://api.spotify.com/v1/artists/39EHxSQAIaWusRqSI9xoyF"
    headers = get_auth_header(token=token)
    result = get(url, headers=headers)
    json_results = json.loads(result.content)["tracks"]
    if len(json_results) == 0:
        print("No tracks found")
        return None
    return json_results

#spark = SparkSession.builder.appName('Spotify EDA').config("spark.some.config.option", "some-value").getOrCreate()
#spark_context = spark.sparkContext
#spotify_artists_df = spark.read.csv('top10k-spotify-artist-metadata.csv')
#print(spotify_artists_df.show())
#exit()
#get to 10 songs in each region by top 10k artists
spotify_artists_df = pd.read_csv('top10k-spotify-artist-metadata.csv')
print(spotify_artists_df.head())
artist_data = {}
artist_data['Name'] = []
artist_data['ID'] = []
artist_data['Gender'] = []
artist_data['Age'] = []
artist_data['Country'] = []
artist_data['Genres'] = []
artist_data['Popularity'] = []
artist_data['Followers'] = []
artist_data['URI'] = []

token = get_token()
for i, artist in enumerate(spotify_artists_df['artist']):
    try:
        artist_data['Name'].append(artist)
        artist_gender = spotify_artists_df['gender'][i]
        artist_data['Gender'].append(artist_gender)
        artist_age = spotify_artists_df['age'][i]
        artist_data['Age'].append(artist_age)
        artist_country = spotify_artists_df['country'][i]
        artist_data['Country'].append(artist_country)
        artist_city_1 = spotify_artists_df['city_1'][i]
        artist_city_2 = spotify_artists_df['city_2'][i]
        artist_city_3 = spotify_artists_df['city_3'][i]
        #get the artist's ID
        result = search_for_artist(token, artist)
        artist_id = result['id']
        artist_data['ID'].append(artist_id)
        artist_genres = result['genres']
        artist_data['Genres'].append(artist_genres)
        artist_popularity = result['popularity']
        artist_data['Popularity'].append(artist_popularity)
        artist_followers = result['followers']['total']
        artist_data['Followers'].append(artist_followers)
        artist_uri = result['uri']
        artist_data['URI'].append(artist_uri)
        #adding all the artist data to artist dataframe
        #for idx, song in enumerate(get_songs_by_artist_US(token, artist_id)):
        #    print(f"{idx+1}: {song['name']}")
        #    print(song)
    except Exception as e:
        print('WARNING: ', e)
print(artist_data)