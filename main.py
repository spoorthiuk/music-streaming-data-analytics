from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from requests import post, get
from dotenv import load_dotenv
import os
import base64
import json

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

def get_songs_by_artist(token, artist_id):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US"
    headers = get_auth_header(token=token)
    result = get(url, headers=headers)
    json_results = json.loads(result.content)["tracks"]
    if len(json_results) == 0:
        print("No tracks found")
        return None
    return json_results
token = get_token()
print(token)

result = search_for_artist(token, "ACDC")
artist_id = result["id"]
for idx, song in enumerate(get_songs_by_artist(token, artist_id)):
    print(f"{idx+1}: {song['name']}")