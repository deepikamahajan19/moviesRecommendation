import requests
import pandas as pd
import boto3
from io import BytesIO

def ingest_data():
    
        
    base_url = "https://api.themoviedb.org/3/movie/top_rated?language=en-US"
    movies=[]

    headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJiMWUwNTFhOGQwNDZjM2QwYjQwZTljOWY5ZTJmYzY5OSIsInN1YiI6IjY1N2IzZTRiZWM4YTQzMDBmZDdmMzdmNiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.FEjWkb4JKsKNZIphN2nMsOWCdlDSkBGoy5fNL6ViPfY"
        }

    def get_genres(headers):
        
        url = "https://api.themoviedb.org/3/genre/movie/list?language=en"


        response = requests.get(url, headers=headers)
        return response.json()

    
    #genre_data=get_genres(headers)

    def parse_genre(response):
        genres=[]
        for item in response["genres"]:
            genres.append(item)
        return genres
    
    

    
    def main_request(base_url,headers,x):
        response = requests.get(base_url+f"&page={x}", headers=headers)
        return response.json()
    
    def get_pages():
        return 49
    
    def parse_json(response):
        temp_movies=[]
        for item in response["results"]:
            res={
                "genre":item["genre_ids"],
                "id":item["id"],
                "original_title":item["original_title"],
                "rating":item["vote_average"],
                "release_date":item["release_date"],
                "overview":item["overview"],
                "original_language":item["original_language"]
                }
            temp_movies.append(res)
        return temp_movies
    
    genres=parse_genre(get_genres(headers))
    
    def save_data(movies,genres):
        #dir="/Users/deepikamahajan/Desktop/airflow-docker/dags" 
        movies_df=pd.DataFrame(movies)
        genre_df=pd.DataFrame(genres)
        parquet_buffer = BytesIO()
        genre_df.to_parquet(parquet_buffer, index=False)
        s3_client = boto3.client('s3')
        bucket_name = 'ott-snowflake'
        key = 'raw_data/genre_df.parquet'
        parquet_buffer.seek(0)  # Reset buffer position
        s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=parquet_buffer.getvalue()
        )
        
        # write movies_df to s3 bucket
        key = 'raw_data/movies_df.parquet'
        parquet_buffer.seek(0)  # Reset buffer position
        movies_df.to_parquet(parquet_buffer, index=False)
        s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=parquet_buffer.getvalue()
        )

        #movies_df.to_parquet(dir+"/RawData/movies_details.parquet")
        #genre_df.to_parquet(dir+"/RawData/genres_details.parquet")
    
    
    for page in range(1,get_pages()+1):
        movies.extend(parse_json(main_request(base_url, headers,page)))
    

    print(len(movies))

    save_data(movies, genres)    
    
ingest_data()
#s3=boto3.client("s3")
#s3.upload_file("ott_details.csv","ott-snowflake","ott_etl_snowflake.csv")
#s3=boto3.resource("s3")
#obj=s3.Object("ott-snowflake","ott_details.csv")