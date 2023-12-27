#import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as funs
#from pyspark.sql.types import StructType, StructField,StringType,FloatType,IntegerType,ArrayType,DoubleType,LongType
import os
import boto3

def transform_data():
    spark=SparkSession.builder.appName("transf").getOrCreate()
    dir="/Users/deepikamahajan/Desktop/airflow-docker/dags"
    def read_files(spark):
        s3_client = boto3.client('s3')

        # Specify the bucket name and the object key (file path in S3)
        bucket_name = 'ott-snowflake'
        object_key = 'raw_data/movies_df.parquet'

        # Specify the local path to save the file
        movies_local_file_path = dir+"/RawData/movies_details.parquet"

        # Download the file
        s3_client.download_file(bucket_name, object_key, movies_local_file_path)

        object_key = 'raw_data/genre_df.parquet'
        genre_local_file_path=dir+"/RawData/genres_details.parquet"
        s3_client.download_file(bucket_name, object_key, genre_local_file_path)
        #movies_file=dir+"/RawData/movies_details.parquet"
        #genre_file=dir+"/RawData/genres_details.parquet"
        movies_df=spark.read.option("path",movies_local_file_path).load()
        movies_df.show(5)
        genres_df=spark.read.option("path",genre_local_file_path).load()
        genres_df.show(5)
        return movies_df, genres_df
    
    
    def movies_data_transform(dfs=read_files(spark)):
        movies,genre=dfs[0],dfs[1]
        movies_df=movies.select(movies.id.alias("id"),movies.original_title.alias('title'),movies.rating.alias("ratings"),
                       movies.original_language.alias("language"),movies.overview.alias("overview"))
        movies_genre_df=movies.select(movies.id.alias("movie_id"),funs.explode_outer("genre").
                                      alias("genre_id"))
        
        movies_genre_df=movies_genre_df.join(genre,movies_genre_df.genre_id==genre.id,"inner")
        
        movies_genre_df=movies_genre_df.select("movie_id","genre_id",movies_genre_df.name.alias("genre_name"))
        movies_genre_df.show(5)
        return movies_df,movies_genre_df

    
    def edit_null(dfs=movies_data_transform()):
        
        movies_df,genre_df=dfs[0],dfs[1]
        movies_df=movies_df.fillna({"ratings":0.0,"overview":" ","language":"en"}) 
        return movies_df,genre_df
    
    def write_transformed_data(dfs=edit_null()):
        movies_df,genre_df=dfs[0],dfs[1]
        movies_df.write.mode("overwrite").csv(dir+"/TransformedData/movies_details.csv")
        genre_df.write.mode("overwrite").csv(dir+"/TransformedData/movies_genre.csv")

    
    write_transformed_data()
    
    spark.stop()
transform_data()
