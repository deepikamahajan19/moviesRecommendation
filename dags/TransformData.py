#import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as funs
#from pyspark.sql.types import StructType, StructField,StringType,FloatType,IntegerType,ArrayType,DoubleType,LongType
import os


def transform_data():
    spark=SparkSession.builder.appName("transf").getOrCreate()
    dir=os.getcwd()
    def read_files(spark):
        movies_file=dir+"/raw_data/movies_details.parquet"
        genre_file=dir+"/raw_data/genres_details.parquet"
        movies_df=spark.read.option("path",movies_file).load()
        genres_df=spark.read.option("path",genre_file).load()
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
        movies_df.write.mode("overwrite").csv(dir+"/transformed_data/movies_details.csv")
        genre_df.write.mode("overwrite").csv(dir+"/transformed_data/movies_genre.csv")

    
    write_transformed_data()
    
    spark.stop()
transform_data()
