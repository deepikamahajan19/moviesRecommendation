## Movies Recommendation 
Recommend movies using "cosine similarity algorithm" based on the movie given by user.

### Approach used: ###
  Based on input movie's genre and langugae in which the input movie is, Im flitering out the dataset.
  On filtered dataset applying "Cosine Similarity algorithm" and get the movies recommendations.
  
### Data set used: ###
  * Around 2500 unique movies are being used which is fetched using restAPI call.
  * Rating associated with each movie based on the average ratings given by the viewers of movie.
  * For each movie a list of genres are there.

### Tech stack used: ###
  * Apache PySpark for transform data.
  * Pandas for extracting data.
  * Airflow for orchestration.
    
### Prerequisites: ###
  * Python 3
  * Java 11 or above
  * Any python environment(like Spyder, pycharm)
  * AWS account for storage

### Getting Started: ###
  Start by fetching the project and run dags -> main.py file. 

### Work in progress: ###
  Streaming the data.
