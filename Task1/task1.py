import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, \
    current_date, year, row_number, explode, split, floor, expr, count
from pyspark.sql.window import Window

TSV_PATH = os.environ['TSV_PATH']


# Creating a spark session, and the pathways

spark = SparkSession.builder.master('local').appName(
    'IMDb_analysis').getOrCreate()

spark.sparkContext.setLogLevel('WARN')

NAME_DATA_PATH = '/tmp/task1/persons.tsv'
RATINGS_PATH = TSV_PATH + "/ratings.tsv"
ORDERS_PATH = '/tmp/task1/orders.tsv'
BASICS_PATH = TSV_PATH + '/basics.tsv'
EPISODES_PATH = '/tmp/task1/episodes.tsv'
CREW_PATH = '/tmp/task1/crew.tsv'
PRINCIPALS_PATH = '/tmp/task1/principals.tsv'


def get_all_ratings():
    df_ratings = spark.read.option('sep', '\t').csv(RATINGS_PATH,
                                                    header=True,
                                                    inferSchema=True)
    return df_ratings


def get_relevant_rating():
    df_rating = get_all_ratings()

    relevant_ratings = df_rating.filter(df_rating['numVotes'] > 100_000)
    return relevant_ratings


def get_movies():
    df_basics = spark.read.option('sep', '\t').csv(BASICS_PATH,
                                                   header=True,
                                                   inferSchema=True)
    df_movies = df_basics.filter(df_basics['titleType'] == 'movie')
    return df_movies


def get_relevant_movies():
    relevant_ratings = get_relevant_rating()
    df_movies = get_movies()
    relevant_movies = relevant_ratings.join(df_movies, on='tconst',
                                            how='inner')
    return relevant_movies


def get_actors():
    principal_df = spark.read.option('sep', '\t').csv(PRINCIPALS_PATH,
                                                      header=True,
                                                      inferSchema=True)
    principal_df = principal_df.filter(principal_df['category'] == 'actor')

    return principal_df


def get_principal_directors():
    principal_dir_df = spark.read.option('sep', '\t').csv(PRINCIPALS_PATH,
                                                          header=True,
                                                          inferSchema=True)
    principal_dir_df = principal_dir_df.filter(
        principal_dir_df['category'] == 'director')

    return principal_dir_df


def all_time_best_100movies():
    relevant_movies = get_relevant_movies()

    all_time_best_top100 = relevant_movies.orderBy(
        col('averageRating').desc()).limit(100)
    desired_columns_order = ['tconst', 'primaryTitle', 'numVotes',
                             'averageRating', 'startYear']
    all_time_best_top100 = all_time_best_top100.select(desired_columns_order)
    return all_time_best_top100


def get_best_movies_last10_years():
    current_year = year(current_date())
    last_10_years_range = (current_year - 9, current_year)
    relevant_movies = get_relevant_movies()

    desired_columns_order = ['tconst', 'primaryTitle', 'numVotes',
                             'averageRating', 'startYear']
    best_last_10years = relevant_movies.filter(
        (relevant_movies['startYear'] >= last_10_years_range[0]) &
        (relevant_movies['startYear'] <= last_10_years_range[1])
    )
    best_last_10years = best_last_10years.select(
        desired_columns_order).orderBy(col('averageRating').desc()).limit(100)
    best_last_10years.show()


def get_popular_movies_60s():
    year_range_60s = (1960, 1969)
    relevant_movies = get_relevant_movies()

    desired_columns_order = ['tconst', 'primaryTitle', 'numVotes',
                             'averageRating', 'startYear']
    best_films_60s = relevant_movies.filter(
        (relevant_movies['startYear'] >= year_range_60s[0]) &
        (relevant_movies['startYear'] <= year_range_60s[1])
    )
    best_films_60s = best_films_60s.select(desired_columns_order).limit(
        100).orderBy(col('averageRating').desc())
    best_films_60s.show()

    def top10_films_by_genre():
        relevant_movies = get_relevant_movies()

        split_movies = relevant_movies.withColumn("genres",
                                                  split(col("genres"), ","))
        exploded_movies = split_movies.withColumn('genre',
                                                  explode(col('genres')))
        window_spec = Window.partitionBy('genre').orderBy(
            col('averageRating').desc())

        ranked_movies = exploded_movies.withColumn('rank', row_number().over(
            window_spec))

        top_10_movies_by_genre = ranked_movies.filter(col('rank') <= 10)

        selected_columns = ['tconst', 'averageRating', 'numVotes',
                            'primaryTitle', 'startYear', 'genre']
        result_df = top_10_movies_by_genre.select(selected_columns)
        return result_df

    # 3rd task
    def get_populat_movies_by_genre_and_decade():
        relevant_movies = get_relevant_movies()

        split_movies = relevant_movies.withColumn("genres",
                                                  split(col("genres"), ","))
        exploded_movies = split_movies.withColumn('genre',
                                                  explode(col('genres')))
        year_range_movies = exploded_movies.withColumn('yearRange', expr(f"concat(floor(startYear/10) *10, \
                                                                        '-', floor(startYear/10)*10 + 9)"))
        window_spec = Window.partitionBy('yearRange', 'genre').orderBy(
            col('averageRating').desc())
        ranked_movies = year_range_movies.withColumn('rank', row_number().over(
            window_spec))
        top_10_movies_by_genre = ranked_movies.filter(col('rank') <= 10)

        selected_columns = ['tconst', 'averageRating', 'numVotes',
                            'primaryTitle', 'startYear', 'genre', 'yearRange']
        result_df = top_10_movies_by_genre.select(selected_columns)
        final_ordered_df = result_df.orderBy(col('yearRange').desc())
        return final_ordered_df

    # Task 4


# Get the actors
def get_names(NAME_DATA_PATH):
    df_names = spark.read.option('sep', '\t').csv(NAME_DATA_PATH,
                                                  header=True,
                                                  inferSchema=True)
    split_names = df_names.withColumn("knownForTitles",
                                      split(col("knownForTitles"), ","))
    exploded_names = split_names.withColumn('knownForTitles',
                                            explode(col('knownForTitles')))
    profession_split = exploded_names.withColumn("primaryProfession", split(
        col('primaryProfession'), ','))
    profession_explode = profession_split.withColumn('primaryProfession',
                                                     explode(
                                                         col('primaryProfession')))
    actors = profession_explode.filter(
        profession_explode.primaryProfession == 'actor')

    desired_columns = ["nconst", "primaryName", "deathYear"]
    actors = actors.select(desired_columns)

    return actors
