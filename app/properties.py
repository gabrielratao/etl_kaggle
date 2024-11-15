from dotenv import load_dotenv
import os

load_dotenv()

mongo_db = {
    'db_name': 'movies_kaggle',
    'colection': 'movies',
    'uri': os.getenv("Uri"),
}

url_datasets = {
    'top_movies': {
        "url": "https://www.kaggle.com/api/v1/datasets/download/octopusteam/imdb-top-1000-movies",
        "zip_folder": "datasets/imdb-top-rated-titles.zip"
    },
    'top_worst_movies': {
        "url": "https://www.kaggle.com/api/v1/datasets/download/octopusteam/imdb-top-1000-worst-rated-titles",
        "zip_folder": "datasets/imdb-worst-rated-titles.zip"
    }
}
