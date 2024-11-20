from collections import defaultdict
from typing import List

import numpy as np
from matplotlib import pyplot as plt

from app.database.db_mongo import MongoDB


class MoviesInfo:
    def __init__(self):
        self._documents = []
        self._genre_rating = {}
        self._score_genres = {}

    def read_movies(self):
        self._documents.extend(MongoDB.read_all_documents())

    def get_movies_info(self, dataset_types):
        self.read_movies()
        self._get_genre_average(dataset_types)

    def _calculate_rating_genre(self, source: str) -> dict or None:
        filtered_movies = [movie for movie in self._documents if movie['tipo_dataset'] == source]

        score_genres = defaultdict(lambda: {'total_votos': 0, 'soma_notas': 0})

        for movie in filtered_movies:
            num_votes = movie['qualificação']['num_votos']
            average = movie['qualificação']['nota_média']

            for genre in movie['gêneros']:
                score_genres[genre]['total_votos'] += num_votes
                score_genres[genre]['soma_notas'] += average * num_votes

        for genre, rating in score_genres.items():
            score_genres[genre]['nota_final'] = rating['soma_notas'] / rating['total_votos']

        self._score_genres = score_genres
        # self._genre_rating = {
        #     'top_movies': max(score_genres.items(), key=lambda item: item[1]['nota_final']),
        #     'top_worst_movies': min(score_genres.items(), key=lambda item: item[1]['nota_final'])
        # }
        if source == 'top_movies':
            self._genre_rating['top_movies'] = max(score_genres.items(), key=lambda item: item[1]['nota_final'])
        elif source == 'top_worst_movies':
            self._genre_rating['top_worst_movies'] = min(score_genres.items(), key=lambda item: item[1]['nota_final'])

        self._gen_image(dataset=source)

    def _get_genre_average(self, dataset_types: List[str]):
        for dataset_type in dataset_types:
            self._calculate_rating_genre(dataset_type)

    @property
    def genre_rating(self):
        return self._genre_rating

    @property
    def score_genres(self):
        return self._score_genres

    def format_genre_rating_info(self):
        for dataset, (genre, rating) in self.genre_rating.items():
            print(
                f'{dataset} - Gênero {genre}'
                f'\ntotal de votos: {rating['total_votos']}'
                f'\nnota final: {rating['nota_final']:.2f}\n'
            )

    def _gen_image(self, dataset):
        cleaned_data = {}
        for genre, info in self._score_genres.items():
            genre_cleaned = genre.strip()  # Remove espaços extras
            cleaned_data[genre_cleaned] = info['nota_final']

        # Gerar gráfico de barras
        genres = list(cleaned_data.keys())
        ratings = list(cleaned_data.values())

        plt.figure(figsize=(10, 6))
        plt.barh(genres, ratings, color='skyblue')
        plt.xlabel('Nota Final')
        plt.ylabel('Gênero')
        plt.title(f'Notas Finais dos Gêneros de Filmes - {dataset}')

        plt.savefig(f'{dataset}.png')

    def run(self, dataset_types: List[str]):
        self.get_movies_info(dataset_types=dataset_types)
