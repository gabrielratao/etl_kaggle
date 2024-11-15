import json
import zipfile
from typing import List

import pandas as pd
import requests

from app.database.db_mongo import MongoDB


class KaggleService:
    def __init__(self, dataset, dataset_url, zip_folder):
        # with open("C:\\.kaggle\\kaggle.json", "r") as f:
        #     credentials = json.load(f)
        #
        # self.user_name = credentials["username"]
        # self.key = credentials["key"]
        self.dataset = dataset
        self.dataset_url = dataset_url
        self.zip_folder = zip_folder

    def request_dataset(self):
        print('Requisição do dataset iniciada')
        response = self._request()

        if response.status_code == 200:
            self.extract_dataset_from_response(response)
        else:
            raise requests.exceptions.RequestException(
                f"Erro na requisição: HTTP {response.status_code} - {response.reason}"
            )
        print('Download do dataset concluído com sucesso')

    def _request(self):
        return requests.get(self.dataset_url)  # auth=(self.user_name, self.key), stream=True, timeout=10)

    def extract_dataset_from_response(self, response) -> None:
        with open(self.zip_folder, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    def read_dataset_from_zip(self) -> pd.DataFrame:
        with zipfile.ZipFile(self.zip_folder, 'r') as zf:
            # Lista os arquivos no ZIP
            zip_file_names = zf.namelist()

            csv_filename = zip_file_names[0]

            with zf.open(csv_filename) as file:
                df = pd.read_csv(file)

        return df

    def transform_dataset(self) -> List[dict]:
        df = self.read_dataset_from_zip()
        movies_doc = self._set_df_to_dict(df)
        movies_doc = self._set_movie_rating_key(movies_doc)

        print('Transformação do dataset para lista de JSON conluída com sucesso')

        return movies_doc

    @staticmethod
    def _set_df_to_dict(df: pd.DataFrame) -> List[dict]:
        return df.to_dict(orient="records")

    def _set_movie_rating_key(self, movies_doc: list) -> list:
        for movie in movies_doc:
            movie['rating'] = {
                'averageRating': movie['averageRating'],
                'numVotes': movie['numVotes']
            }
            movie['genres'] = movie['genres'].split(',')
            movie.pop('averageRating')
            movie.pop('numVotes')
            movie['source'] = self.dataset

        return movies_doc

    @staticmethod
    def load_dataset(movies: list):
        try:
            response = MongoDB.insert_many_documents(
                documents=movies
            )

            if response.get('acknowledged', False):
                print(
                    'Documentos inseridos com sucesso no banco! \n'
                    f'Total de documentos inseridos: {len(response.get('inserted_ids', []))}'
                )
            else:
                print('Houve um erro para inserir os documento no banco')

        except Exception as error:
            print(f'Erro na execução do insert_many {error}')

    def run(self):
        self.request_dataset()
        movies_documents: List[dict] = self.transform_dataset()
        self.load_dataset(movies_documents)
