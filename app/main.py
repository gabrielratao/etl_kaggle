from app.etl.kaggle import KaggleService
from app.insight.movies_info import MoviesInfo
from app.properties import url_datasets


def main():
    for dataset, dataset_info in url_datasets.items():
        print(f'Iniciando processo para o dataset: {dataset}')
    
        KaggleService(
            dataset=dataset,
            dataset_url=dataset_info['url'],
            zip_folder=dataset_info['zip_folder']
        ).run()

    print('Iniciando extração de informação com base nos dados')

    movies_info = MoviesInfo()
    movies_info.run(dataset_types=['top_movies', 'top_worst_movies'])
    movies_info.format_genre_rating_info()


if __name__ == '__main__':
    main()
