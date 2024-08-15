"""
## Use the Airflow Weaviate Provider to generate and query vectors for movie descriptions

This DAG runs a simple MLOps pipeline that uses the Weaviate Provider to import 
movie descriptions, generate vectors for them, and query the vectors for movies based on
concept descriptions.
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from weaviate.util import generate_uuid5
from pendulum import datetime
import logging
import re

t_log = logging.getLogger("airflow.task")

WEAVIATE_USER_CONN_ID = "weaviate_default"
TEXT_FILE_PATH = "include/movie_data.txt"
# the base collection name is used to create a unique collection name for the vectorizer
# note that it is best practice to capitalize the first letter of the collection name
COLLECTION_NAME_BASE = "Movie"

# set the vectorizer to text2vec-openai if you want to use the openai model
# note that using the OpenAI vectorizer requires a valid API key in the
# AIRFLOW_CONN_WEAVIATE_DEFAULT connection.
# If you want to use a different vectorizer
# (https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules)
# make sure to also add it to the weaviate configuration's `ENABLE_MODULES` list
# for example in the docker-compose.override.yml file
VECTORIZER = "text2vec-transformers"

# the collection name is a combination of the base collection name and the vectorizer
COLLECTION_NAME = COLLECTION_NAME_BASE + "_" + VECTORIZER.replace("-", "_")


@dag(
    start_date=datetime(2023, 9, 1),
    schedule=None,
    catchup=False,
    tags=["weaviate"],
    params={
        "movie_concepts": Param(
            ["innovation", "friends"],
            type="array",
            description=(
                "What kind of movie do you want to watch today?"
                + " Add one concept per line."
            ),
        ),
    },
)
def query_movie_vectors():
    @task.branch
    def check_for_collection(conn_id: str, collection_name: str) -> bool:
        "Check if the provided collection already exists and decide on the next step."
        # connect to Weaviate using the Airflow connection `conn_id`
        hook = WeaviateHook(conn_id)

        # check if the collection exists in the Weaviate database
        collection = hook.get_conn().collections.exists(collection_name)

        if collection:
            t_log.info(f"Collection {collection_name} already exists.")
            return "collection_exists"
        else:
            t_log.info(f"collection {collection_name} does not exist yet.")
            return "create_collection"

    @task
    def create_collection(conn_id: str, collection_name: str, vectorizer: str):
        "Create a collection with the provided name and vectorizer."
        hook = WeaviateHook(conn_id)

        hook.create_collection(
            name=collection_name,
        )

    collection_exists = EmptyOperator(task_id="collection_exists")

    def import_data_func(text_file_path: str, collection_name: str):
        "Read the text file and create a list of dicts for ingestion to Weaviate."
        with open(text_file_path, "r") as f:
            lines = f.readlines()

            num_skipped_lines = 0
            data = []
            for line in lines:
                parts = line.split(":::")
                title_year = parts[1].strip()
                match = re.match(r"(.+) \((\d{4})\)", title_year)
                try:
                    title, year = match.groups()
                    year = int(year)
                # skip malformed lines
                except:
                    num_skipped_lines += 1
                    continue

                genre = parts[2].strip()
                description = parts[3].strip()

                data.append(
                    {
                        "movie_id": generate_uuid5(
                            identifier=[title, year, genre, description],
                            namespace=collection_name,
                        ),
                        "title": title,
                        "year": year,
                        "genre": genre,
                        "description": description,
                    }
                )

            print(
                f"Created a list with {len(data)} elements while skipping {num_skipped_lines} lines."
            )
            return data

    import_data = WeaviateIngestOperator(
        task_id="import_data",
        conn_id=WEAVIATE_USER_CONN_ID,
        collection_name=COLLECTION_NAME,
        input_json=import_data_func(
            text_file_path=TEXT_FILE_PATH, collection_name=COLLECTION_NAME
        ),
        trigger_rule="none_failed",
    )

    @task
    def query_embeddings(weaviate_conn_id: str, collection_name: str, **context):
        "Query the Weaviate instance for movies based on the provided concepts."
        hook = WeaviateHook(weaviate_conn_id)
        movie_concepts = context["params"]["movie_concepts"]

        my_movie_collection = hook.get_collection(collection_name)

        movie = my_movie_collection.query.near_text(
            query=movie_concepts,
            return_properties=["title", "year", "genre", "description"],
            limit=1,
        )

        movie_title = movie.objects[0].properties["title"]
        movie_year = movie.objects[0].properties["year"]
        movie_genre = movie.objects[0].properties["genre"]
        movie_description = movie.objects[0].properties["description"]

        t_log.info(f"You should watch {movie_title}!")
        t_log.info(f"It was filmed in {movie_year} and belongs to the {movie_genre} genre.")
        t_log.info(f"Description: {movie_description}")

    chain(
        check_for_collection(
            conn_id=WEAVIATE_USER_CONN_ID, collection_name=COLLECTION_NAME
        ),
        [
            create_collection(
                conn_id=WEAVIATE_USER_CONN_ID,
                collection_name=COLLECTION_NAME,
                vectorizer=VECTORIZER,
            ),
            collection_exists,
        ],
        import_data,
        query_embeddings(
            weaviate_conn_id=WEAVIATE_USER_CONN_ID, collection_name=COLLECTION_NAME
        ),
    )


query_movie_vectors()
