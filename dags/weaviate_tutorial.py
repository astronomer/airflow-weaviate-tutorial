from airflow.decorators import dag, task
from pendulum import datetime
from pathlib import Path
from weaviate_provider.operators.weaviate import (
    WeaviateCheckSchemaOperator,
    WeaviateCreateSchemaOperator,
)
from weaviate_provider.hooks.weaviate import WeaviateHook
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
import json
from airflow.models.param import Param
from include.movie_data.text_to_parquet_script import create_parquet_file_from_txt

WEAVIATE_USER_CONN_ID = "weaviate_user"
WEAVIATE_ADMIN_CONN_ID = "weaviate_admin"
PARQUET_FILE_PATH = "include/movie_data/movie_data.parquet"
TEXT_FILE_PATH = "include/movie_data/movie_data.txt"


@dag(
    start_date=datetime(2023, 9, 1),
    schedule=None,
    catchup=False,
    tags=["weaviate"],
    params={
        "movie_concepts": Param(
            ["discovery", "friends"],
            type="array",
            description="What kind of movie do you want to watch today? Add one concept per line.",
        )
    },
)
def weaviate_tutorial():
    # check if the movie schema exists in the weaviate instance
    # the operator returns True if the schema exists, False otherwise
    check_schema = WeaviateCheckSchemaOperator(
        task_id="check_schema",
        weaviate_conn_id=WEAVIATE_USER_CONN_ID,
        class_object_data=Path("include/movie_data/weaviate_schema.json").read_text(),
    )

    # decide if the movie schema should be created
    @task.branch
    def branch_create_schema(schema_exists: bool) -> str:
        """
        The WeaviateCheckSchemaOperator returns a boolean.  If the schema
        doesn't exist we can use a branch operator to conditionally create
        it.
        """
        WeaviateHook("weaviate_admin").get_conn().schema.delete_all()
        if schema_exists:
            return "schema_exists"
        else:
            return "create_schema"

    create_schema = WeaviateCreateSchemaOperator(
        task_id="create_schema",
        weaviate_conn_id=WEAVIATE_ADMIN_CONN_ID,
        class_object_data="file://include/movie_data/weaviate_schema.json",
    )

    schema_exists = EmptyOperator(task_id="schema_exists")

    @task
    def create_parquet_file(text_file_path, parquet_file_path):
        create_parquet_file_from_txt(text_file_path, parquet_file_path)

    @task.weaviate_import(
        weaviate_conn_id=WEAVIATE_ADMIN_CONN_ID, trigger_rule="none_failed"
    )
    def import_data(class_name):
        import pandas as pd

        df = pd.read_parquet(PARQUET_FILE_PATH)

        return {
            "data": df,
            "class_name": class_name,
            "uuid_column": "movie_id",
        }

    import_data_obj = import_data(class_name="Movie")

    @task
    def query_embeddings(weaviate_conn_id, **context):
        hook = WeaviateHook(weaviate_conn_id)
        movie_concepts = context["params"]["movie_concepts"]

        query = (
            """
            {
                Get {
                    Movie(nearText: {
                        concepts: """
            + json.dumps(movie_concepts)
            + """,
                        distance: 0.75
                    }) {
                        title
                        year
                        description
                        _additional {
                            certainty
                            distance
                        }
                    }
                }
            }
            """
        )

        response = hook.run(query=query)
        top_result = response["data"]["Get"]["Movie"][0]
        print(f"The top result for the concept(s) {movie_concepts} is:")
        print(f"The movie {top_result['title']}, released in {top_result['year']}.")
        print(f"IMDB describes the movie as: {top_result['description']}")

    chain(
        branch_create_schema(check_schema.output),
        [create_schema, schema_exists],
        import_data_obj,
        query_embeddings(WEAVIATE_USER_CONN_ID),
    )

    chain(create_parquet_file(TEXT_FILE_PATH, PARQUET_FILE_PATH), import_data_obj)


weaviate_tutorial()
