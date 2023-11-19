Orchestrate Weaviate operations with Apache Airflow
===================================================

This repository contains the DAG code used in the [Orchestrate Weaviate operations with Apache Airflow tutorial](https://docs.astronomer.io/learn/airflow-weaviate). 

The DAG in this repository uses the following package:

- [Airflow Weaviate provider](https://airflow.apache.org/docs/apache-airflow-providers-weaviate/stable/index.html). 

# How to use this repository

This section explains how to run this repository with Airflow. Note that you will need to copy the contents of the `.env_example` file to a newly created `.env` file. No external connections are necessary to run this repository locally, but you can add your own credentials in the file if you wish to connect to your tools. 

If you want to use the `text2vec-openai` vectorizer you will need to provide an [OpenAI API key of at least tier 1](https://platform.openai.com/docs/guides/rate-limits/usage-tiers?context=tier-free) to the `X-OpenAI-Api-Key` parameter in the `AIRFLOW_CONN_WEAVIATE_DEFAULT` connection.

Download the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) to run Airflow locally in Docker. `astro` is the only package you will need to install locally.

1. Run `git clone https://github.com/astronomer/airflow-weaviate-tutorial.git` on your computer to create a local clone of this repository.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite, but you don't need in-depth Docker knowledge to run Airflow with the Astro CLI.
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.

In this project `astro dev start` spins up 6 Docker containers:

- The Airflow webserver, which runs the Airflow UI and can be accessed at `https://localhost:8080/`.
- The Airflow scheduler, which is responsible for monitoring and triggering tasks.
- The Airflow triggerer, which is an Airflow component used to run deferrable operators.
- The Airflow metadata database, which is a Postgres database that runs on port 5432.
- A local Weaviate instance, which can be accessed at `http://localhost:8081/v1`.
- A local t2v-transformers instance, which can be accessed at `http://localhost:8082/`.

## Resources

- [Orchestrate Weaviate operations with Apache Airflow](https://docs.astronomer.io/learn/airflow-weaviate).
- [Airflow Weaviate provider documentation](https://airflow.apache.org/docs/apache-airflow-providers-weaviate/stable/index.html).
- [Weaviate documentation](https://weaviate.io/developers/weaviate).
