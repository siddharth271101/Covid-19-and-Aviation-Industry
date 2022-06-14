# Impact of Covid-19 on the Aviation Industry

## Introduction
Aviation provides the only rapid worldwide transportation network, which makes it essential for global business. It generates economic growth, creates jobs, and facilitates international trade and tourism. The air transport industry supports a total of 65.5 million jobs globally. It provides 10.2 million direct jobs.

This project aims to analyse the impact of Covid-19 on the aviation industry. It also provided a great opportunity to develop skills and experience in a range of tools such as Apache Airflow, Apache Spark, Tableau and some of the AWS cloud services.

## Architecture
<p align="left">
    <img src="https://github.com/siddharth271101/Covid-19-and-Aviation-Industry/blob/main/assets/images/architecture.png">
</p>

## Airflow Data Pipeline
<p align="left">
    <img src="https://github.com/siddharth271101/Covid-19-and-Aviation-Industry/blob/main/assets/images/Airflow_graph_view.png">
</p>

Airflow orchestrates the following tasks:

1. Upload the data and scripts from local machine to S3 bucket
2. Provision an EMR cluster
3. Submit a spark job to EMR cluster that executes the ETL workflow
4. Wait for the spark submission to complete
5. Terminate the EMR cluster


## Data sources


1. [Opensky](https://zenodo.org/record/6603766)

    The data in this dataset is derived and cleaned from the full OpenSky dataset to illustrate the development of air traffic during the COVID-19 pandemic. It spans all flights seen by the network's more than 2500 members since 1 January 2019.

    In order to avoid the out-of-memory issue, data is incrementally loaded into the s3 bucket.

    Martin Strohmeier, Xavier Olive, Jannis Lübbe, Matthias Schäfer, and Vincent Lenders

    **"Crowdsourced air traffic data from the OpenSky Network 2019–2020"**

    *Earth System Science Data* 13(2), 2021

    [https://doi.org/10.5194/essd-13-357-2021](https://doi.org/10.5194/essd-13-357-2021)

2. [JHU CSSE](https://github.com/CSSEGISandData/COVID-19)

    This dataset includes time-series data tracking the number of people affected by COVID-19 worldwide

3. [Airport Codes](https://datahub.io/core/airport-codes)

    The data is in CSV and contains the list of all airport codes.

4. [Country-Codes](https://datahub.io/core/country-list)

    The dataset contains country names (official short names in English) in alphabetical order as given in ISO 3166-1 and the corresponding ISO 3166-1-alpha-2 code elements. [ISO 3166-1]

5. [Continent-codes](https://www.kaggle.com/datasets/andradaolteanu/country-mapping-iso-continent-region)

    The dataset was used to map countries with continents.

6. [States](https://www.kaggle.com/datasets/arjunaraoc/india-states)

    Contains ISO-3 codes and names of Indian States.

## Tableau Dashboard 
![GLBAL_GIF](https://user-images.githubusercontent.com/91481367/173543706-e313e8ed-27d7-4586-9989-3f33630e9a48.gif)

![India gif](https://user-images.githubusercontent.com/91481367/173548616-e16ec8b7-9cb9-4e13-9c98-005c99466a18.gif)

## Setup
### 1. Prerequisite

1. Docker with at least 4GB of RAM and Docker Compose v1.27.0 or later
2. [AWS account](https://aws.amazon.com/)
3. [AWS CLI installed and configured](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
4. [Tableau Desktop](https://www.tableau.com/products/desktop)

### 2. Clone the repository

Clone and cd into the project directory.

```
git clone <https://github.com/siddharth271101/Covid-19-and-Aviation-Industry.git>
cd beginner_de_project
```

Note: Replace {your-bucket-name} in `setup.sh`, `covid_flights_etl.py` and `covid_flights_dag.py` before proceeding with the steps mentioned below.

Download the data and create an s3 bucket by running `setup.sh` as shown below

```
sh setup.h
```

`setup.sh` also starts to incrementally load the opensky data to the S3 bucket.

After `setup.sh` runs successfully, start the docker container using the following command

### 3. Docker and Airflow

```
docker compose -f docker-compose-LocalExecutor.yml up -d
```

We use the following docker containers -

1. Airflow
2. Postgres DB (as Airflow metadata DB)

Open the Airflow UI by hitting [http://localhost:8080](http://localhost:8080/) in browser, start the covid_flights_dag DAG.

### 4. AWS

Once the dag-run is successful, check the output folder of the S3 bucket.
This [blog](https://aws.amazon.com/blogs/big-data/building-aws-data-lake-visualizations-with-amazon-athena-and-tableau/) explains the steps in detail to build a Tableau dashboard using Athena as a data source.

