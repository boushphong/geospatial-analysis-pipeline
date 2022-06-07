## Geospatial Analysis Pipeline Using NYC Taxi Dataset
- This repo showcases the power of [Great Expectations](https://greatexpectations.io) for Data Quality and [Apache Sedona](https://sedona.apache.org/) for Geospatial Analysis on Big Data.
- The goal is to run data quality check on the public NYC Taxi Dataset, if all checks passes, we could run a Spark Job for ETL processing.

After cloning the repo please run this command to download the necessary data for the pipeline

`wget -P data/ https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-12.csv`

Run Airflow image (with Spark pre-installed) (please use Docker Compose V2)

`docker compose up`

Go to localhost:8080 to explore and trigger the pipeline

![image](https://user-images.githubusercontent.com/59940078/172496772-5ed8ec95-8ac2-4966-8913-5897855a7dcd.png)

## Validation

**Validation Task** will automatically generate documentation about the data against your **expectations suite** . **Expectation suite json file** is stored in (_great_expectations/expectations/my_expectation.json_). Airflow's [GreatExpectationsOperator](https://registry.astronomer.io/providers/great-expectations/modules/greatexpectationsoperator) 
will do the heavy-lifting of orchestrating data quality check and generating expectations docs.
Please consult the official [docs](https://greatexpectations.io/expectations) if you want to add more expectations.

Once done, you can find your data profiling docs at (_great_expectations/uncommitted/data_docs/local_site/index.html_). In a real production environment, you would upload this file into an Object Storage (s3, gcs ..)

![image](https://user-images.githubusercontent.com/59940078/172497883-11d752d0-e6a3-4b57-9d10-45a23eeb96b3.png)

## Spark Job

**Spark Job Task** will transform the data (include some data cleaning). Add a column (BooleanType) to the dataset that indicates whether the trip would go to JFK Airport or not. Finally, output data would be in Parquet files (PARTITION by Day)

To avoid python UDFs which slow down Spark jobs. We utilize native Spark's geospatial library [Apache Sedona](https://sedona.apache.org/) to register native Spark-SQL functions into our Spark Session.

How to know where is the JFK airport? Fetch JFK airport object [here](https://data.cityofnewyork.us/City-Government/Airport-Polygon/xfhz-rhsk). 

**NOTE:** **Buffer distance** and **Spark Runtime Configs** could be changed in (_spark-submit-jobs/etl.py_)

![image](https://user-images.githubusercontent.com/59940078/172499156-d03ea454-b2f6-455b-8783-ad30532db344.png)


