# Airflow Binance Example
This repository exemplifies how to use an airflow setup to collect 
crypto-currency data every X minutes. In the example the data is stored in a 
postgres database. 

To use the setup, do the following:
- Create an account at binance.com
- Get your api authentication details (explained there)
- Paste your details in authentication_details/api_authentication_details.json
- Install docker 
- Run the docker container with "docker-compose up"
- Activate the DAG to start collecting data


For further configurations you can also take a look at:
- docker-compose.yaml
- dags/collect_asset_data.py, especially the variables 
  - CRYPTO_CURRENCIES_TO_COLLECT
  - UPDATE_INTERVAL
  - START_DATE  

Passwords for the database is as follows
- address: "postgres"
- port: "5432"
- user: "airflow", 
- password: "airflow"
- db_name: "airflow"

For the airflow instance the login details are:
user: airflow
password: airflow


ENJOY!

