This project should be run with Python (We used version 3.7).

# Basic configurations

## Software Versions

```
JAVA: 1.8.0_201
SCALA: 2.13.4
apache-spark: stable 3.0.1
```


## Setup your environment

1. Prepare a python virtual environment (optional)

```
python3 -m venv .venv
source .venv/bin/activate
```

2. Install the Python dependencies 

```
pip install -r requirements.txt
```


3. Update ~/.bash_profile environment variables if needed to configure Spark (**Values could be different in your environment!**)

```
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.0.1/libexec
export PATH=/usr/local/Cellar/apache-spark/3.0.1/bin:$PATH
export PYSPARK_PYTHON=python3
```

4. Download the data on Kaggle (https://www.kaggle.com/c/ieee-fraud-detection/data#).


5. Create a file config.ini in the config folder (**see config/template.config.ini for the structure**)
    - DATA_PATH: path of the folder where the input data will be
    - OUTPUT_DATA_PATH: path of the folder where the output data will be
    - IDENTITY_DATA: name of identity data (default is test_identity.csv)
    - TRANSACTION_DATA: name of transaction data (default is test_transaction.csv)  




# Start the Spark Job

1. Run the following command to prepare the spark job. It will create a dist directory in which there will be all needed files and data (actually, just one zip file for now...)

```
make
```

2. Start the Pyspark job (**feel free to update with your own parameters**)

```
spark-submit --name "Pyspark Starter" --master "local[*]" --driver-memory 12g --conf spark.executor.memory=12g --conf spark.driver.maxResultSize=20g --py-files ./dist/jobs.zip main.py
```

3. To give a specific date, add `--etl-date YYYY-MM-DD`parameter at the end of the command.

# Dev and testing

To run unit tests:
```
pip install -r requirements-dev.txt
python -m pytest
```