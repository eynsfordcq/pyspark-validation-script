# pyspark-validation-script

## Overview

This script is to validate data between source and target datasets using Apache Spark. It ensures that row counts are within specified thresholds and can optionally validate the content of the datasets.

## Installation

1. **Clone the Repository:**

2. **(Optional) Create a Virtual Environment:**

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. **Create Configuration File:**

   Use the provided example configuration as a starting point.

   ```bash
   cp validation_config.xml.example validation_config.xml
   ```

2. **Edit `validation_config.xml`:**

   Most of the settings are straightforward, refer to the model `helpers.models.validation_config.py` for details on what is allowed.

## Usage

The arguments are as follows:

```bash
usage: spark_validation.py [-h] --config CONFIG [--datetime DATETIME] [--verbose] [--spark-master {local,yarn}]

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG, -c CONFIG
                        config file path
  --datetime DATETIME, -d DATETIME
                        datetime in YYYY-MM-DD HH:MM:SS
  --verbose, -v         enable debug log
  --spark-master {local,yarn}, -m {local,yarn}
                        set spark master
```

The script can be run in two modes: cluster and client:

```bash
# client 
/path/to/python /spark_validation.py [arguments]

# cluster
rm -f helpers.zip \
&& zip -r helpers.zip helpers/ \
    spark-submit \
    --master=yarn \
    --deploy-mode=cluster \
    --py-files helpers.zip \
    spark_validation.py -m yarn [arguments]
```

## Development

Start the environment in devcontainer. In vscode, in command palette, `Dev Containers: Reopen in Container`

Once in the container, simply do `pip install -r requirements.txt`.

> TODO: include minio as s3 compatible storage in docker compose.
