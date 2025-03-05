import logging
import argparse
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame, Row

from helpers.generic import timer, parse_config
from helpers.models import validation_config

def setup_logger(log_level = logging.INFO) -> None:
    logging.basicConfig(
        level=log_level,
        format=f'[%(levelname)s] %(asctime)-15s - %(name)s - %(filename)s/%(funcName)s/%(lineno)d - %(message)s'
    )

def spark_get_session(app_name: str, master: str) -> SparkSession:
    _master = "local[*]" if master == "local" else master
    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master(_master) \
        .getOrCreate()
    return spark

def spark_read_file(spark: SparkSession, file_config: validation_config.FileConfig) -> DataFrame:
    _format = file_config.format
    _options = file_config.options.model_dump(exclude_none=True)
    _path = file_config.path

    df = spark.read \
        .format(_format) \
        .options(**_options) \
        .load(_path)
    
    return df

def spark_read_db(spark: SparkSession, database_config: validation_config.DatabaseConfig) -> DataFrame:
    _format = database_config.format
    _options = database_config.options.model_dump(exclude_none=True)

    df = spark.read \
        .format(_format) \
        .options(**_options) \
        .load()

    return df

def spark_write_hdfs(spark: SparkSession, config: validation_config.Setting, process_datetime: datetime, data: Dict[str, Any]) -> None:
    # Create a Row object with the required schema
    row = Row(
        timestamp=datetime.now(),
        datetime=process_datetime,
        granularity=config.granularity,
        type=config.validation_type,
        source_name=config.source_name,
        destination_name=config.destination_name,
        source_count=data.get("count_source"),
        destination_count=data.get("count_target")
    )

    df = spark.createDataFrame([row])
    df.write \
        .format("csv") \
        .mode("append") \
        .save(config.summary_log)
    
    logging.info(f"results written into hdfs: {config.summary_log}")

def read_dataset(spark: SparkSession, dataset_config: validation_config.Datasource) -> DataFrame:
    if dataset_config.type == "file":
        df = spark_read_file(spark, dataset_config.file_config)
    elif dataset_config.type == "database":
        df = spark_read_db(spark, dataset_config.database_config)
    return df

def validate_row_count(df_source: DataFrame, df_target: DataFrame, threshold: int) -> Dict[str, Any]:
    count_source = df_source.count()
    count_target = df_target.count()
    is_passed = False

    count_diff = abs(count_source - count_target)
    percentage_diff = count_diff / count_source * 100
    is_passed = True if percentage_diff <= threshold else False 

    logging.info(
        f"row count validation results: \n"
        f"source count:         {count_source} \n"
        f"target count:         {count_target} \n"
        f"count diff:           {count_diff} \n"
        f"percentage diff:      {percentage_diff:.4f} \n"
        f"threshold:            {threshold} \n"
        f"validation passed:    {is_passed} \n"
    )
    
    return {
        "count_source": count_source,
        "count_target": count_target,
        "count_diff": count_diff,
        "percentage_diff": percentage_diff,
        "threshold": threshold,
        "is_passed": is_passed,
    }

def validate_content(df_source: DataFrame, df_target: DataFrame) -> None:
    # Ensure schemas match
    if df_source.columns != df_target.columns:
        logging.info(
            f"content validation results: \n"
            f"schema match:         false \n"
            f"source not in target: N/A \n"
            f"target not in source: N/A \n"
            f"validation passed:    False \n"
        )
        logging.info(df_source.schema)
        logging.info(df_target.schema)
        return

    # Create a unique identifier by concatenating all columns
    source_hash = df_source.selectExpr("concat_ws('||', *) as row").distinct()
    target_hash = df_target.selectExpr("concat_ws('||', *) as row").distinct()

    # Find differences
    diff_source = source_hash.subtract(target_hash)
    diff_target = target_hash.subtract(source_hash)

    diff_source_count = diff_source.count()
    diff_target_count = diff_target.count()
    is_passed = (diff_source_count == 0) and (diff_target_count == 0)
    
    logging.info(
        f"content validation results: \n"
        f"schema match:         true \n"
        f"source not in target: {diff_source_count} \n"
        f"target not in source: {diff_target_count} \n"
        f"validation passed:    {is_passed} \n"
    )

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="config file path", type=str, required=True)
    parser.add_argument("--datetime", "-d", help="datetime in YYYY-MM-DD HH:MM:SS", type=str, required=False)
    parser.add_argument("--verbose", "-v", help="enable debug log", action="store_true")
    parser.add_argument(
        "--spark-master",
        "-m",
        choices=["local", "yarn"],
        default="local",
        help="set spark master"
    )
    return parser.parse_args()

@timer
def main():
    # args and config
    args = parse_args()
    dict_config = parse_config(args.config)
    config = validation_config.ValidationConfig(
        **dict_config,
        args=vars(args)
    )

    # setup logger
    setup_logger()

    # initial logging
    logging.info(
        f"Start validation: {config.setting.validation_name}. "
        f"Type:             {config.setting.validation_type}. "
        f"Granularity:      {config.setting.granularity}. "
        f"Source:           {config.setting.source_name}. "
        f"Destination:      {config.setting.destination_name}. "
    )
    if config.args.verbose:
        logging.info(config.model_dump_json(indent=4, exclude_none=True))

    # spark session
    spark = spark_get_session(config.setting.validation_name, config.args.spark_master)
    logging.info(f"Spark session initialized, using master: {config.args.spark_master}")

    # validation
    # read dfs
    df_source = read_dataset(spark, config.source)
    df_target = read_dataset(spark, config.target)
    logging.info("datasources fetched")

    # validate row count
    res = validate_row_count(df_source, df_target, config.setting.validation_threshold)

    # write to report file
    spark_write_hdfs(spark, config.setting, config.process_datetime, res)

    # Validate content
    if config.setting.validate_content:
        validate_content(df_source, df_target)

if __name__ == "__main__":
    main()