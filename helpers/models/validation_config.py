from datetime import datetime, timedelta
from typing import Optional, Union, Literal
from pydantic import BaseModel 
from pydantic import model_validator, Field


class Args(BaseModel):
    config: str
    datetime: Optional[str]
    verbose: bool
    spark_master: Literal["yarn", "local"]


class Setting(BaseModel):
    validation_name: str
    validation_threshold: int = Field(le=100)
    validate_content: bool = False
    defaulttimedelay: int = 0
    source_name: str = None
    destination_name: str = None
    summary_log: str
    granularity: Literal[ 
        'monthly',
        'daily',
        'hourly'
    ] = None
    validation_type: Literal[
        "hdfs-hdfs",
        "hdfs-jdbc",
        "jdbc-hdfs",
        "jdbc-jdbc"
    ] = None


class CsvOptions(BaseModel):
    """
    https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html
    """
    sep: Optional[str] = None
    encoding: Optional[str] = None
    quote: Optional[str] = None
    escape: Optional[str] = None
    comment: Optional[str] = None
    header: Optional[bool] = None
    inferSchema: Optional[bool] = None
    preferDate: Optional[bool] = None 
    enforceSchema: Optional[bool] = None 
    ignoreLeadingWhiteSpace: Optional[bool] = None
    ignoreTrailingWhiteSpace: Optional[bool] = None
    nullValue: Optional[str] = None 
    nanValue: Optional[str] = None 
    positiveInf: Optional[str] = None 
    negativeInf: Optional[str] = None 
    dateFormat: Optional[str] = None 
    timestampFormat: Optional[str] = None 
    timestampNTZFormat: Optional[str] = None 
    enableDateTimeParsingFallback: Optional[bool] = None 
    maxColumns: Optional[int] = None 
    maxCharsPerColumn: Optional[int] = None 
    mode: Optional[
        Literal[
            'PERMISSIVE',
            'DROPMALFORMED',
            'FAILFAST'
        ]
    ] = None 
    columnNameOfCorruptRecord: Optional[str] = None 
    multiLine: Optional[bool] = None
    charToEscapeQuoteEscaping: Optional[str] = None 
    samplingRatio: Optional[float] = None
    emptyValue: Optional[str] = None 
    locale: Optional[str] = None 
    lineSep: Optional[str] = None 
    unescapedQuoteHandling: Optional[
        Literal[
            'STOP_AT_CLOSING_QUOTE',
            'BACK_TO_DELIMITER',
            'STOP_AT_DELIMITER',
            'SKIP_VALUE',
            'RAISE_ERROR'
        ]
    ] = None


class ParquetOptions(BaseModel):
    """
    https://spark.apache.org/docs/3.5.4/sql-data-sources-parquet.html
    """
    datetimeRebaseMode: Optional[
        Literal[
            'EXCEPTION',
            'CORRECTED',
            'LEGACY'
        ]
    ] = None
    int96RebaseMode: Optional[
        Literal[
            'EXCEPTION',
            'CORRECTED',
            'LEGACY'
        ]
    ] = None
    mergeSchema: Optional[bool] = None 
    compression: Optional[str] = None


class FileConfig(BaseModel):
    format: Literal["csv", "parquet"]
    path: str
    options: Optional[Union[CsvOptions, ParquetOptions]] = None

    @model_validator(mode='before')
    def set_file_options(cls, values):
        _format = values.get('format')
        _options = values.get('options')
        
        if _format == "csv":
            values['options'] = CsvOptions(**_options) if _options else CsvOptions()
        elif _format == "parquet":
            values['options'] = ParquetOptions(**_options) if _options else ParquetOptions()
        return values


class DatabaseOptions(BaseModel):
    """
    https://spark.apache.org/docs/3.5.4/sql-data-sources-jdbc.html
    """
    url: str
    user: Optional[str] = None
    password: Optional[str] = None
    dbtable: Optional[str] = None
    query: Optional[str] = None
    prepareQuery: Optional[str] = None 
    driver: Optional[str] = None
    partitionColumn: Optional[str] = None
    lowerBound: Optional[int] = None
    upperBound: Optional[int] = None
    numPartitions: Optional[int] = None
    queryTimeout: Optional[int] = None
    fetchsize: Optional[int] = None
    sessionInitStatement: Optional[str] = None
    customSchema: Optional[str] = None
    pushDownPredicate: Optional[bool] = None
    pushDownAggregate: Optional[bool] = None
    pushDownLimit: Optional[bool] = None
    pushDownLimit: Optional[bool] = None
    pushDownTableSample: Optional[bool] = None
    keytab: Optional[str] = None
    principal: Optional[str] = None
    refreshKrb5Config: Optional[bool] = None
    connectionProvider: Optional[str] = None
    preferTimestampNTZ: Optional[bool] = None

    @model_validator(mode='before')
    def check_dbtable_or_query(cls, values):
        dbtable = values.get('dbtable')
        query = values.get('query')
        if not (dbtable or query):
            raise ValueError("Either 'dbtable' or 'query' must be provided.")
        if dbtable and query:
            raise ValueError("Only one of 'dbtable' or 'query' should be provided.")
        return values


class DatabaseConfig(BaseModel):
    format: Literal["jdbc"]
    options: DatabaseOptions


class Datasource(BaseModel):
    type: Literal["database", "file"]
    database_config: Optional[DatabaseConfig] = None
    file_config: Optional[FileConfig] = None

    @model_validator(mode='before')
    def check_target_config(cls, values):
        ds_type = values.get('type')
        
        if ds_type == "database" and not values.get('database_config'):
            raise ValueError("database_config must be provided for type 'database'.")
        
        if ds_type == "file" and not values.get('file_config'):
            raise ValueError("file_config must be provided for type 'file'.")
        
        return values


class ValidationConfig(BaseModel):
    args: Args
    setting: Setting
    source: Datasource
    target: Datasource
    process_datetime: Optional[datetime] = None

    @model_validator(mode="after")
    def set_process_datetime(cls, values):
        _datetime = values.args.datetime
        _default_timedelay = values.setting.defaulttimedelay
        _granularity = values.setting.granularity

        if _datetime:
            # not wrapping in exception cause it throw same value error 
            _process_datetime = datetime.strptime(_datetime, "%Y-%m-%d %H:%M:%S")
        else:
            _process_datetime = datetime.now() - timedelta(seconds=_default_timedelay)

        # Truncate _process_datetime based on _granularity
        if _granularity == "monthly":
            _process_datetime = _process_datetime.replace(
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )
        elif _granularity == "daily":
            _process_datetime = _process_datetime.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )
        else:  # hourly
            _process_datetime = _process_datetime.replace(
                minute=0,
                second=0,
                microsecond=0
            )

        values.process_datetime = _process_datetime
        return values
    
    @model_validator(mode="after")
    def set_strftime_string(cls, values):
        _process_datetime: datetime = values.process_datetime
        try:
            values.setting.summary_log = _process_datetime.strftime(values.setting.summary_log)
            for config in [values.source.database_config, values.target.database_config]:
                if config and config.options.query:
                    config.options.query = _process_datetime.strftime(config.options.query)
            for config in [values.source.file_config, values.target.file_config]:
                if config and config.path:
                    config.path = _process_datetime.strftime(config.path)
            return values
        except Exception as e:
            raise ValueError(f"error setting strftime. Error {e}")
