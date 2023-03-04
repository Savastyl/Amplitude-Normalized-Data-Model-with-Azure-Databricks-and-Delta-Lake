# Amplitude Normalized Data Model for September 2021

This technical test requires the creation of an Amplitude Normalized data model for the month of September 2021 using Spark, preferably PySpark. The main transformation involves selecting a subset of columns, renaming them and casting their data types based on a given mapping.

## Transformation
The following mapping is used to select, rename and cast the columns:
![Alt text](Media/Screenshot%202023-03-03%20212945.png)

Implementation
The implementation is done using the following steps:

Load the raw data for September 2021 from a Parquet file using Spark.
Select the required columns from the raw data.
Cast the data types of the selected columns based on the mapping.
Drop the original event_properties and user_properties columns.
Create a Delta table with the result of the transformation.

## Code
The following PySpark code can be used to perform the transformation and create the Delta table:

```
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load the raw data for September 2021 from a Parquet file
raw_data = spark.read.format("parquet").load("abfss://raw@savas.dfs.core.windows.net/amplitude/2021/9/*/*/*.parquet")

# Select the required columns from the raw data
df = raw_data.select ("device_id","event_time","event_type","session_id","event_properties","user_properties")

# Cast the data types of the selected columns based on the mapping
df1 = df.withColumn("device_id",df["device_id"].cast(StringType())) \
         .withColumn("event_time",df["event_time"].cast(TimestampType())) \
         .withColumn("event_type",df["event_type"].cast(StringType())) \
         .withColumn("session_id",df["session_id"].cast(LongType())) \
         .withColumn("app_pillar",col("event_properties.`app.pillar`").cast(StringType())) \
...

```



