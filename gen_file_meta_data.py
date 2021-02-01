# coding: utf-8
import pyspark.sql.functions as F
from pyspark.sql.functions import lower, col, lit, to_date, current_date, udf, greatest, array_sort, size, when
import logging
import time
from pyspark.sql import SparkSession, window
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import os
import time
from datetime import datetime, date
from itertools import chain
from pyspark.sql.functions import create_map, lit
import sys
import json
import hashlib

# os.environ['HADOOP_HOME'] = "C:\\hadoop"
# print (os.environ['HADOOP_HOME'])

start_time = time.time()
debug = "no"
try:
    spark = SparkSession.builder \
        .appName("gen_file_meta_data") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .getOrCreate()
except Exception as e:
    print("Exception in Getting Spark Session for gen_file_meta_data  Error is -- {}".format(e))
    raise Exception

try:
    spark.sparkContext.setLogLevel("ERROR")
    # create spark context
    sc = spark.sparkContext

    print("spark timezone is :", spark.conf.get("spark.sql.session.timeZone"))

    # get the spark session variable and set the variables
    spark_sql_shuffle_partitions = 4
    spark_sql_shuffle_partitions1 = spark.conf.get("spark.sql.shuffle.partitions")
    print('get spark_sql_shuffle_partitions is :', spark_sql_shuffle_partitions1)
    spark.conf.set("spark.sql.shuffle.partitions", spark_sql_shuffle_partitions)
    spark_sql_shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    print('set spark_sql_shuffle_partitions is :', spark_sql_shuffle_partitions)

    insert_dt_df = spark.sql("select date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS') as INSERT_DT").head(1)
    start_job_dt = insert_dt_df[0]['INSERT_DT']
    print("start_job_dt is  :", start_job_dt)
    print("start_job_dt type is  :", type(start_job_dt))
    today_date_str = start_job_dt.split(" ")[0]
    print("today_date is  :", today_date_str)

    base_path = "/app/"
    test_df_path = base_path + "test_df"
    download_file_dir_name = "downloaded_input_data_files"
    downloaded_file_dir_full_path = base_path + download_file_dir_name + "/"
    wholetextfiles_path = downloaded_file_dir_full_path + "*"

    print("base_path to save parquet files is : {}".format(base_path))
    print("downloaded_file_dir_full_path is : {}".format(downloaded_file_dir_full_path))
    print("wholetextfiles_path is : {}".format(wholetextfiles_path))

    #lines11 = lines11.withColumn("today_date", F.lit(today_date_str).cast(DateType()))


    df = spark.createDataFrame([
        (1, 144.5, 5.9, 33, 'M'),
        (2, 167.2, 5.4, 45, 'M'),
        (3, 124.1, 5.2, 23, 'F'),
        (4, 144.5, 5.9, 33, 'M'),
        (5, 133.2, 5.7, 54, 'F'),
        (3, 124.1, 5.2, 23, 'F'),
        (5, 129.2, 5.3, 42, 'M'),
    ], ['id', 'weight', 'height', 'age', 'gender'])

    if debug == "yes":
        df.show()
        print(spark.version)
        print('Count of Rows: {0}'.format(df.count()))
        print('Count of distinct Rows: {0}'.format((df.distinct().count())))

    df.repartition(4) \
        .write \
        .format("parquet") \
        .mode("Overwrite") \
        .save(test_df_path + "df_test")

    if debug == "yes":
        df2 = spark.read.parquet(test_df_path + "df_test")
        df2.show()

    file_data = sc.wholeTextFiles(wholetextfiles_path)
    # File size
    file_data_file_size = file_data.map(lambda x: (x, len(x[1])))
    file_data_file_size_t1 = file_data_file_size.map(lambda x: (x[0][0], x[1]))


    def sha_256(string):
        return hashlib.sha256(str(string).encode('utf-8')).hexdigest()


    def split_lines2(list1):
        list2 = []
        for i in range(len(list1)):
            list2.append(list1[i].split(" "))
        return list2

    try:
        file_data = sc.wholeTextFiles(wholetextfiles_path)
        # Getting a file name from full path
        file_data = file_data.map(lambda x: (x[0].split("/")[-1], x[1]))
        # File size
        file_data_file_size = file_data.map(lambda x: (x, len(x[1])))
        file_data_file_size_t1 = file_data_file_size.map(lambda x: (x[0][0], x[1]))
        file_data_file_sha256 = file_data.map(lambda x: (x, sha_256(x[1])))
        file_data_file_sha256_t1 = file_data_file_sha256.map(lambda x: (x[0][0], x[1]))
        file_data_file_sha256_t1.collect()
        file_data_file_sha256_t1_df = file_data_file_sha256_t1.toDF()

        hashschema = StructType([
            StructField('file_name', StringType(), True),
            StructField('sha256_hexdigest', StringType(), True)
        ])

        hashes_df1 = spark.createDataFrame(data=file_data_file_sha256_t1, schema=hashschema)

        if debug == "yes":
            hashes_df1.show(truncate=False)
            hashes_df1.printSchema()
        hashes_df2 = hashes_df1.withColumn('file_number',
                                           F.regexp_extract(F.col('file_name'), '(\\d+)', 1).cast(IntegerType()))
        hashes_df2 = hashes_df2.withColumn("today_date", F.lit(today_date_str).cast(DateType()))
        if debug == "yes":
            hashes_df2.printSchema()
            hashes_df2.show(truncate=False)
            hashes_df2.orderBy(["file_number"], ascending=[1]).show(truncate=False)
        hashes_df3 = hashes_df2.orderBy(["file_number"], ascending=[1])
        hashes_df3.select("sha256_hexdigest").repartition(1) \
            .write \
            .format("parquet") \
            .mode("Overwrite") \
            .save(base_path + "hashes.pq")

        hashes_df = spark.read.parquet(base_path + "hashes.pq")
        hashes_df.printSchema()
        hashes_df.show(truncate=False)

        if debug == "yes":
            file_data_file_sha256_t1_df.show(20, truncate=False)
        file_data_lines = file_data.mapValues(lambda x: x.split('\n'))
        file_data_lines_t1 = file_data_lines.mapValues(lambda x: split_lines2(x))
        file_data_lines_t2 = file_data_lines_t1.flatMapValues(lambda x: x)
        file_data_word = file_data_lines_t2.flatMapValues(lambda x: x)
        file_data_word_t0 = file_data_word.filter(lambda x: len(x[1]) > 0)
        file_data_word_t1 = file_data_word_t0.map(lambda x: (x, 1))
        file_data_word_t2 = file_data_word_t1.reduceByKey(lambda a, b: a + b)
        file_data_word_t2.collect()
        file_data_word_t3 = file_data_word_t2.map(lambda x: (x[0][0], x[1])).reduceByKey(lambda a, b: a + b)
        file_data_word_t3.collect()
        file_data_word_t3_df = file_data_word_t3.toDF()
        if debug == "yes":
            file_data_word_t3_df.show(20, truncate=False)
        file_data_word_t2_unique = file_data_word_t2.map(lambda x: (x[0][0], x[0][1]))
        file_data_word_t2_unique.collect()
        file_data_word_t2_unique_t1 = file_data_word_t2_unique.map(lambda x: (x, 1))
        file_data_word_t2_unique_t1.collect()
        file_data_word_t2_unique_t2 = file_data_word_t2_unique_t1.map(lambda x: (x[0][0], x[1])).reduceByKey(
            lambda a, b: a + b)
        file_data_word_t2_unique_t2.collect()
        file_data_word_t2_unique_t2_df = file_data_word_t2_unique_t2.toDF()

        if debug == "yes":
            file_data_word_t2_unique_t2_df.printSchema()
            file_data_word_t2_unique_t2_df.show(20, truncate=False)
        stg1 = file_data_file_size_t1.leftOuterJoin(file_data_word_t3).leftOuterJoin(file_data_word_t2_unique_t2)

        rdd1 = stg1.map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][1]))

        dataschema = StructType([
            StructField('file_name', StringType(), True),
            StructField('file_size', IntegerType(), True),
            StructField('word_count', IntegerType(), True),
            StructField('unique_word', IntegerType(), True),
        ])

        data_df1 = spark.createDataFrame(data=rdd1, schema=dataschema)
        if debug == "yes":
            data_df1.printSchema()
            data_df1.show(truncate=False)

        data_df2 = data_df1.join(hashes_df2, \
                (data_df1.file_name == hashes_df2.file_name), how="left_outer") \
                .select([data_df1["*"], \
                hashes_df2.today_date, \
                hashes_df2.file_number])

        data_df3 = data_df2.orderBy(["file_number"], ascending=[1])
        data_df3.select("file_name", "file_size", "word_count", "unique_word", "today_date").repartition(1) \
            .write \
            .format("parquet") \
            .mode("Overwrite") \
            .save(base_path + "data.pq")

        data_pq = spark.read.parquet(base_path + "data.pq")
        data_pq.printSchema()
        data_pq.show(truncate=False)

    except Exception as e:
        print("Exception in gen_file_meta_data main Job  Error is -- {}".format(e))
        raise Exception

except Exception as e:
    print("Exception in gen_file_meta_data main Job  Error is -- {}".format(e))
    raise Exception
print("Total time taken gen_file_meta_data is - {} minutes".format((time.time() - start_time) / 60))
