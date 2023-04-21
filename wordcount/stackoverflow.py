import re
from locale import format_string

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Window, functions
from pyspark.sql.functions import col, to_date, last_day, lit, when, lower, concat, sum, min, unix_timestamp, \
    month, lpad, split, expr, udf, posexplode, regexp_replace, collect_set, lag, approx_count_distinct, coalesce, \
    row_number, explode, monotonically_increasing_id, first, from_json, aggregate, create_map, map_concat, to_json, \
    flatten, transform, collect_list, concat_ws, struct, substring, to_timestamp, regexp_extract, trim, hour, \
    current_timestamp, date_add, isnull, isnan, array_join, broadcast, arrays_zip
from pyspark.sql.types import StructType, ArrayType, MapType, StringType, StructField, IntegerType
import html


def q_1():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    data1 = [
        ["John", "1932-11-14"],
        ["Maike", "1932-10-14"]
    ]

    data2 = [
        ["Aries", "03-21", "04-19"],
        ["Taurus", "04-20", "05-20"],
        ["Gemini", "05-21", "06-20"],
        ["Cancer", "06-21", "07-22"],
        ["Leo star", "07-23", "08-22"],
        ["Virgo", "08-23", "09-22"],
        ["Libra", "09-23", "10-22"],
        ["Scorpio", "10-23", "11-21"],
        ["Sagittarius", "11-22", "12-21"],
        ["Capricorn", "12-22", "01-19"],
        ["Aquarius", "01-20", "02-18"],
        ["Pisces", "02-19", "03-20"],
    ]
    df = spark.createDataFrame(data1).toDF("name", "dob")
    zodiacSignDf = spark.createDataFrame(data2).toDF("sign", "start", "end")

    df.alias("df").join(zodiacSignDf.alias("zodiacSignDf"), to_date(col("df.dob").substr(6, 5), 'MM-dd').between(
        to_date(col("zodiacSignDf.start"), 'MM-dd'), to_date(col("zodiacSignDf.end"), 'MM-dd')
    ), "left").drop("start", "end").distinct().show()


def q_74849759():
    sc = SparkContext("local", "PySpark Word Count Exmaple")
    print("0:", type(sc))
    print("0:", sc)

    # read data from text file and split each line into words
    rdd = sc.textFile("C:\\Users\\marou\\Downloads\\twitter.txt")
    print("1:", type(rdd))
    print("2:", rdd)
    words = rdd.flatMap(lambda line: line.split(" "))
    print("3:", type(words))
    print("4:", words)

    # count the occurrence of each word
    wordmap = words.map(lambda word: (word, 1))
    print("5:", type(wordmap))
    print("6:", wordmap)
    wordCounts = wordmap.reduceByKey(lambda a, b: a + b)
    print("7:", type(wordmap))
    print("8:", wordmap)

    # save the counts to output
    wordCounts.saveAsTextFile("C:\\Users\\marou\\Downloads\\output.txt")
    wordCounts.toDF().explain()


def q_74864258():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data2 = [
        ["Aries", "03-21", "04-19"],
        ["Taurus", "04-20", "05-20"],
        ["Gemini", "05-21", "06-20"],
        ["Cancer", "06-21", "07-22"],
        ["Leo star", "07-23", "08-22"],
        ["Virgo", "08-23", "09-22"],
        ["Libra", "09-23", "10-22"],
        ["Scorpio", "10-23", "11-21"],
        ["Sagittarius", "11-22", "12-21"],
        ["Capricorn", "12-22", "01-19"],
        ["Aquarius", "01-20", "02-18"],
        ["Pisces", "02-19", "03-20"],
    ]
    df = spark.createDataFrame(data2).toDF("item_name", "price_for_2k", "qty_1")
    for column in df.columns:
        df = df.withColumnRenamed(column, column.replace("k", "000") if column.startswith("price_for_") else column)

    df.show()


def q_74865641():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [
        ["2022", "January"],
        ["2021", "December"],
    ]
    df = spark.createDataFrame(data).toDF("year", "month")
    result = df.withColumn("start_date",
                           concat(col("year"), lit("-"), lpad(month(to_date(col("month"), "MMMM")), 2, "0"),
                                  lit("-01"))) \
        .withColumn("end_of_month", last_day(col("start_date")))
    result.show()


def q_74874646():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [
        ["sumit", 30, "bangalore"],
        ["kapil", 32, "hyderabad"],
        ["sathish", 16, "chennai"],
        ["ravi", 39, "bangalore"],
        ["kavita", 12, "hyderabad"],
        ["kavya", 19, "mysore"],
    ]
    df = spark.createDataFrame(data).toDF("name", "age", "city")
    result = df.withColumn("result", when(df.age > 18, "Y").otherwise("N"))
    result.show()


def q_74859950():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [
        ["Hello is  $1620.00 per hello;"],
        ["Hello is recalculated to be 15% of item."],
        ["Hello is a case rate of $4,440.00 for up to 3 days etc"],
        [
            "For multiple services allow 100% of the first item of item, 50% of the second item of item 25% of the 3rd item code 25% of the 4th item"],
    ]

    df = spark.createDataFrame(data).toDF("text")
    result = df.withColumn("splitted_text", split(col("text"), " ")).withColumn("dollars", expr(
        "filter(splitted_text, x -> x like '$%')")) \
        .withColumn("percentage", expr("filter(splitted_text, x -> x like '%\\%')")).drop("splitted_text")
    result.show()


def q_74880468():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    data = [["100MG"], ["1EA"], ["100MG"]]
    df = spark.createDataFrame(data).toDF("size")

    def split_func(str):
        return re.sub("[A-Za-z]+", lambda ele: " " + ele[0] + " ", str)

    split_udf = udf(split_func)

    df.withColumn("splitted", split_udf(col("size"))).show()


def q_74892964():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    data = [
        ["2022-12-20", 30, "Mary"],
        ["2022-12-21", 12, "Mary"],
        ["2022-12-20", 12, "Bob"],
        ["2022-12-21", 15, "Bob"],
        ["2022-12-22", 15, "Alice"],
    ]
    df = spark.createDataFrame(data).toDF("Date", "Amount", "Customer")

    def init_amout_data(df):
        w = Window.orderBy(col("Date"))
        amount_sum_df = df.groupby("Date").agg(sum("Amount").alias("Amount")) \
            .withColumn("amout_sum", sum(col("Amount")).over(w)) \
            .withColumn("prev_amout_sum", lag("amout_sum", 1, 0).over(w)).select("Date", "amout_sum", "prev_amout_sum")
        amount_sum_df.write.mode("overwrite").partitionBy("Date").parquet("./path/amount_data_df")
        amount_sum_df.show(truncate=False)

    # keep only customer data to avoid unecessary data when querying, partitioning by Date will make query faster due to spark filter push down mechanism
    def init_customers_data(df):
        df.select("Date", "Customer").write.mode("overwrite").partitionBy("Date").parquet("./path/customers_data_df")

    def getMaxDate(path):
        return None

    # each day update the amount data dataframe (example at midnight), with only yesterday data: by talking the last amout_sum and adding to it the amount of the last day
    def update_amount_data(last_partition):
        amountDataDf = spark.read.parquet("./path/amount_data_df")
        maxDate = getMaxDate("./path/amount_data_df")  # implement a hadoop method to get the last partition date
        lastMaxPartition = amountDataDf.filter(col("date") == maxDate)
        lastPartitionAmountSum = lastMaxPartition.select("amout_sum").first.getLong(0)
        yesterday_amount_sum = last_partition.groupby("Date").agg(sum("Amount").alias("amount_sum"))
        newPartition = yesterday_amount_sum.withColumn("amount_sum", col("amount_sum") + lastPartitionAmountSum) \
            .withColumn("prev_amout_sum", lit(lastPartitionAmountSum))
        newPartition.write.mode("append").partitionBy("Date").parquet("./path/amount_data_df")

    def update_cusomers_data(last_partition):
        last_partition.write.mode("append").partitionBy("Date").parquet("./path/customers_data_df")

    def query_amount_date(beginDate, endDate):
        amountDataDf = spark.read.parquet("./path/amount_data_df")
        endDateAmount = amountDataDf.filter(col("Date") == endDate).select("amout_sum").first.getLong(0)
        beginDateDf = amountDataDf.filter(col("Date") == beginDate).select("prev_amout_sum").first.getLong(0)
        diff_amount = endDateAmount - beginDateDf
        return diff_amount

    def query_customers_date(beginDate, endDate):
        customersDataDf = spark.read.parquet("./path/customers_data_df")
        distinct_customers_nb = customersDataDf.filter(col("date").between(lit(beginDate), lit(endDate))) \
            .agg(approx_count_distinct(df.Customer).alias('distinct_customers')).first.getLong(0)
        return distinct_customers_nb

    # This is should be executed the first time only
    init_amout_data(df)
    init_customers_data(df)
    yesterday_date = "2022-11-12"
    # This is should be executed everyday at midnight with data of the last day only
    last_day_partition = df.filter(col("date") == yesterday_date)
    update_amount_data(last_day_partition)
    update_cusomers_data(last_day_partition)
    # Optimized queries that should be executed with
    beginDate = "2022-12-20"
    endDate = "2022-12-22"
    answer = query_amount_date(beginDate, endDate) / query_customers_date(beginDate, endDate)
    print(answer)


def q_74908984():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    data1 = [
        ["a", 25, "ast"],
        ["b", None, "phone"],
        ["c", 32, "dlp"],
        ["d", 45, None],
        ["e", 60, "phq"],
    ]
    df1 = spark.createDataFrame(data1).toDF("column1", "column2", "column3")
    data2 = [
        ["a", 25, "ast"],
        ["b", None, "phone"],
        ["c", 32, "dlp"],
        ["d", 45, None],
        ["e", 60, "phq"],
    ]
    df2 = spark.createDataFrame(data2).toDF("column1", "column2", "column3")

    df1 = df1.withColumn("join_column", concat(col("column1"), lit("-"), col("column2"), lit("-"), col("column3")))
    df2 = df2.withColumn("join_column_2", concat(col("column1"), lit("-"), col("column2"), lit("-"), col("column3"))) \
        .withColumnRenamed("column1", "column1_2").withColumnRenamed("column2", "column2_2").withColumnRenamed(
        "column3", "column3_2")

    df1.show()
    df2.show()
    df1.join(broadcast(df2), col("join_column") == col("join_column_2"), "left") \
        .withColumn("column2", when(col("column2") == col("column2_2"), None).otherwise(
        coalesce(col("column2"), col("column2_2")))) \
        .withColumn("column3",
                    when(col("column3") == col("column3_2"), None).otherwise(coalesce("column3", "column3_2"))) \
        .drop("join_column", "join_column_2", "column1_2", "column2_2", "column3_2").show()


def q_74965630():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.read.option("header", "true").csv("./ressources/1.csv", sep='┐')
    df.repartition(80, "ID1", "ID2", "ID3").show()


def q_75060820():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.createDataFrame([(1, 4, 3), (2, 4, 2), (3, 4, 5), (1, 5, 3), (2, 5, 2), (3, 6, 5)], ['a', 'b', 'c'])
    w = Window.partitionBy(col("b")).orderBy(col("b"))
    df.withColumn("d", row_number().over(w)).filter(col("d") <= 2).show()


def q_75061097():
    # prepare data
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.read.option("multiline", "true").json("./ressources/75061097.json")
    df.printSchema()
    df.select("context.custom.dimensions").show(truncate=False)
    df.select("context.custom.dimensions").printSchema()
    # Processing
    result = df.withColumn("id", monotonically_increasing_id()) \
        .select("id", explode(col("context.custom.dimensions"))).select("id", "col.*") \
        .groupby("id").agg(first(col('Activity ID'), ignorenulls=True).alias("Activity ID"),
                           first(col("Activity Type"), ignorenulls=True).alias("Activity Type"),
                           first(col("Bot ID"), ignorenulls=True).alias("Bot ID"),
                           first(col("Channel ID"), ignorenulls=True).alias("Channel ID"),
                           first(col("Conversation ID"), ignorenulls=True).alias("Conversation ID"),
                           first(col("Correlation ID"), ignorenulls=True).alias("Correlation ID"),
                           first(col("From ID"), ignorenulls=True).alias("From ID"),
                           first(col("Recipient ID"), ignorenulls=True).alias("Recipient ID"),
                           first(col("StatusCode"), ignorenulls=True).alias("StatusCode"),
                           first(col("Timestamp"), ignorenulls=True).alias("Timestamp"),
                           ).drop("id")
    result.show(truncate=False)


def q_75061097_2():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.read.option("multiline", "true").json("./ressources/75061097.json")

    # Save dimesions's object schema for later use
    dim_ele_schema = StructType.fromJson(
        df.select('context.custom.dimensions').schema[0].jsonValue()['type']['elementType']
    )

    # Extract dimensions and convert it to MapType to aggregate
    df = (df.select('context.custom.dimensions')
          # Step 1
          .withColumn('dim_map', from_json(to_json('dimensions'), ArrayType(MapType(StringType(), StringType()))))
          # Step 2
          .select(aggregate('dim_map',
                            create_map().cast("map<string,string>"),
                            lambda acc, x: map_concat(acc, x))
                  .alias('dim_map')))

    # Step 3
    df = (df.withColumn("dim", from_json(to_json("dim_map"), dim_ele_schema))
          .select("dim.*"))
    df.show(truncate=False)
    df.explain()


def q_75154979():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    test_df = spark.createDataFrame([
        (1, 1, 1, 0, 1, 1)
    ], ("b1", "b2", "b3", "b4", "b5", "b6"))
    cols = test_df.columns
    test_df = test_df.withColumn('Ind', concat(*cols))

    maxCon_udf = udf(lambda x: max(map(len, x.split('0'))))
    test_df.withColumn('final', maxCon_udf('ind')).show()


def q_75207950():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.createDataFrame([
        (2, "2022-02-02", "2022-02-01 10:03"),
        (3, "2022-02-01", "2022-02-01 10:00"),
        (2, "2022-02-02", None),
        (3, "2022-02-01", "2022-02-03 11:35"),
        (1, "2022-02-01", None),
        (2, "2022-02-02", "2022-02-02 10:05"),
        (3, "2022-02-01", "2022-02-01 10:05"),
        (4, "2022-02-02", None),
        (1, "2022-02-01", "2022-02-01 10:05"),
        (2, "2022-02-02", "2022-02-02 10:05"),
        (4, "2022-02-02", "2022-02-03 11:35"),
        (1, "2022-02-01", None),
        (1, "2022-02-01", "2022-02-01 10:03"),
        (1, "2022-02-01", "2022-02-01 10:05"),
        (4, "2022-02-02", "2022-02-03 11:35"),
        (2, "2022-02-02", "2022-02-02 11:00"),
        (4, "2022-02-02", "2022-02-03 11:35"),
        (3, "2022-02-01", "2022-02-04 11:35"),
        (1, "2022-02-01", "2022-02-01 10:00"),
    ], ("id", "install_time_first", "timestamp"))

    w = Window.partitionBy(col("id")).orderBy(col("install_time_first"))
    w2 = Window.orderBy(col("install_time_first"))
    df = df.withColumn("prev_id", lag("id", 1, None).over(w))
    df.withColumn("index", when(df.prev_id.isNull() | (df.prev_id != df.id), 1).otherwise(0)) \
        .withColumn("index", sum("index").over(w2.rowsBetween(Window.unboundedPreceding, Window.currentRow))) \
        .orderBy("install_time_first", "id").drop("prev_id").show()


def q_75207950_2():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.createDataFrame([
        (2, "2022-02-02", "2022-02-01 10:03"),
        (3, "2022-02-01", "2022-02-01 10:00"),
        (2, "2022-02-02", None),
        (3, "2022-02-01", "2022-02-03 11:35"),
        (1, "2022-02-01", None),
        (2, "2022-02-02", "2022-02-02 10:05"),
        (3, "2022-02-01", "2022-02-01 10:05"),
        (4, "2022-02-02", None),
        (1, "2022-02-01", "2022-02-01 10:05"),
        (2, "2022-02-02", "2022-02-02 10:05"),
        (4, "2022-02-02", "2022-02-03 11:35"),
        (1, "2022-02-01", None),
        (1, "2022-02-01", "2022-02-01 10:03"),
        (1, "2022-02-01", "2022-02-01 10:05"),
        (4, "2022-02-02", "2022-02-03 11:35"),
        (2, "2022-02-02", "2022-02-02 11:00"),
        (4, "2022-02-02", "2022-02-03 11:35"),
        (3, "2022-02-01", "2022-02-04 11:35"),
        (1, "2022-02-01", "2022-02-01 10:00"),
    ], ("id", "install_time_first", "timestamp"))

    df_with_index = df.select("id", "install_time_first").distinct().orderBy("install_time_first", "id") \
        .withColumn("index", monotonically_increasing_id() + 1) \
        .withColumnRenamed("id", "id2").withColumnRenamed("install_time_first", "install_time_first2")
    df.join(df_with_index, (df.id == df_with_index.id2) & (df.install_time_first == df_with_index.install_time_first2),
            "left").orderBy("install_time_first", "id").drop("id2", "install_time_first2").show()


def q_75289895():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.createDataFrame([(142, ["Big House", "Green frog"])], ["AnonID", "New_Data"])
    df = df.withColumn("New_Data", flatten(transform('New_Data', lambda x: split(x, ' '))))
    df.show(truncate=False)


def q_75300476():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.createDataFrame(
        data=[["john", "tomato", 1.99, 1], ["john", "carrot", 0.45, 1], ["bill", "apple", 0.99, 1],
              ["john", "banana", 1.29, 1], ["bill", "taco", 2.59, 1]], schema=["name", "food", "price", "col_1"])
    df = df.groupBy('name').agg(collect_list(concat_ws(' ', 'food', 'price')).alias('sample'))
    df.show(10, False)
    df.printSchema()


def q_75378166():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.createDataFrame(
        data=[["console", "ps5", -10, 8, 1], ["console", "xbox", -8, 6, 0],
              ["console", "ps5", -5, 4, 4], ["console", "xbox", -1, 10, 7], ["console", "xbox", 0, 2, 3],
              ["games", "ps5", -11, 48, 9], ["games", "ps5", -3, 2, 4], ["games", "xbox", 5, 10, 2]
              ], schema=["item", "type", "days_diff", "placed_orders", "cancelled_orders"])
    df = df.groupBy("item", "type").agg(
        collect_list("days_diff").alias("days_diff"),
        collect_list("placed_orders").alias("placed_orders"),
        collect_list("cancelled_orders").alias("cancelled_orders")
    )
    df = df.groupBy("item").agg(
        collect_list("type").alias("types"),
        collect_list("days_diff").alias("days_diff"),
        collect_list("placed_orders").alias("placed_orders"),
        collect_list("cancelled_orders").alias("cancelled_orders")
    )
    df.show(10, False)


def q_75368847():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.createDataFrame([("FTE:56e662f", "CATENA", 0, "CURRENT",
                                 ({"hr_code": 84534, "bgc_val": 170187, "interviewPanel": 6372, "meetingId": 3671})),
                                ("FTE:633e7bc", "Data Science", 0, "CURRENT",
                                 ({"hr_code": 21036, "bgc_val": 170187, "interviewPanel": 764, "meetingId": 577})),
                                ("FTE:d9badd2", "CATENA", 0, "CURRENT",
                                 ({"hr_code": 60696, "bgc_val": 88770}))],
                               ["empId", "organization", "h_cd", "status", "additional"])
    df.show(10, False)
    df.printSchema()
    df.select(struct("additional").alias("additional")).select("additional.*").show(10, False)


def q_75629735():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.createDataFrame([("2023-03-03 17:21:00", 10, '[{"num":55,"cor":32},{"num":14,"cor":54}]'),
                                ("2023-03-03 17:35:00", 11, '[{"num":55,"cor":98},{"num":32,"cor":77}]')],
                               ["timestamp", "offset", "stringdecode"])
    df.show(10, False)
    df.printSchema()
    df.withColumn("stringdecode", expr("substring(stringdecode, 2, length(stringdecode)-2)")) \
        .show(truncate=False)


def q_75758203():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.createDataFrame([
        (1, "10/1/2016 07:25:52 AM"),
        (1, "10/1/2016 08:53:38 AM"),
        (1, "10/1/2016 11:18:50 AM"),
        (1, "10/1/2016 11:19:32 AM"),
        (2, "10/1/2016 10:25:36 AM"),
        (2, "10/1/2016 10:28:08 AM"),
        (3, "10/1/2016 10:57:41 AM"),
        (3, "10/1/2016 08:57:10 PM")
    ],
        ["UID", "Time"])
    df.show(10, False)
    w = Window.orderBy("Time")
    df = df.withColumn("Time", to_timestamp("Time", "MM/d/yyyy hh:mm:ss a")).orderBy("Time").withColumn("PREV_Time",
                                                                                                        lag("Time", 1,
                                                                                                            None).over(
                                                                                                            w)).withColumn(
        "DIFF", col("Time") - col("PREV_Time"))
    df.show(10, False)


def q_75761644():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.read.option("multiline", "true").json("./ressources/75761644.json")
    df.printSchema()
    df.show()
    df = df.select(col("Id"), col("device"), explode(col("Ads")).alias("Ads")) \
        .select("Id", "device", "Ads.*") \
        .select(col("Id"), col("device"), explode(col("Adlist")).alias("Adlist"), col("placement")) \
        .select("Id", "device", "placement", "Adlist.*")
    df.printSchema()
    df.show()


def apply_schema_to_dataframe(df, schema):
    for field in schema.fields:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType)).withMetadata(field.name, field.metadata).n
    return df


def q_75800516():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df1 = spark.createDataFrame([(1, 2, 3), (2, 3, 4)], ["A", "B", "C"])
    df2 = spark.createDataFrame([(1, 2, 3), (2, 3, 4)], ["B", "C", "A"])

    fSchema = StructType([
        StructField("A", IntegerType(), False),
        StructField("B", IntegerType(), True),
        StructField("C", IntegerType(), True)
    ])

    apply_schema_to_dataframe(df1, fSchema).show()
    apply_schema_to_dataframe(df2, fSchema).show()
    apply_schema_to_dataframe(df2, fSchema).printSchema()


def q_75803825():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.createDataFrame(
        [("2023-03-05 MONTH   2020M03 2020-03-01  2020-03-31  ADDR    Fargo-Wahpeton , ND-MN   61.30   98.52   433.23",
          1),
         ("2023-03-05 MONTH   2020M03 2020-03-01  2020-03-31  STATE   TX  43.38   74.61   380.82", 2),
         (
             "2023-03-05 MONTH   2020M03 2020-03-01  2020-03-31  ADDR    Kalamazoo-Battle Creek-Portage, MI  30.19   49.06   266.33",
             3)
         ],
        schema=["value", "id"])
    df.show(truncate=False)
    regex_pattern = '([\s]*)((?<!,)[\s]+)'

    columns = ["insert_dt", "sub_type", "ret_month", "month_start", "month_end", "area_class", "area_details",
               "sub_paid", "sub_pending", "sub_annual_amt"]

    for idx, column in enumerate(columns):
        df = df.withColumn(column, trim(split("value", regex_pattern).getItem(idx)))
    df.show(truncate=False)


def q_75878694():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.createDataFrame(spark.sparkContext.parallelize([
        ['A', 'T1', '2023-01-01'],
        ['A', 'T1', '2023-01-02'],
        ['A', 'T2', '2023-01-03'],
        ['A', 'T2', '2023-01-04'],
        ['A', 'T2', '2023-01-05'],
        ['A', 'T1', '2023-01-06'],
        ['A', 'T1', '2023-01-07'],
        ['A', 'T1', '2023-01-08'],
        ['A', 'T1', '2023-01-09'],
        ['A', 'T1', '2023-01-10'],
        ['B', 'T1', '2023-01-01'],
        ['B', 'T1', '2023-01-02'],
        ['B', 'T1', '2023-01-03'],
        ['B', 'T1', '2023-01-04'],
        ['B', 'T1', '2023-01-05'],
    ]),
        ['Person', 'Task', 'Time'])
    df.show(100, False)
    w = Window.orderBy("Person", "Time")
    df = df.withColumn("Prev_Task", lag("Task", 1, None).over(w)) \
        .withColumn("Prev_Person", lag("Person", 1, None).over(w)) \
        .withColumn("runID",
                    when((col("Prev_Task").isNotNull and col("Task") != col("Prev_Task")) | (
                            col("Person") != col("Prev_Person")), 1).otherwise(0)) \
        .withColumn("runID", sum("runID").over(w) + 1).drop("Prev_Task", "Prev_Person")
    df.show(100, False)


def q_75888427():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.createDataFrame(spark.sparkContext.parallelize([
        [1, 0, "2022-10-01"],
        [1, 1, "2022-10-02"],
        [1, 0, "2022-10-03"],
        [1, 0, "2022-10-04"],
        [1, 0, "2022-10-20"],
        [1, 1, "2023-02-01"],
    ]),
        ['id', 'flag_code', 'date'])

    w = Window.orderBy("date")
    df = df.withColumn("diff", when(col("flag_code") == lag("flag_code").over(w), 0).otherwise(1)) \
        .withColumn("sum", sum("diff").over(w)) \
        .withColumn("next_sum", col("sum") + 1)
    df_min = df.groupby("sum").agg(min("date").alias("next_date"))
    # df = df.drop("sum").join(df_min, df.next_sum == df_min.sum, "left")#.select("id", "flag_code", "date", "next_date")
    df.orderBy("date").show()


def q_75920419():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.createDataFrame(spark.sparkContext.parallelize([
        [1, "me &amp; you", "value"],
        [2, "&gt; &quot;", None],
    ]), ['id', 'text', 'test'])

    def unescape_html(value):
        return html.unescape(value) if isinstance(value, str) else value

    unescape_html_udf = udf(unescape_html)
    for column in df.columns:
        df = df.withColumn(column, unescape_html_udf(col(column)))
    df.show()


def q_75926155():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.createDataFrame(spark.sparkContext.parallelize([
        [1, "me &amp; you", "value"],
        [2, "&gt; &quot;", None],
    ]), ['id', 'text', 'test'])
    df.show()
    df.withColumn("hour", lpad(hour(to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss 'UTC'")), 2, "0")).show()


def q_75942148():
    # create a SparkSession
    from pyspark.sql import functions as F
    spark = SparkSession.builder.appName("DateDataFrame").getOrCreate()

    # define start_date and end_date
    start_date = "1900-01-01"
    end_date = "2022-01-31"

    from datetime import datetime
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    delta = end_date_obj - start_date_obj

    date_df = spark.range(0, delta.days).select(
        F.date_add(F.to_date(F.lit(start_date)), F.col("id").cast("int")).alias("date"))
    date_df.withColumn("percentage", regexp_replace(lit("10.62%"), "%", "").cast("double") / 100).show()
    print(10.62 / 100)


def q_75957287():
    # create a dataframe
    spark = SparkSession.builder.appName("DateDataFrame").getOrCreate()
    df = spark.createDataFrame([
        (1, ""),
        (None, "val1"),
        (2, "val2"),
        (None, ""),
    ], ['col_1', 'col_2'])

    # filter out rows where col_1 is empty or a string value and col_2 is empty
    filtered_df = df.filter((~isnull('col_1')) & (~isnan('col_2')) & (col("col_2") != ""))

    # show the result
    df.show()
    filtered_df.show()


def q_75957812():
    spark = SparkSession.builder.appName("DateDataFrame").getOrCreate()
    data = [("xreplacey me", "replace"), ("I love me", "love"), ("some text", "hello")]
    df = spark.createDataFrame(data, ['c1', 'c2'])
    df.show()
    df.withColumn("c1", expr('regexp_replace(c1, concat("x" , c2, "y"), "")')).show()


def q_75986968():
    spark = SparkSession.builder.appName("DateDataFrame").getOrCreate()
    data = [("Kabelleng", "OB10341_000 - Sling "), ("Kabelleng", None), ("Kabelleng", None),
            ("Kabelleng_2", "OB20121_000 - Sling"), ("Kabelleng_2", None), ("Kabelleng_2", "OB20121_000 - Sling (2)")]
    df = spark.createDataFrame(data, ['AssetName', 'AssetCategoryName'])
    df.show(truncate=False)
    w = Window.partitionBy("AssetName")
    df.withColumn("AssetCategoryName",
                  coalesce(col("AssetCategoryName"), first("AssetCategoryName", True).over(w))).show(truncate=False)


def q_75995607():
    spark = SparkSession.builder.appName("DateDataFrame").getOrCreate()
    data = [
        ("a1", "Engalnd", None),
        ("a2", "Engalnd", None),
        ("b1", "Engalnd", None),
        ("b2", "Engalnd", None),
        ("c1", "Engalnd", None),
        ("c2", "Engalnd", None),
        ("er3", "Engalnd", None),
        ("po9", "Engalnd", None),
        ("ee4", "Engalnd", None),
        ("e4", "Engalnd", None),
        ("t5", "Engalnd", None),
        ("u8", "Engalnd", None),
        ("r4", "Engalnd", None),
        ("zx1", "Engalnd", None),
        ("11d", "Engalnd", None),
        ("22", "Engalnd", None),
        ("2p", "Engalnd", None),
        ("3jk", "Engalnd", None),
        ("56h", "Engalnd", None),
        ("a78", "Engalnd", None),
        ("xxx", "Engalnd", "value1"),
        ("zzz", "Engalnd", "value2"),
    ]
    df = spark.createDataFrame(data, ['Id_a', 'Country', 'Type'])

    missingTypeDf = df.filter(col("Type").isNull())
    notMissingTypeDf = df.filter(~col("Type").isNull())

    from pyspark.sql.functions import rand
    fractions = [0.8, 0.2]

    # Split the DataFrame
    chunkDf1, chunkDf2 = missingTypeDf.randomSplit(fractions, seed=13)

    chunkDf1 = chunkDf1.withColumn("Type", lit("R"))
    chunkDf2 = chunkDf2.withColumn("Type", lit("NR"))

    resultDf = notMissingTypeDf.unionByName(chunkDf1).unionByName(chunkDf2)

    resultDf.show(200, truncate=False)


def q_4628618():
    spark = SparkSession.builder.appName("DateDataFrame").getOrCreate()
    data = [
        ("The error is in %s value is %s.", "xx", "z"),
        ("The new cond is in %s is %s.", "y", "ww"),
    ]
    df = spark.createDataFrame(data, ['ErrorDescBefore', 'name', 'value'])

    format_udf = udf(lambda str, name, value: str.replace('%s', name, 1).replace('%s', value, 1))

    df.withColumn("ErrorDescAfter", format_udf(col("ErrorDescBefore"), col("name"), col("value"))).show(truncate=False)
    df = df.withColumn("ErrorDescAfter", regexp_replace(col("ErrorDescBefore"), col("name"), "", 1)).show(
        truncate=False)


def q_75996404():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [
        (1,
         "questionA : put returns between paragraphs questionB : indent code by 4 spaces questionC : for linebreak add 2 spaces at end"),
        (2, "questionA : add language identifier questionB : create code fences questionC : to highlight code"),
    ]
    df = spark.createDataFrame(data, ['id', 'description'])

    df.withColumn("regex", split("description", "questionB\\s+:*\\s+").getItem(1)) \
        .withColumn("regex", trim(split("regex", "question").getItem(0))).show(truncate=False)


def q_75998018():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [
        (1, "A"),
        (2, "B"),
        (3, "C"),
        (4, "D"),
        (5, "E"),
    ]
    df = spark.createDataFrame(data, ['id', 'val'])
    df.withColumn("index", monotonically_increasing_id() + 1).show()


def q_76003512():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [
        (123, 345, "24/03/2023 09:06"),
        (123, 345, "24/03/2023 09:06"),
        (123, 345, "24/03/2023 09:04"),
        (234, 567, "24/03/2023 09:05"),
        (234, 567, "24/03/2023 09:05"),
        (234, 567, "23/03/2023 09:05"),
    ]
    df = spark.createDataFrame(data, ['vehicleNumber', 'ProductionNumber', 'checkDate'])
    df.show()

    from pyspark.sql.functions import max, to_timestamp
    from pyspark.sql.window import Window

    # Convert checkDate to datetime format
    df = df.withColumn("checkDate", to_timestamp("checkDate", "dd/MM/yyyy HH:mm"))

    # Define the window specification
    windowSpec = Window.partitionBy(["vehicleNumber", "ProductionNumber"])

    # Apply the window function and select the rows with max checkDate
    maxDateDF = df.select("*", max("checkDate").over(windowSpec).alias("maxDate")) \
        .filter("checkDate = maxDate") \
        .drop("maxDate")

    maxDateDF.show()


def q_76005319():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [
        ("First", "a a a a b c d e f c d s"),
        ("Second", "d f g r b d s z e r a e"),
        ("Thirs", "d f g v c x w b c x s d f e"),
    ]
    df = spark.createDataFrame(data, ['column1', 'column2'])
    df.show()

    pattern = "(?i)\\b(?:b\\W+)(\\w+\\W+\\w+\\W+\\w+\\W+\\w+\\W+\\w+)\\b"
    df = df.withColumn("column2", regexp_extract(col("column2"), pattern, 1))
    df.show(truncate=False)


def q_76013013():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [
        ("apple", 1, 4, None),
        ("apple", 2, None, -2),
        ("apple", 3, None, 5),
        ("banana", 1, 12, None),
        ("banana", 2, None, 4),
        ("banana", 1, 1, None),
        ("banana", 2, None, -1),
    ]
    df = spark.createDataFrame(data, ["Category", "Time", "Stock-level", "Stock-change"])
    df.show()

    get_default = lambda column: when(column.isNull(), 0).otherwise(column)
    w = Window.partitionBy("Category").orderBy("Time")
    df.withColumn("Stock-level", lag("Stock-level").over(w) + get_default(col("Stock-change"))).show()


def q_76029862():
    spark = SparkSession.builder.appName("test").getOrCreate()
    read_df = spark \
        .read \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS 'UTC'") \
        .option("inferSchema", "true") \
        .option("inferTimestamp", "true") \
        .json("./ressources/76029862.json")
    read_df.show()
    read_df.printSchema()


def q_76073065():
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.read.option("inferSchema", "true").option("multiline", "true").json("./ressources/76073065.json")
    df = df.withColumn("tables", explode(col("tables"))).select("tables.*").withColumn("rows", explode(col("rows"))) \
        .withColumn("tmp", explode(arrays_zip("columns", "rows"))).select("tmp.columns.name", "tmp.rows")

    df.groupBy(lit(1)).pivot("name").agg(first(col("rows"))).drop("1").show()


def q_76076409():
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.createDataFrame(
        [
            (1001, "2023-04-01", False, 0, 0),
            (1001, "2023-04-02", False, 0, 0),
            (1001, "2023-04-03", False, 1, 1),
            (1001, "2023-04-04", False, 1, 1),
            (1001, "2023-04-05", True, 4, 3),
            (1001, "2023-04-06", False, 4, 3),
            (1001, "2023-04-07", False, 4, 3),
            (1001, "2023-04-08", False, 10, 6),
            (1001, "2023-04-09", True, 10, 0),
            (1001, "2023-04-10", False, 12, 2),
            (1001, "2023-04-11", False, 13, 3),
        ],
        ["id", "date", "reset", "cumsum", "new_cumsum"],
    )

    w = Window.orderBy("date")
    w2 = Window.partitionBy("partition").orderBy("date")
    df = df.withColumn("diff", col("cumsum") - lag("cumsum", default=0).over(w)) \
        .withColumn("partition", when(~col("reset"), 0).otherwise(1)) \
        .withColumn("partition", sum("partition").over(w)) \
        .withColumn("new_cumsum_2", sum(col("diff")).over(w2)).drop("diff", "partition")

    df.show()


# not = ~
if __name__ == "__main__":
    q_76076409()
