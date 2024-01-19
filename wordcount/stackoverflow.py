import re

from pyspark import SparkContext
from pyspark.sql import SparkSession, Window, functions, Column
from pyspark.sql.functions import col, to_date, last_day, lit, when, lower, concat, sum, unix_timestamp, \
    month, lpad, split, expr, udf, posexplode, regexp_replace, collect_set, lag, approx_count_distinct, coalesce, \
    row_number, explode, monotonically_increasing_id, first, from_json, aggregate, create_map, map_concat, to_json, \
    flatten, transform, collect_list, concat_ws, struct, to_timestamp, format_number, isnan, unhex, substring, \
    dense_rank, last, desc, asc, arrays_zip, datediff, array, map_zip_with, map_from_arrays, array_repeat
from pyspark.sql.types import StructType, ArrayType, MapType, StringType, StructField, IntegerType, DoubleType

spark = SparkSession.builder.getOrCreate()


def q_1():
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
    data = [["100MG"], ["1EA"], ["100MG"]]
    df = spark.createDataFrame(data).toDF("size")

    def split_func(str):
        return re.sub("[A-Za-z]+", lambda ele: " " + ele[0] + " ", str)

    split_udf = udf(split_func)

    df.withColumn("splitted", split_udf(col("size"))).show()


def q_74892964():
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
    df1.join(df2, col("join_column") == col("join_column_2"), "left") \
        .withColumn("column2", when(col("column2") == col("column2_2"), None).otherwise(
        coalesce(col("column2"), col("column2_2")))) \
        .withColumn("column3",
                    when(col("column3") == col("column3_2"), None).otherwise(coalesce("column3", "column3_2"))) \
        .drop("join_column", "join_column_2", "column1_2", "column2_2", "column3_2").show()


def q_74965630():
    df = spark.read.option("header", "true").csv("./ressources/1.csv", sep='┐')
    df.show()


def q_75060820():
    df = spark.createDataFrame([(1, 4, 3), (2, 4, 2), (3, 4, 5), (1, 5, 3), (2, 5, 2), (3, 6, 5)], ['a', 'b', 'c'])
    w = Window.partitionBy(col("b")).orderBy(col("b"))
    df.withColumn("d", row_number().over(w)).filter(col("d") <= 2).show()


def q_75061097():
    # prepare data

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
    test_df = spark.createDataFrame([
        (1, 1, 1, 0, 1, 1)
    ], ("b1", "b2", "b3", "b4", "b5", "b6"))
    cols = test_df.columns
    test_df = test_df.withColumn('Ind', concat(*cols))

    maxCon_udf = udf(lambda x: max(map(len, x.split('0'))))
    test_df.withColumn('final', maxCon_udf('ind')).show()


def q_75207950():
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
    df = spark.createDataFrame([(142, ["Big House", "Green frog"])], ["AnonID", "New_Data"])
    df = df.withColumn("New_Data", flatten(transform('New_Data', lambda x: split(x, ' '))))
    df.show(truncate=False)


def q_75300476():
    df = spark.createDataFrame(
        data=[["john", "tomato", 1.99, 1], ["john", "carrot", 0.45, 1], ["bill", "apple", 0.99, 1],
              ["john", "banana", 1.29, 1], ["bill", "taco", 2.59, 1]], schema=["name", "food", "price", "col_1"])
    df = df.groupBy('name').agg(collect_list(concat_ws(' ', 'food', 'price')).alias('sample'))
    df.show(10, False)
    df.printSchema()


def q_75378166():
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


def q_75670176():
    df = spark.createDataFrame([[2000000.0, 759740220.0]], ['sale_amt', 'total_value'])
    df.show()
    df = df.withColumn("new_col", functions.round(col("total_value")).cast(StringType()))
    df.withColumn("new_col", format_number("total_value", 1)).show()

    @udf(returnType=StringType())
    def to_string(value):
        return str(value)

    df.withColumn("new_col", to_string(col("total_value"))).show()
    df.withColumn("new_col", transform("total_value", lambda x: str(x))).show()


def q_75587842():
    url = "https://gist.githubusercontent.com/JishanAhmed2019/e464ca4da5c871428ca9ed9264467aa0/raw/da3921c1953fefbc66dddc3ce238dac53142dba8/failure.csv"
    from pyspark import SparkFiles
    spark.sparkContext.addFile(url)
    df = spark.read.csv(SparkFiles.get("failure.csv"), header=True, sep='\t')
    df.show(2)
    spark.range(1).write.parquet("proto.parquet")


def q_75699018():
    df = spark.createDataFrame([["2023-03-02T07:32:00+00:00"]], ['timestamp'])
    df.show(truncate=False)
    df = df.withColumn("timestamp_utc", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssXXX"))
    df.show(truncate=False)
    df.printSchema()


def q_75368847():
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


def q_76076547():
    df = spark.createDataFrame([(1, "2023-12-10T11:00:00.826+0000")],
                               ["id", "etaTs"])
    df.show(10, False)
    df = df.withColumn(
        "etaTs",
        to_timestamp(col("etaTs"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    )
    df.show()


def q_76078849():
    df = spark.createDataFrame([
        (1, [[1, 30981733]]),
        (1, [[1, 598319049], [2, 38453298], [3, 2007569845]]),
        (1, [[1, 10309216]]),
        (1, [[1, 730446111], [2, 617024811], [3, 665689309], [4, 883699488], [5, 159896736]]),
        (1, [[1, 10290923], [2, 33282357]]),
        (1, [[1, 102649381], [2, 10294853], [3, 10294854], [4, 44749181], [5, 35132896]]),
        (1, [[1, 10307642], [2, 10307636], [3, 15754215], [4, 45612359], [5, 10307635]]),
        (1, [[1, 43982130], [2, 15556050], [3, 15556051], [4, 11961012], [5, 16777263]]),
        (1, [[1, 849607426], [2, 185158834], [3, 11028011], [4, 10309801], [5, 11028010]]),
        (1, [[1, 21905160], [2, 21609422], [3, 21609417], [4, 20554612], [5, 20554601]])
    ], ["id", "substitutes"])
    df.show(10, False)
    df.printSchema()
    df.rdd.map(lambda row: list(map(lambda arr: arr[-1], row.substitutes))).toDF().show()
    df = df.withColumn("substitutes_2", concat(*[col("substitutes")[i] for i in range(5)]))
    df = df.select("*", *[col("substitutes_2").getItem(i).alias(f"new_column_{i}") for i in range(5)])
    df.show(10, False)


def q_76036392():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    import pyspark.sql.functions as f
    from pyspark.sql.functions import lit

    # User input for number of rows
    n_a = 10
    n_a_c = 5
    n_a_c_d = 3
    n_a_c_e = 4

    # Define the schema for the DataFrame
    schema_a = StructType([StructField("id1", StringType(), True)])
    schema_a_b = StructType(
        [
            StructField("id1", StringType(), True),
            StructField("id2", StringType(), True),
            StructField("extra", StringType(), True),
        ]
    )
    schema_a_c = StructType(
        [
            StructField("id1", StringType(), True),
            StructField("id3", StringType(), True),
        ]
    )
    schema_a_c_d = StructType(
        [
            StructField("id3", StringType(), True),
            StructField("id4", StringType(), True),
        ]
    )
    schema_a_c_e = StructType(
        [
            StructField("id3", StringType(), True),
            StructField("id5", StringType(), True),
        ]
    )

    # Create a list of rows with increasing integer values for "id1" and a constant value of "1" for "id2"
    rows_a = [(str(i),) for i in range(1, n_a + 1)]
    rows_a_integers = [str(i) for i in range(1, n_a + 1)]
    rows_a_b = [(str(i), str(1), "A") for i in range(1, n_a + 1)]

    def get_2d_list(ids_part_1: list, n_new_ids: int):
        rows = [
            [
                (str(i), str(i) + "_" + str(j))
                for i in ids_part_1
                for j in range(1, n_new_ids + 1)
            ]
        ]
        return [item for sublist in rows for item in sublist]

    rows_a_c = get_2d_list(ids_part_1=rows_a_integers, n_new_ids=n_a_c)
    rows_a_c_d = get_2d_list(ids_part_1=[i[1] for i in rows_a_c], n_new_ids=n_a_c_d)
    rows_a_c_e = get_2d_list(ids_part_1=[i[1] for i in rows_a_c], n_new_ids=n_a_c_e)

    # Create the DataFrame
    df_a = spark.createDataFrame(rows_a, schema_a)
    df_a_b = spark.createDataFrame(rows_a_b, schema_a_b)
    df_a_c = spark.createDataFrame(rows_a_c, schema_a_c)
    df_a_c_d = spark.createDataFrame(rows_a_c_d, schema_a_c_d)
    df_a_c_e = spark.createDataFrame(rows_a_c_e, schema_a_c_e)
    df_a.join(df_a_b, on="id1").join(df_a_c, on="id1").join(df_a_c_d, on="id3")
    df_a_c_e.show(truncate=False)
    # Join everything
    df_join = (
        df_a.join(df_a_b, on="id1")
        .join(df_a_c, on="id1")
        .join(df_a_c_d, on="id3")
        .join(df_a_c_e, on="id3")
    )

    # Nested structure
    # show
    df_nested = df_join.withColumn("id3", f.struct(f.col("id3"))).orderBy("id4")

    for i, index in enumerate([(5, 3), (4, 3), (3, None)]):
        remaining_columns = list(set(df_nested.columns).difference(set([f"id{index[0]}"])))
        df_nested = (
            df_nested.groupby(*remaining_columns)
            .agg(f.collect_list(f.col(f"id{index[0]}")).alias(f"id{index[0]}_tmp"))
            .drop(f"id{index[0]}")
            .withColumnRenamed(
                f"id{index[0]}_tmp",
                f"id{index[0]}",
            )
        )

        if index[1]:
            df_nested = df_nested.withColumn(
                f"id{index[1]}",
                f.struct(
                    f.col(f"id{index[1]}.*"),
                    f.col(f"id{index[0]}"),
                ).alias(f"id{index[1]}"),
            ).drop(f"id{index[0]}")

    # Investigate for duplicates in id3 (should be unique)
    df_test = df_nested.select("id2", "extra", f.explode(f.col("id3")["id3"]).alias("id3"))

    for i in range(30):
        df_test.groupby("id3").count().filter(f.col("count") > 1).show()


def q_76108321():
    df1 = spark.read.option("multiline", "true").json("./ressources/76108321_1.json")
    df2 = spark.read.option("multiline", "true").json("./ressources/76108321_2.json") \
        .withColumnRenamed("data", "data_2").withColumnRenamed("id", "id_2").withColumnRenamed("total_seconds",
                                                                                               "total_seconds_2")

    df1_exploded = df1.withColumn("data", explode(col("data")))
    df2_exploded = df2.withColumn("data_2", explode(col("data_2"))).drop("total_seconds_2")

    resultDf = df1_exploded.join(df2_exploded, (df1_exploded.id == df2_exploded.id_2) & (
            df1_exploded.data.id == df2_exploded.data_2.id), "outer") \
        .withColumn("id", coalesce(col("id"), col("id_2"))) \
        .withColumn("data",
                    struct(coalesce(col("data.id"), col("data_2.id")), coalesce(col("data.name"), col("data_2.name")),
                           coalesce(col("data.seconds"), lit(0)) + coalesce(col("data_2.seconds"), lit(0)))) \
        .select("data", "id", "total_seconds") \
        .groupby("id").agg(collect_list("data").alias("data"))

    total_seconds_df = df1.join(df2, df1.id == df2.id_2, "outer") \
        .withColumn("id", coalesce(col("id"), col("id_2"))) \
        .withColumn("total_seconds", coalesce(col("total_seconds"), lit(0)) + coalesce(col("total_seconds_2"), lit(0))) \
        .select("id", "total_seconds")
    resultDf = resultDf.join(total_seconds_df, ["id"], "left")
    resultDf.show(truncate=False)
    resultDf.repartition(1).write.mode("overwrite").json("./ressources/output/76108321.json")


def q_76138313():
    import pandas as pd

    from pyspark.sql.functions import isnan

    users = {
        'name': ['John', None, 'Mike'],
        'salary': [400.0, None, 200.0]
    }

    pdf = pd.DataFrame(users)
    sdf = spark.createDataFrame(pdf)

    # filter out the rows with salaries greater than 300
    # sdf_filtered = sdf.filter(~isnan(sdf.salary) & (sdf.salary > 300))
    sdf.filter(sdf.name != "John").show()


def q_76133727():
    import pyspark.pandas as ps
    import pandas as pd

    df_pandas = spark.read.option("multiline", "true").json('./ressources/76133727.json')
    df_pandas.show()


def q_76149371():
    from pyspark.sql.functions import max

    df = spark.read.option("header", "true").option("delimiter", ";").csv("./ressources/76149371.csv")
    df.show(truncate=False)
    max_rank = df.agg(max("rank")).collect()[0][0]
    for i in range(1, int(max_rank) + 2):
        df = df.withColumn(f"sub{i}", when(col("rank") == i, col("sub")).otherwise(lit(0)))
    df.show(truncate=False)


def q_76148734():
    data = [
        ("A", ["1", "2", "3"], ["4", "5"], ["6"]),
        ("B", ["7", "8"], ["9"], ["10", "11", "12"]),
        ("C", ["13", "14", "15"], ["16", "17"], ["18", "19"]),
    ]
    df = spark.createDataFrame(data, ["col1", "array_col1", "array_col2", "array_col3"])
    df.show(truncate=False)
    expanded_df = df.select("col1", *[col("array_col1")[i].alias(f"col_{i + 1}") for i in range(3)])


def q_76150614():
    import pyspark.pandas as ps
    import pandas as pd
    import requests

    def get_vals(row):

        # make api call
        return row['A'] * row['B']

    # Create a pandas DataFrame
    pdf = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})

    # apply function - get api responses
    pdf['api_response'] = pdf.apply(lambda row: get_vals(row), axis=1)
    pdf.sample(5)
    # Unpack JSON API Response
    try:
        df = pd.json_normalize(pdf['api_response'].str['location'])
    except TypeError as e:
        print(f"Error: {e}")

    # To pySpark DataFrame
    psdf = ps.DataFrame(df)
    psdf.head(5)


def q_76362482():
    data = [
        ("A", ["comedy", "horror"]),
        ("B", ["romance", "comedy"]),
        ("C", ["thriller", "sci-fi"]),
        ("D", ["sci-fi", "horror", "thriller"]),
        ("E", ["sci-fi", "horror", "comedy"]),
        ("E", ["sci-fi", "romance", "comedy"]),
        ("E", ["horror", "romance", "comedy"]),
        ("E", ["thriller", "romance", "comedy"]),
    ]
    df = spark.createDataFrame(data, ["movie_description", "tags"])
    explodedDF = df.withColumn("id", monotonically_increasing_id()).withColumn("tags", explode(col("tags"))).select(
        "id", "tags")
    joinDf = explodedDF.join(explodedDF.withColumnRenamed("tags", "tags2"), ["id"], "left").filter(
        col("tags") != col("tags2"))
    pairCounts = joinDf.groupBy("tags", "tags2").count()
    pairCounts.show()
    maxCountValue = pairCounts.agg(functions.max(col("count"))).first()[0]
    max_tag_1 = pairCounts.filter(col("count") == maxCountValue).select("tags").first()[0]
    max_tag_2 = pairCounts.filter(col("count") == maxCountValue).select("tags2").first()[0]
    print(max_tag_1, max_tag_2)


def q_76362482_2():
    from pyspark.sql.functions import col, size, array_sort
    from pyspark.ml.fpm import FPGrowth

    df = spark.createDataFrame([
        ("A", ["comedy", "horror"]),
        ("B", ["romance", "comedy"]),
        ("C", ["thriller", "sci-fi"]),
        ("D", ["sci-fi", "horror", "thriller"]),
        ("E", ["sci-fi", "horror", "comedy"]),
        ("E", ["sci-fi", "romance", "comedy"]),
        ("E", ["horror", "romance", "comedy"]),
        ("E", ["thriller", "romance", "comedy"]),
    ], ["id", "items"])

    # Sort tags to ignore pairs order
    df = df.withColumn('items', array_sort(col('items')))

    fpGrowth = FPGrowth(itemsCol="items", minSupport=0.1, minConfidence=0.1)
    model = fpGrowth.fit(df)

    freq_tags = model.freqItemsets.filter(size(col('items')) == 2).sort(col('freq').desc())

    freq_tags.show()


def q_76366517():
    data1 = [("A", None, 1, 'Highest'), ("B", "A", 2, 'Medium'), ("C", "B", 3, 'Lowest'), ("D", "B", 3, 'Lowest')]
    df1 = spark.createDataFrame(data=data1, schema=['ID', 'ParentID', 'Hierarchy', 'HierarchyName'])

    df = df1.select(col("ID"), col("ParentID"),
                    functions.array(col("ID"), col("Hierarchy"), col("HierarchyName")).alias("HierarchyTree"))
    df2 = df.drop("ID")
    df = df.drop("ParentID")
    join = df.join(df2, df.ID == df2.ParentID, "left")

    join.show()


def q_76398305():
    from pyspark.sql import SparkSession

    from pyspark.sql.functions import udf
    # Create a SparkSession

    # Sample DataFrame with hex values
    data = [("48656c6c6f20576f726c64",),
            ("53616d706c65204279746573",),
            ("74657374206d657373616765",),
            ("48656c6c6f20576f726c647a21",),  # With non-hex character 'z'
            ("546573742e",)]  # With non-hex character '.'

    df = spark.createDataFrame(data, ["value"])

    byte_array_to_ascii = udf(lambda x: bytearray(x).decode('utf-8'))
    df = df.withColumn("ascii_value", byte_array_to_ascii("value"))
    df.show()


def q_76433635():
    df = spark.createDataFrame([(0, "A", 1), (1, "A", None), (2, "B", 2), (3, "B", None), (4, "B", 3)],
                               ["id", "col1", "col2"])
    pdf_lookup = spark.createDataFrame([(0, "A", 4), (1, "B", 5)], ["id", "col1", "col2"])

    pdf_lookup = pdf_lookup.select(col("col1"), col("col2").alias("col2_tmp"))
    df.join(pdf_lookup, ["col1"], "left").withColumn("col2", coalesce(col("col2"), col("col2_tmp"))).drop(
        "col2_tmp").show()


def q_x():
    df = spark.createDataFrame([(0, "azeazeazqsdf456546541", 1), (1, "12a32ze1aze51az65az", None)],
                               ["id", "couple_zone", "valeur"])

    df.withColumn("substr1", substring("couple_zone", 1, 8)).withColumn("substr2",
                                                                        substring("couple_zone", -8, 8)).show()


def q_76531824():
    df = spark.createDataFrame([
        (1, 10.02, "2023-01-28 19:22:59.266508"),
        (1, 2.02, "2023-01-28 20:22:59.266508"),
        (1, 5.0, "2023-02-28 12:21:34.466508"),
        (2, 18.32, "2023-01-18 01:34:01.222408")
    ], ["userID", "amount", "date"])

    df = df.withColumn("date", to_date("date")).groupby("date").agg(first("userID").alias("userID"),
                                                                    first("amount").alias("amount"),
                                                                    sum("amount").alias("acummulated_amount")) \
        .withColumn("acummulated_amount", sum("acummulated_amount").over(Window.partitionBy("userID").orderBy("date")))
    df.show()


def q_76907940():
    Programmatic_df = spark.createDataFrame([
        ("amazon", "IO123456"),
        ("google", "IO113456"),
        ("google", "IO111456"),
        ("yahoo", "IO111156"),
        ("amazon", "IO111116")
    ], ["Company", "contract number"])

    window_spec = Window.orderBy('Company')
    Programmatic_df = Programmatic_df.withColumn('Company number', dense_rank().over(window_spec))
    Programmatic_df.show()


def q_76907274():
    df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

    def func(itr):
        for person in itr:
            print(person.name)

    df.foreachPartition(func)


def q_76926353():
    df = spark.createDataFrame([(162038, "04/04/23 18:42", 1258, 972395),
                                (162038, "04/04/23 18:42", 1258, 551984),
                                (162038, "04/04/23 18:42", 1258, 488298),
                                (162038, "04/04/23 18:42", 1258, 649230),
                                (162038, "26/02/23 16:28", 2715, 372225),
                                (162038, "26/02/23 16:28", 2715, 911716),
                                (162038, "26/02/23 16:28", 2715, 696677),
                                (162038, "26/02/23 16:28", 2715, 229455),
                                (162038, "26/02/23 16:28", 2715, 870016),
                                (162038, "29/01/23 13:07", 1171, 113719),
                                (162038, "29/01/23 13:07", 1171, 553461),
                                (162060, "01/05/23 18:42", 1259, 300911),
                                (162060, "01/05/23 18:42", 1259, 574962),
                                (162060, "01/05/23 18:42", 1259, 843300),
                                (162060, "01/05/23 18:42", 1259, 173719),
                                (162060, "05/05/23 18:42", 2719, 254899),
                                (162060, "05/05/23 18:42", 2719, 776553),
                                (162060, "05/05/23 18:42", 2719, 244739),
                                (162060, "05/05/23 18:42", 2719, 170742),
                                (162060, "05/05/23 18:42", 2719, 525719),
                                (162060, "10/05/23 18:42", 1161, 896919),
                                (162060, "10/05/23 18:42", 1161, 759465)],
                               ["customer_id", "order_ts", "order_nbr", "item_nbr"])

    window_spec = Window.partitionBy("customer_id").orderBy("order_ts")
    df = df.withColumn("row_num", dense_rank().over(window_spec))
    df.show()


def q_77396807():
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
    from pyspark.sql.window import Window as w
    from datetime import datetime, date
    spark = SparkSession.builder.config("spark.sql.repl.eagerEval.enabled", True).getOrCreate()

    # Base dataframe
    df = spark.createDataFrame(
        [
            (1, date(2023, 10, 1), date(2023, 10, 2), "open"),
            (1, date(2023, 10, 2), date(2023, 10, 3), "close"),
            (2, date(2023, 10, 1), date(2023, 10, 2), "close"),
            (2, date(2023, 10, 2), date(2023, 10, 4), "close"),
            (3, date(2023, 10, 2), date(2023, 10, 4), "open"),
            (3, date(2023, 10, 3), date(2023, 10, 6), "open"),
        ],
        schema="id integer, date_start date, date_end date, status string"
    )

    # We define two partition functions
    partition = w.partitionBy("id").orderBy("date_start", "date_end").rowsBetween(w.unboundedPreceding,
                                                                                  w.unboundedFollowing)
    partition2 = w.partitionBy("id").orderBy("date_start", "date_end")

    # Define dataframe A
    A = df.withColumn(
        "date_end_of_last_close",
        f.max(f.when(f.col("status") == "close", f.col("date_end"))).over(partition)
    ).withColumn(
        "rank",
        f.row_number().over(partition2)
    )
    A.show()
    A_result = A.filter(f.col("rank") == 1).drop("rank")
    A_result.show()


def q_77443290():
    df = spark.read.option("multiline", "true").json("./ressources/77443290.json")
    df = df.withColumn("mydoc", explode("mydoc")).select("mydoc.*").withColumn("Information",
                                                                               explode("Information")).select(
        "Information.*")
    df = df.groupby().agg(*[concat_ws(",", collect_list(col)).alias(f"flat{col}") for col in df.columns])
    df.explain()


def q_77448212():
    localdf = spark.createDataFrame(
        spark.sparkContext.parallelize(
            [
                [1, 24, None, None],
                [1, 23, None, None],
                [1, 22, 1, 1],
                [1, 21, None, 1],
                [1, 20, None, 1],
                [1, 19, 1, 1],
                [1, 18, None, None],
                [1, 17, None, None],
                [1, 16, 2, 2],
                [1, 15, None, None],
                [1, 14, None, None],
                [1, 13, 3, 3],
            ]
        ),
        ["ID", "Record", "Target", "ExpectedValue"],
    )

    localdf.show()

    w = Window.partitionBy("ID").orderBy(desc("Record"))
    w2 = Window.partitionBy("ID").orderBy(asc("Record"))
    localdf = localdf.withColumn("last_desc", last("Target", ignorenulls=True).over(w)) \
        .withColumn("last_asc", last("Target", ignorenulls=True).over(w2)) \
        .withColumn("result", when(col("last_desc") == col("last_asc"), col("last_asc")).otherwise(None))
    localdf.orderBy("ID", desc("Record")).show()


def q_77449438():
    # Define the data
    data = [
        (3333, [{"code": 123, "description": "Business Email"}, {"code": 789, "description": "Primary Email"}]),
        (9234, [{"code": 123, "description": "Business Email"}, {"code": 789, "description": "Primary Email"},
                {"code": 456, "description": "Secondary Email"}])
    ]

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("struct_field", ArrayType(StructType([
            StructField("code", IntegerType(), True),
            StructField("description", StringType(), True)
        ]), True), True)
    ])

    # Create the DataFrame
    df = spark.createDataFrame(data, schema)
    df.printSchema()
    df.withColumn("struct_field", explode("struct_field")).select("ID", "struct_field.*").show(truncate=False)


def q_77539886():
    df = spark.createDataFrame([
        (4, 2, 7, 9, 1, 3)
    ], ["A", "B", "C", "D", "E", "F"])
    for column in df.columns:
        df = df.withColumn(column, struct(col(column), lit(" " + column)))
    df.show()
    df = df.select(col("A").cast("string"), col("B").cast("string"))

    concatenation_expr = expr("concat_ws(', ', {}) as AllData".format(", ".join(df.columns)))
    concatenation_expr = expr("concat_ws({}) as AllData".format(", ".join(["'{}'".format(col) for col in df.columns])))

    concatenated_df = df.selectExpr("*", concatenation_expr)

    concatenated_df.show()


def q_77558127():
    spark_df = spark.createDataFrame([
        (4, 2, 7, 9, 1, 3)
    ], ["A", "B", "C", "D", "E", "F"])

    def my_function(row, index_name):
        return True

    def partition_func(rows):
        for row in rows:
            print(row)
        return my_function(rows, "blabla")

    spark_df.foreachPartition(partition_func)


def q_77587293():
    df = spark.createDataFrame([(['a', 'b', 'c'], [1, 4, 2], 3),
                                (['b', 'd'], [7, 2], 1),
                                (['a', 'c'], [1, 2], 8)], ["id", "label", "md"])

    df = df.selectExpr("md", "inline(arrays_zip(id, label))")
    w = Window.partitionBy("md")
    df.withColumn("mx_label", functions.max("label").over(w)).filter(col("label") == col("mx_label")).drop(
        "mx_label").show(truncate=False)


def q_77582760():
    df = spark.createDataFrame(
        [(1, 1), (2, None), (3, None), (4, None), (5, 5), (6, None), (7, None), (8, 8), (9, None), (10, None),
         (11, None), (12, None)],
        ["row_id", "group_id"])

    df.withColumn("group_id", last("group_id", ignorenulls=True).over(Window.orderBy("row_id"))).show()


def q_77587293():
    df = spark.createDataFrame([(['a', 'b', 'c'], [1, 4, 2], 3),
                                (['b', 'd'], [7, 2], 1),
                                (['a', 'c'], [1, 2], 8)], ["id", "label", "md"])

    df = df.selectExpr("md", "inline(arrays_zip(id, label))")
    w = Window.partitionBy("md")
    df.withColumn("mx_label", functions.max("label").over(w)).filter(col("label") == col("mx_label")).drop(
        "mx_label").show(truncate=False)


def q_77625270():
    df = spark.createDataFrame([
        (11, "09/11/2023"),
        (11, "13/11/2023"),
        (11, "15/11/2023"),
        (11, "21/11/2023"),
        (11, "23/11/2023"),
        (11, "24/11/2023"),
        (12, "16/11/2023"),
        (12, "21/11/2023"),
        (12, "25/11/2023"),
        (12, "01/12/2023"),
        (12, "03/12/2023"),
        (12, "05/12/2023")
    ], ["Cust_Id", "Date_Of_Purchase"])
    w = Window.partitionBy("Cust_Id").orderBy("Date_Of_Purchase")
    df = df.withColumn("Date_Of_Purchase", to_date(col("Date_Of_Purchase"), "dd/MM/yyyy")) \
        .withColumn("Prev_Date_Of_Purchase", lag("Date_Of_Purchase", 2).over(w)) \
        .withColumn("days_between", datediff(col("Date_Of_Purchase"), col("Prev_Date_Of_Purchase")))
    df.show()

    df.filter(col("days_between") <= 5).select("Cust_Id").distinct().show()


def q_77623335():
    df1 = spark.createDataFrame([
        (1, 1, 2, 11),
        (1, 2, 2, 13),
        (2, 1, 4, 14),
        (2, 1, 2, 77),
    ], ["_change_type", "update_preimage", "update_postimage", "external_id"])
    dfX = df1.filter(df1['_change_type'] == 'update_preimage')
    dfY = df1.filter(df1['_change_type'] == 'update_postimage')

    dfX.show()
    dfY.show()
    from pyspark.sql.functions import col, array, lit, when, array_remove

    # get conditions for all columns except id
    # conditions_ = [when(dfX[c]!=dfY[c], lit(c)).otherwise("") for c in dfX.columns if c != ['external_id', '_change_type']]

    select_expr = [
        col("external_id"),
        *[dfY[c] for c in dfY.columns if c != 'external_id'],
        # array_remove(array(*conditions_), "").alias("column_names")
    ]

    print(select_expr)

    dfX.join(dfY, "external_id").select(*select_expr).show()


def q_77628601():
    data = [("Alice", ["apple", "banana", "orange"], 5, 8, 3),
            ("Bob", ["apple"], 2, 9, 1)]
    schema = ["name", "fruits", "apple", "banana", "orange"]
    df = spark.createDataFrame(data, schema=schema)

    df.withColumn("array", array(col("apple"), col("banana"), col("orange"))) \
        .withColumn("new_col", arrays_zip("fruits", "array")).drop("array") \
        .withColumn("new_col", expr("filter(new_col, x-> x.fruits IS NOT NULL)")) \
        .withColumn("new_col", udf(dict, MapType(StringType(), IntegerType()))(col("new_col"))).show(truncate=False)


def q_77633394():
    df = spark.read.option("multiline", "true").json("./ressources/77633394.json")
    df = df.select("data.*", "datetime").select("occupancy.*", "datetime")
    df.show(truncate=False)

    # Register the UDF with Spark
    parse_struct_udf = udf(list, ArrayType(DoubleType()))

    # Use the UDF to transform the struct column into an array column
    df = df.select("datetime", lit("2023").alias("year"), "ltm", explode(parse_struct_udf("2023")).alias("values")) \
        .union(
        df.select("datetime", lit("2024").alias("year"), "ltm", explode(parse_struct_udf("2024")).alias("values")))
    df.show(truncate=False)


def q_77653807():
    persons = spark.createDataFrame([
        ("John", 25, 100483, "john@abc.com"),
        ("Sam", 49, 448900, "sam@abc.com"),
        ("Will", 63, None, "will@abc.com"),
        ("Robert", 20, 299011, None),
        ("Hill", 78, None, "hill@abc.com"),
    ], schema=["name", "age", "serial_no", "mail"])
    persons.show(truncate=False)
    people = spark.createDataFrame([
        ("John", 100483, "john@abc.com"),
        ("Sam", 448900, "sam@abc.com"),
        ("Will", 229809, "will@abc.com"),
        ("Robert", 299011, None),
        ("Hill", 567233, "hill@abc.com"),
    ], schema=["name", "s_no", "e_mail"])
    people.show(truncate=False)
    final_df = persons.join(people.select("s_no", "e_mail"), persons.mail == people.e_mail, "left") \
        .withColumn("serial_no", coalesce("serial_no", "s_no")).drop("s_no", "e_mail")
    final_df.show()


def q_77680762():
    df1 = spark.createDataFrame([
        ("1A", "3412asd", "value-1", 25, "UK"),
        ("2B", "2345tyu", "value-2", 34, "IE"),
        ("3C", "9800bvd", "value-3", 56, "US"),
        ("4E", "6789tvu", "value-4", 78, "IN")
    ], schema=["ID", "Company_Id", "value", "score", "region"])
    df1.show(truncate=False)
    df2 = spark.createDataFrame([("1A", "3412asd", "value-1", 25, "UK"),
                                 ("2B", "2345tyu", "value-5", 36, "IN"),
                                 ("3C", "9800bvd", "value-3", 56, "US")],
                                schema=["ID", "Company_Id", "value", "score", "region"])
    df2.show(truncate=False)
    df1.select("ID", "Company_Id").exceptAll(df2.select("ID", "Company_Id")).join(df1, on=["ID", "Company_Id"]).show(
        truncate=False)
    df1.select("ID", "Company_Id", "score").join(df2.select("ID", "Company_Id", "score"), on='ID', how='anti').show(
        truncate=False)


def q_77685797():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    # Create a Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Example DataFrames
    map_data = [('a', 'b', 'c', 'good'), ('a', 'a', '*', 'very good'),
                ('b', 'd', 'c', 'bad'), ('a', 'b', 'a', 'very good'),
                ('c', 'c', '*', 'very bad'), ('a', 'b', 'b', 'bad')]

    columns = ["col1", "col2", 'col3', 'result']

    mapping_table = spark.createDataFrame(map_data, columns)

    data = [[('a', 'b', 'c'), ('a', 'a', 'b'),
             ('c', 'c', 'a'), ('c', 'c', 'b'),
             ('a', 'b', 'b'), ('a', 'a', 'd')
             ]]

    columns = ["col1", "col2", 'col3']
    df = spark.createDataFrame(data, columns)
    mapping_table.show(truncate=False)
    df.show(truncate=False)


def q_77689901():
    df = spark.createDataFrame([("Hero", "Msg"), ("Hero", "MTC")], ["component", "offer"])

    df = df.withColumn("id", lit(1))
    w = Window().orderBy(col('id'))
    df = df.withColumn("row_num", row_number().over(w) - 1)
    df = df.groupBy("id").agg(
        map_from_arrays(collect_list("row_num"), collect_list("component")).alias("component"),
        map_from_arrays(collect_list("row_num"), collect_list("offer")).alias("offer"))
    df = df.withColumn("json", to_json(struct("component", "offer")))
    df.show(truncate=False)

    json_schema = StructType([
        StructField("component", StructType([
            StructField("0", StringType(), True),
            StructField("1", StringType(), True)
        ]), True),
        StructField("offer", StructType([
            StructField("0", StringType(), True),
            StructField("1", StringType(), True)
        ]), True)
    ])
    df = df.withColumn("json", from_json(col("json"), json_schema))

    df.show(truncate=False)
    df.printSchema()


def q_77702813():
    df = spark.createDataFrame([
        (18, "AAA", 2, 1, 5),
        (18, "BBB", 2, 2, 4),
        (16, "BBB", 2, 3, 3),
        (16, "CCC", 2, 4, 2),
        (17, "CCC", 1, 5, 1)
    ], ["id", "Type", "id_count", "Value1", "Value2"])

    filter_cond = (df['id_count'] == 2) & (df['Type'] != 'AAA')
    df1 = df.filter(filter_cond)
    df2 = df.filter(~filter_cond)
    df1.groupby("id").agg(
        concat_ws("+", collect_list("Type")).alias("Type"),
        first("id_count"),
        sum("Value1").alias("Value1"),
        sum("Value2").alias("Value2")
    ).union(df2).show()


def q_77769586():
    from pyspark.sql import SparkSession
    from os import system as ossystem
    import time

    spark = SparkSession.builder.master("local[4]").getOrCreate()
    txtfile = spark.read.text("./ressources/77769586.csv")
    print(txtfile.count())
    txtfile.explain()
    time.sleep(10)

    print(txtfile.count())
    txtfile.explain()


def q_77845666():
    df_assembled = spark.createDataFrame([
        (18, "AAA", 2, 1, 5),
        (18, "BBB", 2, 2, 4),
        (16, "BBB", 2, 3, 3),
        (16, "CCC", 2, 4, 2),
        (17, "CCC", 1, 5, 1)
    ], ["col1", "Type", "id_count", "Value1", "Value2"])

    outlier_df = spark.createDataFrame([
        (1, "ZZZ", 2),
        (2, "YYY", 2),
        (3, "XXX", 2),
        (4, "RRR", 2),
        (5, "SSS", 1)
    ], ["aa", "bb", "cc"])

    from pyspark.sql.functions import monotonically_increasing_id
    w = Window.orderBy(monotonically_increasing_id())
    df_assembled = df_assembled.withColumn("x", monotonically_increasing_id())
    df_assembled = df_assembled.withColumn("id", row_number().over(w))
    outlier_df = outlier_df.withColumn("id", row_number().over(w))
    result_df = df_assembled.join(outlier_df, df_assembled.id == outlier_df.id, how='inner')
    result_df.show()

if __name__ == "__main__":
    q_77845666()
