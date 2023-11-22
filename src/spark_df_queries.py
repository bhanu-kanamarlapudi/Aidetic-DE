import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, dayofweek, day, month, year, col, avg, stddev, when, desc, round, \
    countDistinct, corr


def question_1(df):
    """
    question : How does the Day of a Week affect the number of earthquakes?
    :param df:
    :return:
    """
    # Extract day of the week
    df = df.withColumn("DayOfWeek", dayofweek("Date"))
    # Filtering Null rows
    df = df.filter(col("DayOfWeek").isNotNull())
    # Total earthquakes per day
    print("Total earthquakes per day:")
    df.groupBy("DayOfWeek").count().orderBy("DayOfWeek").show()
    # Average magnitude per day
    print("Average magnitude per day:")
    df.groupBy("DayOfWeek").avg("Magnitude").orderBy("DayOfWeek").show(7)


def question_2(df, year_of_interest):
    """
    question : What is the relation between Day of the month and Number of earthquakes that happened in a year?
    :param df:
    :param year_of_interest:
    :return:
    """
    # DayOfMonth
    df = df.withColumn("DayOfMonth", day("Date"))
    df = df.withColumn("Year", year("Date"))
    # Filtering Null rows
    df = df.filter(col("DayOfMonth").isNotNull())
    df = df.filter(col("Year").isNotNull())

    # Filter data for a specific year
    filtered_data = df.filter(df["Year"] == year_of_interest)
    # Group by day of the month and count the number of earthquakes
    print(f"Relation between Day of Month and Number of earthquakes that happened in {year_of_interest}:")
    # Aggregate by year and day of month, count earthquakes
    (filtered_data.groupBy("Year", "DayOfMonth").agg(count("DayOfMonth").alias("Quakes")).orderBy("Year", "DayOfMonth")
     .show(31, False))


def question_3(df):
    """
    question : What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?
    :param df:
    :return:
    """
    # Extract month and year
    df = df.withColumn("Month", month("Date"))
    df = df.withColumn("Year", year("Date"))
    # Filter data for the desired years (1965 to 2016)
    filtered_data = df.filter((df["Year"] >= 1965) & (df["Year"] <= 2016))
    # Group by month and calculate the average frequency of earthquakes
    result = filtered_data.groupBy("Month").count().orderBy("Month")
    # Calculate the average frequency per month
    total_months = result.count()
    total_earthquakes = result.select("count").agg({"count": "sum"}).collect()[0][0]
    average_frequency = total_earthquakes / total_months
    # Print the average frequency
    print(f"Average frequency of earthquakes per month from 1965 to 2016: {average_frequency:.2f}")
    print("Average frequency of earthquakes by month:")
    result.show()


def question_4(df):
    """
    question : What is the relation between Year and Number of earthquakes that happened in that year?
    :param df:
    :return:
    """
    # Extract year from date column
    df = df.withColumn("Year", year(df.Date))
    # Filtering Null rows
    df = df.filter(col("Year").isNotNull())
    # Aggregate by year and count number of earthquakes
    print("Number of earthquakes per year:")
    df.groupBy("Year").count().orderBy("Year").show()

    # Alternative to show plot-ready data frame
    # year_counts = df.groupBy("Year").count()
    # year_counts.orderBy("Year").toPandas().set_index("Year").plot(kind='bar')


def question_5(df):
    """
    question : How has the earthquake magnitude on average been varied over the years?
    :param df:
    :return:
    """
    # Extract year
    df = df.withColumn("Year", year("Date"))
    # Filtering Null rows
    df = df.filter(col("Year").isNotNull())
    # Group by year and calculate the average magnitude
    print("Average Magnitude per year:")
    df.groupBy("Year").agg(avg("Magnitude").alias("AverageMagnitude")).orderBy("Year").show()


def question_6(df):
    """
    question: How does year impact the standard deviation of the earthquakes?
    """
    # Extract year
    df = df.withColumn("Year", year("Date"))
    # Filtering Null rows
    df = df.filter(col("Year").isNotNull())
    # Group by year and calculate the standard deviation of magnitudes
    print("Year impact on Standard deviation:")
    df.groupBy("Year").agg(stddev("Magnitude").alias("MagnitudeStdDev")).orderBy("Year").show()


def question_7(df):
    """
    question : Does geographic location have anything to do with earthquakes?
    :param df:
    approach: This analysis looks at:
        1) Number of distinct earthquake locations
        2) Mean and standard deviation of latitude and longitude
        3) Top 10 locations by number of recorded earthquakes

    If location is not related, we would expect to see random uniform distributions.
    Deviations from that suggest geographic correlations with seismic activity.
    """
    # Get number of distinct locations
    num_locations = df.select(countDistinct("Latitude", "Longitude")).first()[0]

    # Calculate statistics on latitude, longitude
    print("Statisics based on Latitude and Longitude:")
    df.select([
        avg("Latitude").alias("avg_lat"),
        stddev("Latitude").alias("std_lat"),
        avg("Longitude").alias("avg_lon"),
        stddev("Longitude").alias("std_lon")
    ]).show(10)

    # Group by rounded lat/lon and count earthquakes per location
    print("Number of earthquakes per location:")
    df.withColumn("LatRounded", round(col("Latitude"), 2)) \
        .withColumn("LonRounded", round(col("Longitude"), 2)) \
        .groupBy("LatRounded", "LonRounded") \
        .count() \
        .orderBy("count", ascending=False) \
        .show(10, truncate=False)

    print(f"Number of distinct locations: {num_locations}")


def question_8(df):
    """
    question : Where do earthquakes occur very frequently?
    :param df:
    :return:
    """
    # Round latitude and longitude
    df = df.withColumn('LatRounded', round(col('Latitude'), 2)).withColumn('LonRounded', round(col('Longitude'), 2))
    # Aggregate by lat/lon and count
    print("Number of earthquakes by Latitude and Longitude:")
    df.groupBy('LatRounded', 'LonRounded').count().orderBy(desc('count')).show()
    print("Number of earthquakes by Latitude:")
    df.groupBy(round(col('Latitude'), 2)).count().orderBy(desc('count')).show()


def question_9(df):
    """
    question : What is the relation between Magnitude, Magnitude Type , Status and Root Mean Square of the earthquakes?
    :param df:
    :return:
    """
    # Convert string columns to integer using when statement
    df = df.withColumn("Magnitude_Type_Num",
                       when(col("Magnitude Type") == "ML", 1)
                       .when(col("Magnitude Type") == "MW", 2)
                       .otherwise(0))

    df = df.withColumn("Status_Num",
                       when(col("Status") == "Reviewed", 1)
                       .when(col("Status") == "Automatic", 2)
                       .otherwise(0))

    # Calculate correlation between columns
    mag_rms_corr = df.select(corr("Magnitude", "Root Mean Square")).first()[0]
    mag_type_rms_corr = df.select(corr("Magnitude_Type_Num", "Root Mean Square")).first()[0]
    status_rms_corr = df.select(corr("Status_Num", "Root Mean Square")).first()[0]

    # Print correlations
    if mag_rms_corr is not None:
        print(f"Magnitude & RMS correlation: {mag_rms_corr:.3f}")
    else:
        print("No data to calculate Magnitude/RMS correlation")
    if mag_type_rms_corr is not None:
        print(f"Magnitude Type & RMS correlation: {mag_type_rms_corr:.3f}")
    else:
        print("No data to calculate Magnitude Type/RMS correlation")
    if status_rms_corr is not None:
        print(f"Status & RMS correlation: {status_rms_corr:.3f}")
    else:
        print("No data to calculate Status/RMS correlation")


def spark_sql_questions():
    spark = SparkSession.builder.appName("EarthQuakeAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    properties = {
        "url": "jdbc:mysql://localhost:3306/aidetic",
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "root",
        "password": "kaushikB!025",
        "table_name": "neic_earthquakes"
    }

    df = spark.read \
        .format("jdbc") \
        .option("url", properties["url"]) \
        .option("dbtable", properties["table_name"]) \
        .option("driver", properties["driver"]) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .load()

    parser = argparse.ArgumentParser()
    parser.add_argument('--all', type=str, help="All")
    parser.add_argument('--questionNum', type=int, help="Enter Question Number")
    parser.add_argument("--yearOfInterest", type=int, help="Please enter year of interest for Question-2 (Default = "
                                                           "2015)")
    args = parser.parse_args()

    if args.all is not None:
        print("Question 1:")
        question_1(df)
        print("Question 2:")
        if args.yearOfInterest:
            question_2(df, args.yearOfInterest)
        else:
            question_2(df, 2015)
        print("Question 3:")
        question_3(df)
        print("Question 4:")
        question_4(df)
        print("Question 5:")
        question_5(df)
        print("Question 6:")
        question_6(df)
        print("Question 7:")
        question_7(df)
        print("Question 8:")
        question_8(df)
        print("Question 9:")
        question_9(df)
    if args.all is None:
        if args.questionNum == 1:
            question_1(df)
        elif args.questionNum == 2:
            if args.yearOfInterest:
                question_2(df, args.yearOfInterest)
            else:
                question_2(df, 2015)
        elif args.questionNum == 3:
            question_3(df)
        elif args.questionNum == 4:
            question_4(df)
        elif args.questionNum == 5:
            question_5(df)
        elif args.questionNum == 6:
            question_6(df)
        elif args.questionNum == 7:
            question_7(df)
        elif args.questionNum == 8:
            question_8(df)
        elif args.questionNum == 9:
            question_9(df)


if __name__ == '__main__':
    spark_sql_questions()
