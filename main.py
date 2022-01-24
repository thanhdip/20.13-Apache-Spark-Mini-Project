from pyspark.sql import SparkSession
from operator import add


# DATA
# incident_id INT
# incident_type STRING (I: initial sale, A: accident, R: repair)
# vin_number STRING
# make STRING (The brand of the car, only populated with incident type “I”)
# model STRING (The model of the car, only populated with incident type “I”)
# year STRING (The year of the car, only populated with incident type “I”)
# Incident_date DATE (Date of the incident occurrence)
# description STRING

column_names = [
    'incident_id',
    'incident_type',
    'vin_number',
    'make',
    'model',
    'year',
    'incident_date',
    'description'
]


def extract_vin_key_value(row):
    # Extract vin, make and year
    row = row.split(",")
    row_dict = dict(zip(column_names, row))
    if not row_dict["incident_type"] == "R":
        return (row_dict["vin_number"], (row_dict["make"], row_dict["year"]))
    else:
        return (""("", ""))


def populate_make(vin_reports):
    # Takes in list of reports of of a single vin and returns list
    # List is made up of the initial purchase reports and accidents.
    # Number of accidents is list length - 1.
    reports = list(vin_reports)
    make = ""
    year = ""
    ret = []

    # Go through list to see where make and model is
    for value in reports:
        if not value[0] == "":
            make = value[0]
            year = value[1]

    # Return list with number of accident reports
    if not reports[0][0] == "":
        for _ in range(1, len(reports)):
            ret.append((make, year))
    return ret


def extract_make_key_value(row):
    return (f"{row[0]}-{row[1]}", 1)


def main():
    # Read data
    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    sc = spark.sparkContext
    raw_rdd = sc.textFile("data.csv")

    # Step one: Filter out accident incidents with make and year

    vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
    enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

    # Step two: Count number of occurrence for accidents for the vehicle make and year

    make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
    final = make_kv.reduceByKey(add)

    final_df = spark.createDataFrame(final, ['make-year', 'count'])

    final_df.write.format("text").option(
        "header", "false").mode("overwrite").save("output.txt")


if __name__ == "__main__":
    main()
