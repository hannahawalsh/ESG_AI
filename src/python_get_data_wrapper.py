# Databricks notebook source
import gdelt
import os
import datetime
from collections import Counter
from pprint import pprint
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql.types import *
from functools import reduce
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import re
from itertools import product
import time

# COMMAND ----------

def get_russell_companies(n=100, sector=None):
    """
    Get the top n Russell 1000 companies from
    https://www.ishares.com/us/products/239707/ishares-russell-1000-etf
    Companies that have multiple classes of stock (e.g. Series A, Series C)
    are collapsed into one company name.
    """
    def rm_class(s):
        """ Remove the class, series, etc. from a stock. """
        class_types = ["CLASS", "SERIES", "REIT", "SHS"]
        share_class = [" ".join(x) + r"( |$)" for x in product(class_types,
                       ["A", "B", "C", "I"])]
        for sc in share_class:
          s = re.sub(sc, "", s).strip()
        return s

    # Download and parse the Russell 1000 companies
    url = ("https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/"
           "1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund")
    russell_1000 = pd.read_csv(url, skiprows=range(9), header=0,
                               usecols=["Name", "Sector"])
    russell_1000["Name"] = russell_1000.Name.astype(str).apply(rm_class)
    russell_1000.drop_duplicates(inplace=True)

    # Filter to sector
    if sector:
        russell_1000 = russell_1000[russell_1000.Sector == sector]
    return russell_1000.iloc[:n].Name.to_list()


def make_aliases(c):
    """
    Make a list of aliases for a given company using a few predefined
    rules. Try to encompass as many options as possible keeping in mind
    there will be aliases left out.
    """
    c = c.lower()

    # URLs (e.g. ".com")
    if len(c) > 3 and c[-4] == ".":
        a = c
        c = c.rsplit(".", 1)[0].replace(".", " ")
        aliases = set([c, a])
    else:
        aliases = set([c])

    # Single letter endings
    if len(c.split()[-1]) == 1:
        c = c.rsplit(" ", 1)[0]
        aliases.add(c)

    # Company legal endings
    endings = ["inc", "corp", "plc", "reit", "co", "cor", "group", "company",
               "trust", "energy", "international", "of america", "pharmaceuticals",
               "clas", "in", "nv", "sa", "re"]
    n_endings = 3  # Can have up to 3 of these endings
    for _ in range(n_endings):
        aliases.update([a.rsplit(" ", 1)[0] for a in aliases if
                        any([a.endswith(" " + e) for e in endings])])
        c = c.rsplit(" ", 1)[0] if any([c.endswith(" " + e) for e in endings]) else c

    # Alias any dashes and replace in company name
    aliases.update([a.replace("-", "") for a in aliases] +
                   [a.replace("-", " ") for a in aliases])
    c = c.replace("-", " ")

    # If '&' stands on its own, add alias of 'and'
    aliases.update([a.replace(" & ", " and ") for a in aliases])

    return {c: list(aliases)}


def get_company_alias_dict(n=100, sector=None):
    """
    Download the companies and loop through them to find their aliases
    """
    companies = get_russell_companies(n, sector)
    comp_dict = dict()
    for c in companies:
        comp_dict.update(make_aliases(c))
    return comp_dict

# COMMAND ----------

join_sdfs = lambda sdf_list: reduce(spark_DataFrame.union, sdf_list)

class Fields:
    """ Variables to use across many functions. """
    keep = ["DATE", "SourceCommonName", "DocumentIdentifier", "Themes",
            "Organizations", "V2Tone"]
    tone = ["Tone", "PositiveTone", "NegativeTone", "Polarity",
            "ActivityDensity", "SelfDensity", "WordCount"]
    base_url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
    n_orgs = 300
    organizations = get_company_alias_dict(n=n_orgs)



@udf(ArrayType(StringType(), True))
def simple_expand_spark(x):
    """ Expand a semicolon separated strint to a list (ignoring empties)"""
    if not x:
        return []
    return list(filter(None, x.split(";")))



@udf(MapType(StringType(), DoubleType()))
def tone_expand_spark(x):
    """ Expand the tone field. """
    if not x:
        return {t: None for t in Fields.tone}
    return {Fields.tone[i]: float(v) for i, v in enumerate(x.split(","))}



@udf(BooleanType())
def has_theme_spark(x, theme):
    """ Is the given theme included in any of the listed themes? """
    return any([theme in lst.split("_") for lst in x])


@udf(StringType())
def clean_organization(s):
    """ Standardize the organization names. """
    for k, v in Fields.organizations.items():
        if s.lower() in v:
            return k
    return s.lower()



class ReformatData:
    def reformat_spark_dataframe(self, sdf):
        """
        Given a spark data frame of the downloaded data, reformat it
        into human-readable fields.
        Add a few more fields for our purposes.
        """
        sdf = sdf.select(*Fields.keep)

        # Reformat existing columns
        sdf = (sdf.withColumnRenamed("DocumentIdentifier", "URL")
                  .withColumn("Themes", simple_expand_spark("Themes"))
                  .withColumn("Organizations", simple_expand_spark("Organizations"))
                  .withColumn("V2Tone", tone_expand_spark("V2Tone"))
               )

        # Create ESG columns & explode organization column
        sdf = (sdf.withColumn("E", has_theme_spark("Themes", F.lit("ENV")))
                  .withColumn("S", has_theme_spark("Themes", F.lit("UNGP")))
                  .withColumn("G", has_theme_spark("Themes", F.lit("ECON")))
                  .withColumn("Organization", F.explode("Organizations"))
                  .withColumn("Organization", clean_organization("organization"))
                  .filter(F.col("organization").isin(list(Fields.organizations.keys())))
               )

        # Expand tone columns
        exprs = [F.col("V2Tone").getItem(k).alias(k) for k in Fields.tone]

        sdf = sdf.select(*sdf.columns, *exprs).drop("V2Tone")
        return sdf


    def get_gdelt_table(self, date, gd):
        """
        Download the GDELT table as a pandas dataframe using the gdelt package.
        Return a spark data frame.
        """
        pdf = gd.Search(date, table="gkg", coverage=True, output="df")
        pdf["DATE"] = pd.to_datetime(pdf["DATE"], format="%Y%m%d%H%M%S")

        sdf = spark.createDataFrame(pdf)
        print("   * loaded *  ")
        return sdf


    def all_data(self, start_date, end_date, daily_save_dir, override_save=False):
        """
        For each date between start_date and end_date, either download
        and clean the data or load the pre-saved data. Save the day's data
        in case of future use (so it doesn't have to be downloaded and cleaned again)
        """
        print("Loading and cleaning all data")
        data_list = []
        gd = gdelt.gdelt(version=2)
        saved_dates = [x.name.replace("/", "") for x in dbutils.fs.ls(daily_save_dir)]

        # Download and format the daily data
        for i, date in enumerate(pd.date_range(start_date, end_date).astype(str)):
            if i % 7 == 1:
                # Prevent it hanging like it does sometimes
                time.sleep(60)

            try:
                print(date)
                file_path = os.path.join(daily_save_dir, date)
                today = datetime.datetime.now().date().strftime("%Y-%m-%d")

                if date in saved_dates and date != today and not override_save:
                    # Load file
                    print(f"  ** found file for {date} **")
                    df = spark.read.format("delta").option("header", "true").load(file_path)

                else:
                    # Create file
                    df = self.reformat_spark_dataframe(self.get_gdelt_table(date, gd))
                    df.write.format("delta").option("header", "true").mode("overwrite").save(file_path)
                data_list.append(df)
                del df, file_path, today
                spark.catalog.clearCache()

            except Exception as e:
              print(f"!!! Failed to complete {date}!")
              print("  ****   Reason:\n" + str(e) + "\n\n")

        return join_sdfs(data_list)

# COMMAND ----------

def save_as_csv(spark_file_path, save_file_path, sdf=None):
    """
    Save a spark dataframe as a csv by converting it
    to pandas (so it saves in one file).
    """
    @udf(StringType())
    def to_string(a):
        return str(a)

    # If not given data, load data
    if not sdf:
        filepath = spark_file_path[:-1] if spark_file_path.endswith("/") else spark_file_path
        sdf = (spark.read.format("delta")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(spark_file_path)
              )

    # Convert all pandas-incompatible fields to strings
    incompatible = [f for f, t in sdf.dtypes if t.startswith("array")
                    or t.startswith("map")]
    for col in incompatible:
        sdf = sdf.withColumn(col, to_string(col))

    # Transform to pandas and save
    pdf = sdf.toPandas()
    pandas_path = save_file_path.replace("dbfs:/", "/dbfs/")
    pdf.to_csv(pandas_path, index=False)
    print(f"csv file saved to {pandas_path}")

# COMMAND ----------

def create_and_save_data(start_date, end_date, save_csv=True):
    """
    """
    # Directories
    org_types = f"Russell_top_{Fields.n_orgs}"
    base_dir = f"dbfs:/mnt/esg/financial_report_data/GDELT_data_{org_types}"
    daily_save_dir = os.path.join(base_dir, "daily_data")
    range_save_dir = os.path.join(base_dir, f"{start_date}__to__{end_date}")

    dbutils.fs.mkdirs(base_dir)
    dbutils.fs.mkdirs(daily_save_dir)
    dbutils.fs.mkdirs(range_save_dir)

    # Download and reformat the data
    data = ReformatData().all_data(start_date, end_date, daily_save_dir)
    print(f"There are {data.count():,d} data points for {len(Fields.organizations)} "
          f"organizations from {start_date} to {end_date}")

    # Save the data
    print("Saving Data...")
    data_save_path = os.path.join(range_save_dir, "data_as_delta")
    data.write.format("delta").mode("overwrite").save(data_save_path)
    print(f"Saved to {data_save_path}")

    if save_csv:
        print("Saving as CSV...")
        save_cols = ["DATE", "SourceCommonName", "URL", "E", "S", "G",
                     "Organization", "Tone", "PositiveTone", "NegativeTone",
                     "Polarity", "ActivityDensity", "SelfDensity", "WordCount"]
        csv_file_path = os.path.join(range_save_dir, "data_as_csv.csv")
        save_as_csv(data_save_path, csv_file_path, sdf=data.select(*save_cols))

    return data

# COMMAND ----------
