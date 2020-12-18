# Databricks notebook source
import os
from pprint import pprint
import pandas as pd
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import Window
import sys
import datetime

# COMMAND ----------

def show_df_rounded(df, places=4, rows=20):
    dtypes = {k: v for k, v in df.dtypes}
    date_cols = [k for k, v in dtypes.items() if v in ["date", "timestamp"]]
    str_cols = [k for k, v in dtypes.items() if v == "string"]
    int_cols = [k for k, v in dtypes.items() if "int" in v]
    
    show_cols = [F.date_format(c, "y-MM-dd").alias(c) if c in date_cols
                 else (F.col(c).alias(c) if c in str_cols
                 else (F.format_number(c, 0).alias(c) if c in int_cols
                 else (F.format_number(c, places).alias(c))))
                 for c in df.columns]
    show_cols = [c for c in show_cols]
    df.select(*show_cols).limit(rows).show()

# COMMAND ----------

def forward_fill(df, sort_col, fill_col):
    # Forward fill
    window = Window.orderBy(sort_col).rowsBetween(-sys.maxsize, 0)
    df = df.withColumn(fill_col, F.last(fill_col, ignorenulls=True).over(window))
    return df
  

def fill_dates(sdf, min_date, max_date, date_col="date"):
    """ Add all days in a date range then forward fill. """
    # Get all the dates between min and max and add to dataframe
    date_df = spark.createDataFrame([[d.date()] for d in 
                  pd.date_range(min_date, max_date)], [date_col])
    sdf = date_df.join(sdf, on=date_col, how="left").orderBy(F.col(date_col).asc())
    for col in [c for c in sdf.columns if c != date_col]:
        sdf = forward_fill(sdf, date_col, col)
    sdf = sdf.withColumn(date_col, F.col(date_col).cast("date"))
    return sdf


def daily_tone(filtered_df, name):
    """ """
    colname = f"{name.replace(' ', '_')}_tone"
    tone_df = (filtered_df.groupby(F.date_format("DATE", "y-MM-dd").alias("date"))
                          .agg((F.sum("Tone") / F.sum("WordCount")).alias(colname))
                          .select("date", f"{colname}")
                          .withColumn("date", F.to_date("date", format="y-MM-dd"))
                          .withColumn("date", F.col("date").cast("date"))
                          .orderBy(F.col("date").asc())
              )
    return tone_df

# COMMAND ----------

def subtract_cols(df, col1, col2):
    df = (df.withColumn(col1, df[f"{col1}"] - df[f"{col2}"])
            .withColumnRenamed(col1, col1.replace("_tone", "_diff")))
    return df


def get_col_avgs(df):
    exclude = [k for k, v in df.dtypes if v in ["date", "timestamp", "string"]]
    avgs = df.select([F.avg(c).alias(c) for c in df.columns if c not in exclude]).collect()[0]
    return {c: avgs[c] for c in df.columns if c not in exclude}

# COMMAND ----------

def make_tables(start_date, end_date):
    """
    """
    # Directories
    org_types = f"Russell_top_{Fields.n_orgs}"
    base_dir = f"dbfs:/mnt/esg/financial_report_data/GDELT_data_{org_types}"
    range_save_dir = os.path.join(base_dir, f"{start_date}__to__{end_date}")
    esg_dir = os.path.join(range_save_dir, "esg_scores")
    dbutils.fs.mkdirs(esg_dir)
    
    # Load data
    data_path = os.path.join(range_save_dir, "data_as_delta")
    try:
        data = (spark.read.format("delta")
                     .option("header", "true")
                     .option("inferSchema", "true")
                     .load(data_path)
               )
        print("Data Loaded!")
    except:
        print("Data for these dates hasn't been generated!!!")
        return

    # Get all organizations
    print("Finding all Organizations")
    organizations = [x.Organization for x in data.select(
                     "Organization").distinct().collect()]
    
    # Get the overall tone
    print("Calculating Tones Over Time")
    overall_tone = daily_tone(data, "industry")
    esg_tones = {L: daily_tone(data.filter(f"{L} == True"), "industry")
                 for L in ["E", "S", "G"]}
    
    # Loop through the organizations to get the average daily tone for each company
    pct_idxs = range(0, len(organizations), len(organizations) // 10)
    for i, org in enumerate(organizations):
        if i in pct_idxs:
            print(f"{pct_idxs.index(i) * 10}%")
        tone_label = f"{org.replace(' ', '_')}_tone"
        
        overall_org_df = data.filter(f"Organization == '{org}'")
        org_tone = daily_tone(overall_org_df, org)
        overall_tone = subtract_cols(overall_tone.join(org_tone, on="date", how="left"),
                                     tone_label, "industry_tone")
      
        for L, tdf in esg_tones.items():
            esg_org_df = overall_org_df.filter(f"{L} == True")
            esg_org_tone = daily_tone(esg_org_df, org)
            esg_tones[L] = subtract_cols(tdf.join(esg_org_tone, on="date", how="left"), 
                                         tone_label, "industry_tone")            
    del data   
    
    # Average to get overall scores
    print("Computing Overall Scores")
    scores = {}
    overall_scores = get_col_avgs(overall_tone)
    esg_scores = {L: get_col_avgs(tdf) for L, tdf in esg_tones.items()}

    for org in organizations:
        diff_label = f"{org.replace(' ', '_')}_diff"
        scores[org] = {L: tdf[diff_label] for L, tdf in esg_scores.items()}
        scores[org]["T"] = overall_scores[diff_label]
      
      
    # Save all the tables
    print("Saving Tables")  
    
    # Overall ESG
    print("    Daily Overall ESG")
    path = os.path.join(esg_dir, "overall_daily_esg_scores.csv").replace("dbfs:/", "/dbfs/")
    pd_df = overall_tone.toPandas().set_index("date").sort_index().asfreq(freq="D", method="ffill")
    pd_df.to_csv(path, index=True)
    
    # E, S, and G
    for L, tdf in esg_tones.items():
        print("    Daily " + L)
        path = os.path.join(esg_dir, f"daily_{L}_score.csv").replace("dbfs:/", "/dbfs/")
        pd_df = tdf.toPandas().set_index("date").sort_index().asfreq(freq="D", method="ffill")
        pd_df.to_csv(path, index=True)

    # Averaged scores
    print("    Average Scores")
    score_path = path = os.path.join(esg_dir, "average_esg_scores.csv").replace("dbfs:/", "/dbfs/")
    pd.DataFrame(scores).to_csv(score_path, index=True)
    print("DONE!")
