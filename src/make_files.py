# Databricks notebook source
# MAGIC %run ./python_get_data_wrapper

# COMMAND ----------

# MAGIC %run ./ESG_Financial_Segment

# COMMAND ----------

# MAGIC %run ./Node2Vec

# COMMAND ----------

base_dir = "dbfs:/mnt/esg/financial_report_data"

# COMMAND ----------

def make_files(base_dir, start_date, end_date):

    # Directories
    org_types = f"Russell_top_{Fields.n_orgs}"
    org_dir = os.path.join(base_dir, f"GDELT_data_{org_types}")
    date_string = f"{start_date}__to__{end_date}"
    date_dir = os.path.join(org_dir, date_string)
    dbutils.fs.mkdirs(org_dir)


    # Check if file has already been created, and if not, do so
    exists = False
    subdirs = [x.name for x in dbutils.fs.ls(org_dir)]
    if date_string + "/" in subdirs:
        subsubdirs = [x.name for x in dbutils.fs.ls(date_dir)]
        if "data_as_delta/" in subsubdirs:
            exists = True
    if exists:
        print("Data already created!")
    else:
        print("Creating Data")
        _ = create_and_save_data(start_date, end_date, save_csv=True)


    # Check if ESG data has already been created, and if not, do so
    if exists and "esg_data/" in subsubdirs:
        print("\n\nESG data already created!")
    else:
        print("\n\nMaking Tables")
        make_tables(start_date, end_date)


    # Create embeddings and connections files
    print("\n\nComputing Embeddings & Connections")
    make_embeddings_and_connections(start_date, end_date)



# After this function, in a new cell, run
# %sql
# OPTIMIZE delta.`saved path`
# where saved path has to actually be typed out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Days of Data
# MAGIC ----

# COMMAND ----------

start_date = "2020-12-01"
end_date = "2020-12-02"
make_files(base_dir, start_date, end_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/esg/financial_report_data/GDELT_data_Russell_top_300/2020-12-01__to__2020-12-02/data_as_delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 Days of Data

# COMMAND ----------

start_date = "2020-12-01"
end_date = "2020-12-10"
make_files(base_dir, start_date, end_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/esg/financial_report_data/GDELT_data_Russell_top_300/2020-12-01__to__2020-12-10/data_as_delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 Month of Data
# MAGIC ---

# COMMAND ----------

start_date = "2020-11-11"
end_date = "2020-12-12"
make_files(base_dir, start_date, end_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/esg/financial_report_data/GDELT_data_Russell_top_300/2020-11-11__to__2020-12-12/data_as_delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 Months of Data
# MAGIC ---

# COMMAND ----------

start_date = "2020-06-12"
end_date = "2020-12-12"
make_files(base_dir, start_date, end_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/esg/financial_report_data/GDELT_data_Russell_top_300/2020-06-12__to__2020-12-12/data_as_delta`

# COMMAND ----------
