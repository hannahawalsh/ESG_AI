# Databricks notebook source
# DBTITLE 1,Imports
import pandas as pd
import numpy as np
import networkx as nx
from nodevectors import Node2Vec as NVVV
from sklearn.decomposition import PCA
import os
import itertools
import pickle

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")

# COMMAND ----------

# DBTITLE 1,Load Data from Delta Table
def load_data(save_path, file_name): 
  df = (spark.read.format("delta")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load(os.path.join(save_path, file_name))
           )
  return df.toPandas()


def filter_non_esg(df): 
    return df[(df['E']==True) | (df['S'] == True) | (df['G'] == True)]

# COMMAND ----------

class graph_creator:
    def __init__(self, df):
        self.df = df

    def create_graph(self):
        # Find Edges
        df_edge = pd.DataFrame(self.df.groupby("URL").Organization.apply(list)
                               ).reset_index()

        get_tpls = lambda r: (list(itertools.combinations(r, 2)) if
                              len(r) > 1 else None)
        df_edge["SourceDest"] = df_edge.Organization.apply(get_tpls)
        df_edge = df_edge.explode("SourceDest").dropna(subset=["SourceDest"])

        # Get Weights
        source_dest = pd.DataFrame(df_edge.SourceDest.tolist(),
                                   columns=["Source", "Dest"])
        sd_mapping = source_dest.groupby(["Source", "Dest"]).size()
        get_weight = lambda r: sd_mapping[r.Source, r.Dest]
        source_dest["weight"] = source_dest.apply(get_weight, axis=1)

        # Get
        self.organizations = set(source_dest.Source.unique()).union(
                             set(source_dest.Dest.unique()))
        self.G = nx.from_pandas_edgelist(source_dest, source="Source",
            target="Dest", edge_attr="weight", create_using=nx.Graph)
        return self.G

# COMMAND ----------

def get_embeddings(G, organizations):
    # Fit graph
    g2v = NVVV()
    g2v.fit(G)
    
    # Embeddings
    embeddings = g2v.model.wv.vectors
    pca = PCA(n_components=3)
    principalComponents = pca.fit_transform(embeddings)
    d_e = pd.DataFrame(principalComponents)
    d_e["company"] = organizations
    return d_e, g2v

# COMMAND ----------

def get_connections(organizations, topn=25):
    l = [g2v.model.wv.most_similar(org, topn=topn)
         for org in organizations]
    df_sim = pd.DataFrame(l, columns=[f"n{i}" for i in range(topn)])
    for col in df_sim.columns:
        new_cols = [f"{col}_rec", f"{col}_conf"]
        df_sim[new_cols] = pd.DataFrame(df_sim[col].tolist(), 
                                        index=df_sim.index)
    df_sim = df_sim.drop(columns=[f"n{i}" for i in range(topn)])
    df_sim.insert(0, "company", list(organizations))
    return df_sim

# COMMAND ----------

def make_embeddings_and_connections(start, end):
    base_dir = f"dbfs:/mnt/esg/financial_report_data/GDELT_data_Russell_top_300"
    save_dir = os.path.join(base_dir, f"{start}__to__{end}")
    csv_file = "data_as_csv.csv"

    # Load data
    print("Loading Data")
    df = pd.read_csv(os.path.join(save_dir, csv_file).replace("dbfs:/", "/dbfs/"))
    df = filter_non_esg(df)

    # Create graph
    print("Creating Graph")
    creator = graph_creator(df)
    G = creator.create_graph()
    organizations = list(creator.organizations)

    # Save graph as pkl
    fp = os.path.join(save_dir, "organization_graph.pkl").replace("dbfs:/", "/dbfs/")
    with open(fp, "wb") as f:
        pickle.dump(G, f)
        
    # Create embeddings
    print("Creating embeddings")
    emb_path = os.path.join(save_dir, "pca_embeddings.csv").replace("dbfs:/", "/dbfs/")
    d_e, g2v = get_embeddings(G, organizations)
    d_e.to_csv(emb_path, index=False)
    
    # Create connections
    print("Creating connections")
    df_sim = get_connections(organizations)
    sim_path = os.path.join(save_dir, "connections.csv")
    df_sim.to_csv(sim_path.replace("dbfs:/", "/dbfs/"))
    
    # Save organizations as delta
    conn_path = os.path.join(save_dir, "CONNECTIONS")
    conn_data = spark.createDataFrame(df_sim)
    conn_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(conn_path)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Create Graph and Add Nodes and Edges
# #Create the graph and add the notes 
# organizations = df2.Organization.unique().tolist()
# G = nx.Graph()
# for org in organizations:
#   G.add_node(org)

# COMMAND ----------

# #Get the Edges and add them to the graph
# #df_edge = pd.DataFrame(df2.groupby("URL").Organizations.first())
# df_edge = pd.DataFrame(df2.groupby("URL").Organization.apply(list))
# df_edge = df_edge.reset_index()
# #df_edge.head()

# def get_tuples(row): 
#   if len(row) > 1:
#     return list(itertools.combinations(row,2))
#   else: 
#     return None

# def get_i(row,i): 
#   return row[i]

# df_edge["SourceDest"] = df_edge.Organization.apply(lambda i: get_tuples(i))
# df_edge = df_edge.explode("SourceDest")
# df_edge = df_edge[~df_edge.SourceDest.isnull()]
# df_edge["Source"] = df_edge.SourceDest.apply(lambda i: get_i(i,0))
# df_edge["Dest"] = df_edge.SourceDest.apply(lambda i: get_i(i,1))
# df_edge.head(20)
# df_edge = df_edge[["Source","Dest"]]
# edges = [tuple(r) for r in df_edge.to_numpy()]
# G.add_edges_from(edges)

# COMMAND ----------

# map = df_edge.groupby(['Source', 'Dest']).size()
# map['abbvie']['amgen']
# def get_weight(row,map): 
#   return map[row.Source,row.Dest]
# df_edge["weight"] = df_edge[["Source","Dest"]].apply(lambda i: get_weight(i,map),axis=1)
# df_edge

# COMMAND ----------

# G_test = nx.from_pandas_edgelist(df_edge, 'Source', 'Dest',
#                             create_using=nx.DiGraph(), edge_attr='weight')
# G

# COMMAND ----------

# import pickle
# fp = "/dbfs/mnt/esg/G_1_month.pkl"
# with open(fp, 'wb') as f:
#     pickle.dump(G, f)

# COMMAND ----------

# g2v = NVVV()
# # way faster than other node2vec implementations
# # Graph edge weights are handled automatically
# g2v.fit(G)

# COMMAND ----------

# embeddings = g2v.model.wv.vectors
# embeddings.shape

# COMMAND ----------

# "visa" in em

# COMMAND ----------

# embeddings = g2v.model.wv.vectors
# from sklearn.decomposition import PCA
# pca = PCA(n_components=3)
# principalComponents = pca.fit_transform(embeddings)
# d_e = pd.DataFrame(principalComponents)
# d_e['company'] = organizations
# d_e.to_csv('/dbfs/mnt/esg/10_day_embeddings_pca.csv',index=None)

# COMMAND ----------

# def expand_tuple(row): 
#   return row[0],row[1]

# l = []
# for i in organizations:
#   sim = g2v.model.wv.most_similar(i,topn=25)
#   l.append(sim)
# c = [f"n{i}" for i in range(25)]
# df_sim = pd.DataFrame(l,columns=c)
# df_sim["company"] = organizations
# for i in c: 
#   cols = [i+"_rec",i+"_conf"]
#   df_sim[cols] = df_sim[i].apply(pd.Series)
# df_sim = df_sim.drop(c,axis=1)
# df_sim

# COMMAND ----------

# # Save the data
# spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
# spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")

# save_path = "dbfs:/mnt/esg/financial_report_data"
# dbutils.fs.mkdirs(save_path)

# file_name = "CONNECTIONS"
# data = spark.createDataFrame(df_sim)
# data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(save_path, file_name))
# print(f"Saved to {os.path.join(save_path, file_name)}")

# COMMAND ----------

# save_path = "dbfs:/mnt/esg/financial_report_data"
# file_name = "CONNECTIONS"
# df_sim = load_data(save_path,file_name)
# df_sim.head(10)

# COMMAND ----------

# df_sim.to_csv("/dbfs/mnt/esg/connectionsV2_10days.csv")

# COMMAND ----------

# dbutils.fs.ls("/mnt/esg/")

# COMMAND ----------

# import pickle  # python3

# fp = '/dbfs/mnt/esg/graph.pkl'
# # Dump graph
# with open(fp, 'wb') as f:
#     pickle.dump(G, f)

# COMMAND ----------

# G.edge_subgraph

# COMMAND ----------

# company = 'intercontinental exchange'
# edges = []
# for i in G.edges: 
#   if i[0] == (company): 
#     edges.append(i)
# G2 = G.edge_subgraph(edges)

# COMMAND ----------

# G.nodes

# COMMAND ----------


