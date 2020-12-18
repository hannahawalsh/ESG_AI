import pandas as pd
import numpy as np
import networkx as nx
import itertools

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

        edges = df_edge.SourceDest.values
        G.add_edges_from(edges)

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
