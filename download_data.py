import pandas as pd
import numpy as np
import os

class Data:

    def paths(self, data_path):
        # Paths to data files
        self.connections = os.path.join(data_path, "connections.csv")
        self.data = os.path.join(data_path, "data_as_csv.csv")
        self.embeddings = os.path.join(data_path, "pca_embeddings.csv")
        esg_path = os.path.join(data_path, "ESG")
        self.avg_esg = os.path.join(esg_path, "average_esg_scores.csv")
        self.daily_esg = os.path.join(esg_path, "overall_daily_esg_scores.csv")
        self.e_score = os.path.join(esg_path, "daily_E_score.csv")
        self.s_score = os.path.join(esg_path, "daily_S_score.csv")
        self.g_score =os.path.join(esg_path, "daily_S_score.csv")



    def read(self, time_period="ten_days"):
        if time_period == "ten_days":
            data_path = os.path.join("Data", "dec1_to_dec10")
        elif time_period == "one_month":
            data_path = os.path.join("Data", "nov11_to_dec12")
        else:
            print("We don't have data for that")
            return
        self.paths(data_path)

        data = {"conn": pd.read_csv(self.connections),
                "data": pd.read_csv(self.data, parse_dates=["DATE"],
                                 infer_datetime_format=True),
                "embed": pd.read_csv(self.embeddings),
                "overall_score": pd.read_csv(self.daily_esg,
                                  index_col="date", parse_dates=["date"],
                                 infer_datetime_format=True),
                "E_score": pd.read_csv(self.e_score, parse_dates=["date"],
                                 infer_datetime_format=True, index_col="date"),
                "S_score": pd.read_csv(self.s_score, parse_dates=["date"],
                                 infer_datetime_format=True, index_col="date"),
                "G_score": pd.read_csv(self.g_score, parse_dates=["date"],
                                 infer_datetime_format=True, index_col="date"),
                "ESG": pd.read_csv(self.avg_esg),
                }
        # Dat column to date (not timestamp)
        data["data"]["DATE"] = data["data"]["DATE"].dt.date

        # Multiply tones by large number
        esg_tables = ["E_score", "S_score", "G_score", "overall_score", "ESG"]
        for t in esg_tables:
            num_cols = data[t].select_dtypes(include=["number"]).columns
            data[t][num_cols] *= 10000

        return data
