import pandas as pd
import math
from tqdm import tqdm
import requests
from db_connection import DB


class Wrapper:
    def __init__(self):
        # Get API KEY
        with open("./API_KEY.txt", "r") as rf:
            api_key = rf.read().strip()
        self.c = Coda(api_key, "nADf8mVx-6")
        self.db = DB(host="localhost", port=27017, dbname="ExpenseTracker")

    def run(self):
        # self._update_transactions()
        self._update_statements()

    def _update_transactions(self) -> None:
        """
        Update Transaction Mongo Database with data from the Coda Website.
        :return:
        """
        def process_mongo(cols):
            df = self.db.show_table("Transaction")
            if not df.empty:
                df["UUID"] = df["UUID"].astype(float)
            else:
                df = pd.DataFrame(columns=cols)
            return df

        # Get Coda Data
        coda_df = self._process_transactions()
        # Get Mongo Data
        mongo_df = process_mongo(coda_df.columns)
        # Get Entries in Coda but not in Mongo
        diff_df = self._get_diff(mongo_df, coda_df)
        print(f"Number of Rows to Update: {diff_df.shape[0]}")
        # Update Mongo with delta data
        self.db.add_rows(diff_df.to_dict("records"), "Transaction")

    def _process_transactions(self) -> pd.DataFrame:
        df = self.c.exp_table("Historical Transactions")
        # Process dtypes
        df['Note'] = df["Note"].str.strip()
        df['Date'] = pd.to_datetime(df['Date'])
        df['Value'] = df['Value'].str.replace(r"[\$,]", "", regex=True).astype(float)
        df['Amount (SGD)'] = df['Amount (SGD)'].str.replace(r"[\$,]", "", regex=True).astype(float)
        # delete Columns
        del df['Display']

        return df

    def _update_statements(self) -> None:
        """
        probably a 1 time run. just to update app with historical data
        :return:
        """
        # TODO: import data from CODA
        pass

    @staticmethod
    def _get_diff(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """
        Get Delta of df1 and df2. We want those in df2 that is not in df1.
        :param df1: the main dataframe
        :param df2: we want the values that are in df2  but not in df1
        :return: dataframe
        """
        df = pd.merge(df1, df2, how="outer", on="UUID", indicator=True)
        df = df.loc[df["_merge"] == "right_only"]

        cols = list(df1.columns)
        df.rename(columns={x + "_y": x for x in cols}, inplace=True)

        return df[cols]


class Coda:
    def __init__(self, api_key, doc_id):
        self.uri = {
            "table": f"https://coda.io/apis/v1/docs/{doc_id}/tables",
            "row": f"https://coda.io/apis/v1/docs/{doc_id}/tables/" + "{table}/rows",
            "columns": f"https://coda.io/apis/v1/docs/{doc_id}/tables/" + "{table}/columns"
        }
        self.sess = self._init_sess(api_key)

    @staticmethod
    def _init_sess(key):
        s = requests.Session()
        s.headers.update({'Authorization': f'Bearer {key}'})
        return s

    def exp_table(self, tn: str) -> pd.DataFrame:
        """
        Export Data Table from Coda
        :param tn: String value of table name
        :return: DataFrame
        """
        det = self._get_t_details(tn)

        df_data = []
        total_pages, page_token = math.ceil(det['total'] / 200), None
        for _ in tqdm(range(0, total_pages), total=total_pages, desc=f"Extracting Table ({tn})"):
            param = {
                "useColumnNames": "true"
            }
            if page_token is not None:
                param['pageToken'] = page_token

            rsp = self.sess.get(self.uri['row'].format(table=tn), params=param)
            assert rsp.status_code == 200, rsp.text

            data = rsp.json()
            df_data += [x['values'] for x in data['items']]
            page_token = data.get('nextPageToken', None)
        df = pd.DataFrame(df_data)

        if "UUID" in df.columns:
            df['UUID'] = df['UUID'].astype(float)

        return df

    def _get_t_details(self, tn: str) -> dict:
        """
        Get Table Details like rowCount / id / etc.
        :param tn: String value of table name
        :return: dictionary of table details
        """
        rsp = self.sess.get(self.uri["table"] + f"/{tn}")
        assert rsp.status_code == 200, rsp.text

        data = rsp.json()
        return {"total": data['rowCount'], "id": data['id']}


if __name__ == '__main__':
    w = Wrapper()
    w.run()
