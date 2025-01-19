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
        try:
            self.db = DB(host="localhost", port=27017, dbname="ExpenseTracker")
            print()
            print(f"Failed to Connect to Database")
            print()
        except TimeoutError as e:
            self.db = None

    def run(self):
        # self._update_transactions()
        # self._update_statements()
        data_list = self._export_transactions()

        # data list to csv
        for k, v in data_list.items():
            df = pd.DataFrame(v)
            df.to_csv(f"{k}.csv", index=False)

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
        # coda_df = self._process_transactions()
        # # Get Mongo Data
        # mongo_df = process_mongo(coda_df.columns)
        # # Get Entries in Coda but not in Mongo
        # diff_df = self._get_diff(mongo_df, coda_df)
        # print(f"Number of Rows to Update: {diff_df.shape[0]}")
        # # Update Mongo with delta data
        # self.db.add_rows(diff_df.to_dict("records"), "Transaction")

    def _export_transactions(self, is_historical=True) -> dict:
        """
        Get Coda Data tables and export all datatables into google drive
        :param is_historical: specify to export historical data table or current data table
        :return: dictionary of all the list and the transactions.
        """
        # Get Data Lists E.g. Accounts / Statements / Categories
        df = self.c.exp_table("Account")
        list_acc, list_acc_type = self._process_acc(df)  # Split Type into list
        df = self.c.exp_table("Categories")
        list_trans_cat, list_cat_cat, list_trans_type = self._process_cats(df)

        # Get Transactions
        df = self.c.exp_table("Historical Transactions" if is_historical else "Current Transactions")

        # Convert Currency to List Type
        list_currency = [{"id": i + 1, "label": x} for i, x in enumerate(sorted(df['Currency'].unique()))]

        # Process dtypes
        df['Note'] = df["Note"].str.strip()
        df['Date'] = pd.to_datetime(df['Date'])
        df['Value'] = df['Value'].str.replace(r"[\$,]", "", regex=True).astype(float)
        df['Amount (SGD)'] = df['Amount (SGD)'].str.replace(r"[\$,]", "", regex=True).astype(float)
        # Set Foreign Keys
        df['id'] = df.index + 1  # generate unique index
        df["trans_cat_id"] = df["Category"].apply(lambda x: [d for d in list_trans_cat if d["label"] == x][0]["id"])
        df["acc_id"] = df["Account"].apply(lambda x: [d for d in list_acc if d["label"] == x][0]["id"])
        df['curr_id'] = df["Currency"].apply(lambda x: [d for d in list_currency if d["label"] == x][0]['id'])
        df.rename(columns={
            "Note": "note",
            "Date": "trans_date",
            "Value": "raw_value",
            "Rate": "conv_rate",
        }, inplace=True)
        out_cols = ["id", "note", "trans_date", "raw_value", "conv_rate", "trans_cat_id", "acc_id", "curr_id"]
        return {
            "trans": df[out_cols].to_dict("records"),
            "acc": list_acc,
            "acc_type": list_acc_type,
            "curr": list_currency,
            "cat_cat": list_cat_cat,
            "trans_cat": list_trans_cat,
            "trans_type": list_trans_type
        }

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

    @staticmethod
    def _process_acc(df) -> tuple:
        """
        Convert Data frames into multiple report list. Account has Account Type and Name. We want
        Account Type to be its own set of data table list.
        :param df:
        :return: two list. 1 contains accounts and the other returns account Types
        """
        # Get all Labels for Accounts and convert to datatable
        acc_type_list = []
        for i, _type in enumerate(sorted(df['Type'].unique())):
            acc_type_list.append({
                "id": i + 1,
                "label": _type
            })

        # Set ID of acc_type_list to dataframe
        df['acc_type_id'] = df['Type'].apply(lambda x: [d for d in acc_type_list if d["label"] == x][0]["id"])
        df.rename(columns={
            "Name": "label"
        }, inplace=True)
        df["id"] = df.index + 1

        return df[["id", "label", "acc_type_id"]].to_dict("records"), acc_type_list

    @staticmethod
    def _process_cats(df) -> tuple:
        """
        Split Categories into multiple lists.
        :param df:
        :return: 3 list
            1 - Transaction Categories
            2 - Category Categories (Main Category of Transaction Categories)
            3 - transaction Types (E.g. Expense / Income)
        """
        # Convert Category to category_category (The category of the transaction Category)
        list_cat_cat = [{"id": i + 1, "label": x} for i, x in enumerate(sorted(df["Category"].unique()))]
        # Convert Type to list (E.g. Expense/Income)
        list_trans_type = [{"id": i + 1, "label": x} for i, x in enumerate(sorted(df['Type'].unique()))]

        # Set foreign key to transaction category
        df['id'] = df.index + 1
        df['type_id'] = df['Type'].apply(lambda x: [d for d in list_trans_type if d['label'] == x][0]['id'])
        df['cat_id'] = df['Category'].apply(lambda x: [d for d in list_cat_cat if d['label'] == x][0]['id'])
        df.rename(columns={
            "Name": "label",
            "Affect Cash Flow": "affect cash flow",
            "Notes": "notes"
        }, inplace=True)

        return (df[["id", "label", "notes", "affect cash flow", "type_id", "cat_id"]].to_dict("records"),
                list_cat_cat, list_trans_type)


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
