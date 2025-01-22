import pandas as pd
from utils.db_connection import DB
from utils.coda import Coda
from utils.sheets import GSheet
import datetime
from tqdm import tqdm

"""
Goal of this script is to get data from CODA and throw into Google sheets.
we can use google sheets as a prototype database to store data. 
the Django back end will need to perform the checks for data integrity.
"""


class Wrapper:
    def __init__(self):
        # Get API KEY
        with open("./keys/API_KEY.txt", "r") as rf:
            api_key = rf.read().strip()
        self.c = Coda(api_key, "nADf8mVx-6")
        try:
            self.db = DB(host="localhost", port=27017, dbname="ExpenseTracker")
        except TimeoutError as e:
            print()
            print(f"Failed to Connect to Database")
            print()
            self.db = None

    def run(self):
        # self._update_transactions()
        # self._update_statements()
        data_list = self._export_transactions()

        # data list to csv
        num_rows_added = {}
        for k, v in tqdm(data_list.items(), total=len(data_list), desc="Uploading to Google Sheets"):
            # initialize Google Worksheets
            gs = GSheet(
                service_acc_path="./keys/GOOGLE_SERVICE_ACCOUNT.json",
                doc_id="1o1pU8GcUO9aJTE9fQ7-grJiUkzCEUmZiVqyTQy6PhU0",
                ws_name=k
            )
            df = pd.DataFrame(v)
            cur_date = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

            # Compare Google sheets db and local dataframe, import only the diff
            df_sheet = gs.get_in_df()
            if len(df_sheet.columns) == 0:
                gs.update_sheet(df, export_to=f"./backup/{k}_backup-{cur_date}.csv")
                num_rows_added[k] = df.shape[0]
                continue

            # Find the delta, those in df but not in df_sheet
            df_new = pd.merge(df_sheet, df, how="outer", on="id", indicator=True)
            df_new = df_new.loc[df_new['_merge'] == "right_only"]
            df_new.rename(columns={
                x + "_y": x for x in list(df.columns) if x != "id"
            }, inplace=True)
            df_new = df_new[df.columns]
            num_rows_added[k] = df_new.shape[0]

            gs.update_sheet(df_new, export_to=f"./backup/{k}_backup-{cur_date}.csv", is_append=True)

        for k, v in num_rows_added.items():
            print(f"Added {v} rows for {k}")

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
        list_acc, list_acc_type = self._process_acc()  # Get Account and Account Type
        list_trans_cat, list_cat_type, list_trans_type = self._process_cats()

        # Get Transactions
        df = self.c.exp_table("Historical Transactions" if is_historical else "Current Transactions")

        # Process dtypes
        df['Note'] = df["Note"].str.strip()
        df['Value'] = df['Value'].str.replace(r"[\$,]", "", regex=True).astype(float)
        # Set Foreign Keys
        df["trans_cat_id"] = df["Category"].apply(lambda x: [d for d in list_trans_cat if d["label"] == x][0]["id"])
        df["acc_id"] = df["Account"].apply(lambda x: [d for d in list_acc if d["label"] == x][0]["id"])
        df.rename(columns={
            "Note": "note",
            "Date": "trans_date",
            "Value": "raw_value",
            "Rate": "conv_rate",
            "Currency": "currency"
        }, inplace=True)
        out_cols = ["id", "note", "trans_date", "raw_value", "currency", "conv_rate", "trans_cat_id", "acc_id"]

        return {
            "transactions": df[out_cols].to_dict("records"),
            "account": list_acc,
            "account_type": list_acc_type,
            "category_type": list_cat_type,
            "transaction_category": list_trans_cat,
            "transaction_type": list_trans_type
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

    def _process_acc(self) -> tuple:
        """
        Extract Dataframe from Coda.
        Convert Data frames into multiple report list. Account has Account Type and Name. We want
        Account Type to be its own set of data table list.
        :return: two list. 1 contains accounts and the other returns account Types
        """
        acc_df = self.c.exp_table("Account")
        acc_type_df = self.c.exp_table("AccountType")

        # Set ID of acc_type_list to dataframe
        acc_type_df.rename(columns={"Type": "label"}, inplace=True)
        list_acc_type = acc_type_df.to_dict("records")
        acc_df["acc_type_id"] = acc_df["Type"].apply(lambda x: [d for d in list_acc_type if d["label"] == x][0]["id"])

        # rename acc df
        acc_df.rename(columns={"Name": "label"}, inplace=True)

        return acc_df[["id", "label", "acc_type_id"]].to_dict("records"), list_acc_type

    def _process_cats(self) -> tuple:
        """
        Get Category and all associated foreign tables for categories
        Convert to dataframe and export as record list
        :return: 3 list
            1 - Transaction Categories
            2 - Category Type (Main Category of Transaction Categories)
            3 - transaction Types (E.g. Expense / Income)
        """
        # Get data from CODA
        df_trans_cat = self.c.exp_table("Categories")  # Used to connect Transactions to a category
        df_trans_type = self.c.exp_table("TransactionType")
        df_cat_type = self.c.exp_table("CategoryType")  # Used as a general category for trans_cat

        # Rename Category Type
        df_cat_type.rename(columns={"Category": "label"}, inplace=True)
        list_cat_type = df_cat_type.to_dict("records")
        # Rename Transaction Type
        df_trans_type.rename(columns={"Type": "label"}, inplace=True)
        list_trans_type = df_trans_type.to_dict("records")

        # Set two type list as foreign keys
        df_trans_cat['trans_type_id'] = df_trans_cat["Type"].apply(
            lambda x: [d for d in list_trans_type if d["label"] == x][0]["id"])
        df_trans_cat['cat_type_id'] = df_trans_cat['Category'].apply(
            lambda x: [d for d in list_cat_type if d["label"] == x][0]["id"])

        # Raname Parent Dataframe
        df_trans_cat.rename(columns={
            "Name": "label",
            "Affect Cash Flow": "affect cash flow",
            "Notes": "notes"
        }, inplace=True)

        return (
            df_trans_cat[["id", "label", "notes", "affect cash flow",
                          "trans_type_id", "cat_type_id"]].to_dict("records"),
            list_cat_type, list_trans_type
        )


if __name__ == '__main__':
    w = Wrapper()
    w.run()
