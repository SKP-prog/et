import math
import glob
import pathlib
import re
import requests
import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from db_connection import DB

DOC_ID = "nADf8mVx-6"
URI = {
    "table": "https://coda.io/apis/v1/docs/{docId}/tables",
    "row": "https://coda.io/apis/v1/docs/{docId}/tables/{table}/rows",
    "columns": "https://coda.io/apis/v1/docs/{docId}/tables/{table}/columns"
}
HEADER = {'Authorization': 'Bearer {api_key}'}


def main():
    set_api_key("./API_KEY.txt")
    # tns = get_all_table_names()  # Get all table names in doc

    # Get Table Data
    df = get_td("Historical Transactions")
    # Process dtypes
    df['Date'] = pd.to_datetime(df['Date'])
    df['Value'] = df['Value'].str.replace(r"[\$,]", "", regex=True).astype(float)
    df['Amount (SGD)'] = df['Amount (SGD)'].str.replace(r"[\$,]", "", regex=True).astype(float)

    del df['Display']

    df.to_csv(f"Trans-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv", index=False)

    # Update Database using the extracted data
    update_db(df)

    # Keep latest 7 only
    keep_latest(r"Trans-.*\.csv")


def update_db(df: pd.DataFrame):
    """
    Updates Mongo Db with latest data
    :param df: Dataframe to use to update mongo database
    :return:
    """
    db = DB(host="localhost", port=27017, dbname="ExpenseTracker")

    # get data in extracted df that is not in database df
    df1 = db.show_table("Transaction")
    if not df1.empty:
        new_df = get_diff(df1, df)
        print(f"Number of Rows to Update: {new_df.shape[0]}")
        if not new_df.empty:
            db.add_rows(new_df.to_dict("records"), "Transaction")
    else:
        db.add_rows(df.to_dict("records"), "Transaction")


def get_diff(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Get Delta of df1 and df2. We want those in df2 that is not in df1.
    :param df1: the main dataframe
    :param df2: we want the values that are in df2  but not in df1
    :return: dataframe
    """
    df = pd.merge(df1, df2, how="outer", on="UUID", indicator=True)
    df = df.loc[df["_merge"] == "right"]

    cols = list(df1.columns)
    df.rename(columns={x + "_y": x for x in cols}, inplace=True)
    return df[cols]

def get_all_table_names() -> list:
    """
    get a list of table names
    :return: a list of table names
    """
    res = requests.get(URI["table"].format(docId=DOC_ID), headers=HEADER)
    assert res.status_code == 200

    return [x['name'] for x in res.json()['items'] if x['tableType'] != 'view']


def get_td(tn: str) -> pd.DataFrame:
    """
    Get Table Data using Table Name. Get all rows
    :param tn: String value of table name
    :return: DataFrame
    """
    det = get_t_details(tn)

    df_data = []
    total_pages, page_token = math.ceil(det['total'] / 200), None
    for _ in tqdm(range(0, total_pages), total=total_pages, desc=f"Extracting Table ({tn})"):
        param = {
            "useColumnNames": "true"
        }
        if page_token is not None:
            param['pageToken'] = page_token

        rsp = requests.get(URI['row'].format(docId=DOC_ID, table=tn), headers=HEADER, params=param)
        assert rsp.status_code == 200

        data = rsp.json()
        df_data += [x['values'] for x in data['items']]
        page_token = data.get('nextPageToken', None)
    df = pd.DataFrame(df_data)

    return df


def get_t_details(tn: str) -> dict:
    """
    Get Table Details like rowCount / id / etc.
    :param tn: String value of table name
    :return: dictionary of table details
    """
    rsp = requests.get(URI["table"].format(docId=DOC_ID) + f"/{tn}", headers=HEADER)
    assert rsp.status_code == 200

    data = rsp.json()
    return {"total": data['rowCount'], "id": data['id']}


def set_api_key(path: str) -> None:
    """

    :param path: Path to API KEY text file. file should consist of the key only
    :return: nothing. It sets the HEADER global object
    """
    with open(path, "r") as rf:
        HEADER['Authorization'] = HEADER['Authorization'].format(api_key=rf.read().strip())


def keep_latest(regex: str, num_keep=7, root_path="./") -> None:
    """
    Keep the last 7 output files
    :param regex: regex for the files to keep track of
    :param num_keep: number of files to keep
    :param root_path: path to get the files from
    :return:
    """
    files = sorted(get_filepaths(root_path, regex), reverse=True)
    for x in files[num_keep:]:
        os.remove(x)


def get_filepaths(root_path: str, regex: str) -> list:
    """
    Get file paths that has the end file name using regex
    :param root_path: path to search through
    :param regex: the regex of files you want to extract
    :return: list of file paths from regex
    """
    root_path = pathlib.Path(root_path)

    for _, _, files in os.walk(root_path):
        for file in files:
            if re.match(regex, file):
                yield (root_path / file).absolute()
        break  # only iterate through the first layer


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
