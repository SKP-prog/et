import math

import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime

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
    del df['UUID']

    print(df)
    print(df.dtypes)
    df.to_csv(f"Trans-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv", index=False)


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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
