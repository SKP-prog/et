import math
import requests
API_KEY = ""
DOC_ID = "nADf8mVx-6"
URI = {
    "table": "https://coda.io/apis/v1/docs/{docId}/tables",
    "row": "https://coda.io/apis/v1/docs/{docId}/tables/{table}/rows",
    "columns": "https://coda.io/apis/v1/docs/{docId}/tables/{table}/columns"
}
HEADER = {'Authorization': f'Bearer {API_KEY}'}

def main():
    tns = get_all_table_names()

def get_all_table_names() -> list:
    """
    get a list of table names
    :return: a list of table names
    """
    res = requests.get(URI["table"].format(docId=DOC_ID), headers=HEADER)
    assert res.status_code == 200

    return res.json()

def get_td():
    """
    Get Table Details
    :return: dictionary of table info
    """


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
