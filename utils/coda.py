import requests
import pandas as pd
from tqdm import tqdm
import math


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
