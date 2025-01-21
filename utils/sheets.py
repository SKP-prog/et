import datetime

import gspread
import pandas as pd


class GSheet:
    def __init__(self, service_acc_path: str, doc_id: str, ws_name: str = "Sheet1", verbose=0):
        """
        Allows users to connect to Google sheets of an email account using Google Cloud API
        See Guide: https://www.youtube.com/watch?v=zCEJurLGFRk&t=110s
        :param service_acc_path: the service account json file path
        :param doc_id: the str id of the Google sheet document. can be found using the URL of document
        :param ws_name: Typically, it's called Sheet1
        """
        assert service_acc_path
        assert doc_id

        client = gspread.service_account(filename=service_acc_path)
        doc = client.open_by_key(doc_id)
        try:
            self.ws = doc.worksheet(ws_name)
        except gspread.exceptions.WorksheetNotFound as e:
            if verbose > 0:
                print(f"Unable to find worksheet with name: {ws_name}")
                print(f"System creating worksheet with name: {ws_name} ...")
            self.ws = doc.add_worksheet(title=ws_name, rows=1, cols=1)

    def get_in_df(self) -> pd.DataFrame:
        """
        Extract Work sheet from Google Sheets into a dataframe
        :return: pd.dataframe with sheet data
        """
        df = pd.DataFrame(self.ws.get_all_records())
        return df

    def update_sheet(self, df, export_to=None, datetime_type="datetime64[ns, UTC+08:00]"):
        """
        Copy dataframe and replace google sheet with dataframe.
        System will Back up the records in the Google sheets before replacing them
        :param df: Dataframe to overwrite google sheet with
        :param export_to: path to export the backup to
        :param datetime_type: specify the str that is used to identify datetime
        :return: Path to back up
        """
        if export_to is None:
            export_to = f"./sheets_backup_{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}.csv"
        # Backup to CSV
        prev_df = self.get_in_df()
        prev_df.to_csv(export_to, index=False)

        # Process All Datetime fields
        for col in df.columns:
            if df[col].dtype != datetime_type:
                continue
            df[col] = df[col].astype(str)

        # Overwrite Google sheets
        self.ws.clear()
        self.ws.update([df.columns.values.tolist()] + df.values.tolist())

        return export_to
