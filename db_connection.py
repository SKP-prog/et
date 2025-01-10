from pymongo import MongoClient
import pandas as pd


class DB:
    def __init__(self, host=None, port=None, dbname=None):
        """
        Connection to Mongo DB
        :param str host: url to local/remote mongo db
        :param int port: Port number that the mongo db is using
        :param str dbname: Name of database you want to connect to
        """
        client = MongoClient(host, port)
        self.db = client[dbname]
        self.table = None

    def add_row(self, row_entry: dict):
        """
        add row to collection (TABLE)
        row_entry: dictionary for the table {column: value}
        """
        self.table.insert_one(row_entry)

    def add_rows(self, entries: list, table_name: str):
        """
        Add Multiple Rows to database

        :param list entries: A list of dictionary to insert to database
        :param str table_name: table name to update
        """
        assert hasattr(self.db, table_name), f"Unable to identify collection with name: {table_name}"
        self.db[table_name].insert_many(entries)

    def del_row(self, row_entry: dict):
        """
        remove row from collection (Table)
        row_entry: dictionary to filter a single entry to delete {column: value}
        """
        self.table.delete_one(row_entry)

    def show_table(self, tn: str, flt: dict = None):
        """
        Show Collection Data
        :param tn: Names of Table
        :param flt: dictionary filter
        :return: Dataframe
        """
        self.table = self.db[tn]

        if flt is None:
            flt = {}

        assert self.table is not None
        df = pd.DataFrame(list(self.table.find(flt)))
        if not df.empty:
            del df["_id"]
        return df
