import pymongo.errors
from pymongo import MongoClient
import pandas as pd
from bson.objectid import ObjectId


class DB:
    def __init__(self, host=None, port=None, dbname=None):
        """
        Connection to Mongo DB
        :param str host: url to local/remote mongo db
        :param int port: Port number that the mongo db is using
        :param str dbname: Name of database you want to connect to
        """
        try:
            client = MongoClient(host, port, serverSelectionTimeoutMS=1)
            client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as e:
            raise TimeoutError(f"Unable to connect to database, {host}:{port}")
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
        try:
            self.db.validate_collection(table_name)
        except pymongo.errors.OperationFailure:
            raise KeyError(f"Table {table_name} does not exist.")

        assert hasattr(self.db, table_name), f"Unable to identify collection with name: {table_name}"

        if len(entries) > 0:
            self.db[table_name].insert_many(entries)
        else:
            print("No Data to Add!")

    def del_row(self, row_entry: dict):
        """
        remove row from collection (Table)
        row_entry: dictionary to filter a single entry to delete {column: value}
        """
        self.table.delete_one(row_entry)

    def show_table(self, tn: str, flt: dict = None, get_id: bool = False):
        """
        Show Collection Data
        :param tn: Names of Table
        :param flt: dictionary filter
        :param get_id: Boolean value if True, returns ID else remove ID
        :return: Dataframe
        """
        self.table = self.db[tn]

        if flt is None:
            flt = {}

        assert self.table is not None
        df = pd.DataFrame(list(self.table.find(flt)))
        if not df.empty and not get_id:
            del df["_id"]
        return df

    @staticmethod
    def to_object_id(id_str: str = None, id_list: list = None):
        """
        Convert id strings to object id
        :param id_str:
        :param id_list:
        :return:
        """
        assert (id_str is None and id_list is not None) or (id_str is not None and id_list is None)
        if id_str is not None:
            return ObjectId(id_str)
        if id_list is not None:
            return map(ObjectId, id_list)

