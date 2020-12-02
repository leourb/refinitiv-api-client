"""Static Data Module"""

import os
import requests
import urllib3

import sqlalchemy as sa

from RefinitivAPIClient.dss_requests import DSS
from RefinitivAPIClient.utility import Utility
from datetime import datetime, timedelta

urllib3.disable_warnings()

DSS_DATA = {
    'login': {
        'username': "",
        'password': ""
    },
    'token_url': 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken'
}

PROXY = {
    'http': "",
    'https': ""
}


class Datashelf:
    """Metadata Class only"""
    def __init__(self):
        """Populate the class with metadata"""
        self.dss = DSS_DATA
        self.proxy = Utility.select_proxy(PROXY)
        self.session_token = self._get_token()
        self.dss_headers = DSS.get('headers')
        self.dss_headers['Authorization'] = f'Token {self.session_token}'

    def _get_token(self):
        """
        Get the authorization token from DSS
        :return: a string with the token
        :rtype: str
        """
        if os.path.isfile("token.p"):
            time_threshold = datetime.fromtimestamp(os.path.getmtime("token.p")) + timedelta(hours=23)
            if datetime.now() < time_threshold:
                return Utility.read_from_pickle("token.p")
        dss_extraction_request_headers = dict()
        dss_extraction_body = dict()
        dss_extraction_body["Credentials"] = dict()
        dss_extraction_request_headers['Prefer'] = 'respond-async'
        dss_extraction_request_headers['Content-Type'] = 'application/json'
        dss_extraction_body["Credentials"]["Username"] = self.dss.get('login').get('username')
        dss_extraction_body["Credentials"]["Password"] = self.dss.get('login').get('password')
        response = requests.post(url=self.dss.get('token_url'), headers=dss_extraction_request_headers,
                                 json=dss_extraction_body, proxies=self.proxy, verify=False)
        if response.status_code != 200:
            print(f"There was an error getting the token. Error Code: {str(response.status_code)}")
        token = response.json()["value"]
        Utility.write_to_pickle(token, "token.p")
        return token


class PostgresDB:
    """Create a connection to a Postgres DB"""

    def __init__(self, db_conn=None):
        """
        Initialize the class establishing the connection with the Postgres endpoint or dbLeo as default
        :param db_conn: Postgres endpoint
        :type db_conn: str
        """
        self._db_conn = '' if not db_conn else db_conn
        self._engine = sa.create_engine(self._db_conn)
        self._connection = self._engine.raw_connection()
        self._cursor = self._connection.cursor()

    def get_db_conn(self):
        """
        Returns db_conn private var
        :return: a string with the db address where to upload the data
        :rtype: str
        """
        return self._db_conn

    def get_engine(self):
        """
        Returns engine private var
        :return: a string with the db address where to upload the data
        :rtype: sqlalchemy.engine.base.Engine
        """
        return self._engine

    def get_connection(self):
        """
        Returns connection private var
        :return: a string with the db address where to upload the data
        :rtype: sqlalchemy.pool.base._ConnectionFairy
        """
        return self._connection

    def get_cursor(self):
        """
        Returns cursor private var
        :return: a string with the db address where to upload the data
        :rtype: psycopg2.extensions.cursor
        """
        return self._cursor

    def _clean_schema_in_db(self, schema=None):
        """
        Clean all the tables in the public schema
        :param str schema: name of the schema to Drop
        :return: a message with the result of the operation
        :rtype: str
        """
        schema = schema if schema is not None else "public"
        user_input = input(f"Do you really want to delete schema {schema}? Please note that this operation will REMOVE"
                           f"all the data in schema {schema}! [Y] or [N]")
        if user_input.lower() in ["y", "yes"]:
            print(f"Removing schema {schema} as per user input")
            self.get_cursor().execute(f"""DROP SCHEMA {schema} CASCADE; CREATE SCHEMA {schema};""")
            self.get_connection().commit()
            self.get_connection().close()
            print(f"Schema {schema} removed and recreated in database")
        else:
            return "No action taken at this time."


DatashelfClass = Datashelf()
PostgresClass = PostgresDB()
