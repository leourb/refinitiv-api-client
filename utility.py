"""Utility Module containing utility classes"""

import pandas as pd
import pickle
import requests

from dateutil import parser


class Utility:
    """Static class to contain methods"""

    @staticmethod
    def write_to_pickle(input_file, filename):
        """
        Write to pickle a given file
        :param str input_file: file to read
        :param str filename: name of the file to write the content of the file read
        :return: a pickled object
        :rtype: pickle
        """
        return pickle.dump(input_file, open(filename, "wb"))

    @staticmethod
    def read_from_pickle(input_file):
        """
        Read from pickle a given file
        :param str input_file: name of the input file to read binary
        :return: a string with the file read
        :rtype: str
        """
        return pickle.load(open(input_file, "rb"))

    @staticmethod
    def to_pandas(response):
        """
        Transform the JSON output from a request in a Dataframe
        :param dict response: JSON response of the query
        :return: a DataFrame with the parsed JSON response
        :rtype: pd.DataFrame
        """
        return pd.DataFrame(response)

    @staticmethod
    def read_csv(filename):
        """
        Read the CSV filename and returns a Pandas DataFrame
        :param str filename: name of the file to read and process
        :return: a DataFrame with the parsed CSV file
        :rtype: pd.DataFrame
        """
        return pd.read_csv(filename, low_memory=False)
        
    @staticmethod
    def select_proxy(proxy):
        """
        Select the correct proxy to use for the session
        :param: dict proxy: dictionary with the proxy addresses
        :return: a dictionary with the correct proxy addresses
        :rtype: dict
        """
        try:
            requests.get("https://www.google.com", proxies=proxy, verify=False)
        except requests.exceptions.ProxyError:
            print("Proxy changed to ")
            proxy['http'] = ""
            proxy['https'] = ""
        return proxy

    @staticmethod
    def format_identifiers(ids):
        """
        Format the identifiers according to their layout
        :param: list or str ids: list of the identifiers to format
        :return: a list of tuples formatted ids
        :rtype: list
        """
        if type(ids) is str:
            input_ids = ids.split(",")
        elif type(ids) is list:
            input_ids = ids
        else:
            raise TypeError(f"The function accepts in input string or list types only. The type input is {type(ids)}")
        formatted_ids = list()
        for single_id in input_ids:
            if "." in single_id or "=" in single_id:
                formatted_ids.append((single_id, "Ric"))
            elif len(single_id) == 12:
                formatted_ids.append((single_id, "Isin"))
            elif len(single_id) == 9:
                formatted_ids.append((single_id, "Cusip"))
            elif len(single_id) == 7:
                formatted_ids.append((single_id, "Sedol"))
            elif len(single_id) < 7:
                formatted_ids.append((single_id, "Ticker"))
            else:
                print(f"No id_type recognized for {single_id}. Skipping it...")
        return formatted_ids

    @staticmethod
    def split_list(identifiers, chunks=100):
        """
        Split the identifiers list in different chunks. If chunks isn't specified, it will be of 100 elements each chunk
        :param list identifiers: list with all the identifiers
        :param int chunks: number of chunks to split the list
        :return: a dictionary of chunks
        :rtype: iterable
        """
        if len(identifiers) < chunks:
            return identifiers
        for i in range(0, len(identifiers), chunks):
            yield identifiers[i:i + chunks]

    @staticmethod
    def list_of_lists_converter(list_to_convert, chunks):
        """
        Split a list of records into a list of lists (useful for multiprocessing constructs)
        :param list list_to_convert: list of tuples to convert
        :param int chunks: number of elements in each sub-list
        :return: a list of lists with the split records
        :rtype: list
        """
        list_of_tuples = list(zip(*[iter(list_to_convert)] * chunks))
        results = list()
        for i in list_of_tuples:
            temp_list = list()
            for j in i:
                temp_list.append(j)
            results.append(temp_list)
        reminder = len(list_to_convert) % chunks
        if len(list_to_convert) % chunks == 0:
            return results
        else:
            results.append(list_to_convert[len(list_to_convert) - reminder:len(list_to_convert)])
            return results

    @staticmethod
    def get_valid_token():
        """
        Get a valid token
        :return: a string with the valid token value
        :rtype: str
        """
        return Utility.read_from_pickle("token.p")

    @staticmethod
    def split_string_in_n_chars_max(list_of_strings, n=50):
        """
        Split the input in a list of strings each of which has a max length of N chars
        :param list_of_strings: string or list of strings
        :type list_of_strings: str or list
        :param n: number of characters per each string
        :type: int
        :return: a list of strings each of which has a length of maximum N characters
        :rtype: list
        """
        strings_to_process = list_of_strings
        if type(strings_to_process) == str:
            strings_to_process = strings_to_process.split(",")
        results = list()
        max_length = n
        temp = str()
        for i in strings_to_process:
            if (len(temp) + len(i)) <= max_length:
                temp += i + ","
            else:
                results.append(temp[:len(temp) - 1])
                temp = i + ","
        return results

    @staticmethod
    def validate_and_format_date_objects(dictionary, data_type=True):
        """
        Validate a date object
        :param dict dictionary: dictionary with the information needed
        :param bool data_type: True for Numeric, False for Date
        :return: an error message or a validated object
        :rtype: dict or str
        """
        if list(dictionary.keys())[0] not in ["comparison", "range"]:
            return "Search must be performed either with \"comparison\" or \"range\""
        if "range" in dictionary:
            if "from" not in dictionary["range"] or "to" not in dictionary["range"]:
                return "When comparison is used as search type another dictionary with \"from\" and \"to\" properties" \
                       "needs to be created"
        if "comparison" in dictionary:
            if dictionary["comparison"]["operator"] not in ["Equals", "GreaterThan", "GreaterThanEquals",
                                                            "LessThan", "LessThanEquals", "NotEquals"]:
                return "A comparison search has to have one of the following operators: \"Equals\", \"GreaterThan\", " \
                       "\"GreaterThanEquals\", \"LessThan\", \"LessThanEquals\", \"NotEquals\""
        formatted_obj = dict()
        if "range" in dictionary:
            odata_type = "#ThomsonReuters.Dss.Api.Search.NumericRangeComparison" if data_type \
                else "#ThomsonReuters.Dss.Api.Search.DateRangeComparison"
            formatted_obj = {
                "@odata.type": odata_type,
                "From": str(parser.parse(dictionary["range"]["from"]).isoformat()) + "Z" if not data_type else
                dictionary["range"]["from"],
                "To": str(parser.parse(dictionary["range"]["to"]).isoformat()) + "Z" if not data_type else
                dictionary["range"]["to"]
            }
        if "comparison" in dictionary:
            odata_type = "#ThomsonReuters.Dss.Api.Search.NumericValueComparison" if data_type \
                else "#ThomsonReuters.Dss.Api.Search.DateValueComparison"
            formatted_obj = {
                "@odata.type": odata_type,
                "ComparisonOperator": dictionary["comparison"]["operator"],
                "Value": str(parser.parse(dictionary["comparison"]["value"]).isoformat()) + "Z" if not data_type else
                dictionary["comparison"]["value"]
            }
        return formatted_obj

    @staticmethod
    def transform_in_list_of_elements(string_or_list):
        """
        Check the type of the input and transform it in a list of strings
        :param str or list string_or_list: string or list input
        :return: a list of string elements
        :rtype: list
        """
        if type(string_or_list) == str:
            return string_or_list.split(",")
        elif string_or_list is None:
            return None
        return string_or_list
