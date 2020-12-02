"""Main DSS Module"""

import json
import os
import re
import requests
import time
import zipfile

from simplejson import JSONDecodeError

from datetime import datetime, timedelta
from dateutil import parser
from pprint import pprint

from RefinitivAPIClient.datashelf import DatashelfClass, PostgresClass
from RefinitivAPIClient.dss_requests import DSS, JSON_REQUESTS
from RefinitivAPIClient.utility import Utility

# Reference on the API Schema at: https://hosted.datascopeapi.reuters.com/RestApi.Help/Home/RestApiProgrammingSdk
# Internal Doc:


class Refinitiv:
    """Handles all the requests doable with the REST API"""

    def __init__(self):
        """Initialize the class with the sub-classes"""
        self.list_fields = ListFields()
        self.request_data = Requests()
        self.securities_search = Searches()
        self.gui_operations = GUIOperations()
        self.operations = Operations()
        Refinitiv.set_max_results(1000000)

    @staticmethod
    def set_max_results(num):
        """
        Set the maximum numbers of results in the header
        :param int num: number of results to be returned
        :return: an header object
        :rtype: dict
        """
        DatashelfClass.dss_headers["Prefer"] = "odata.maxpagesize={}; respond-async".format(num)
        return DatashelfClass.dss_headers


class ListFields:
    """Group all the functions to send requests to list events"""

    @staticmethod
    def list_available_fields_for_price_history():
        """
        List all the fields for the Price History template
        :return: a JSON response with the list of the field available for timeseries
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('price_history')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()["value"]

    @staticmethod
    def list_available_fields_for_eod():
        """
        List all the fields for the EOD template
        :return: a JSON response with the list of the field available for EOD
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('eod')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()["value"]

    @staticmethod
    def list_available_fields_for_ca():
        """
        List all the fields for the Corporate Actions template
        :return: a JSON response with the list of the field available for CA
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('ca')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()["value"]

    @staticmethod
    def list_available_fields_for_ownership():
        """
        List all the fields for the Ownership template
        :return: a JSON response with the list of the field available for Ownership
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('ownership_data')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()["value"]

    @staticmethod
    def list_available_fields_for_tc():
        """
        List all the fields for the Terms and Conditions template
        :return: a JSON response with the list of the field available for T&C
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('tc')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()["value"]

    @staticmethod
    def list_available_fields_for_composite():
        """
        List all the fields for the Composite template
        :return: a JSON response with the list of the field available for Composite
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('composite')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()["value"]

    @staticmethod
    def list_available_templates_by_name(name):
        """
        List a template already created on DSS by name
        :param str name: Name of the template
        :return: a JSON response with the list of the field available for Composite
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('templates_by_name') % name
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()

    @staticmethod
    def list_available_instrument_lists(entity=False):
        """
        List all the Instrument Lists available on DSS
        :param bool entity: if True, it will try to access the Entity endpoint
        :return: a JSON response with the list of the field available for Composite
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('instrument_lists') if not entity else \
            DSS.get('endpoints').get('get_fields').get('entity_lists')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()

    @staticmethod
    def list_available_instrument_lists_by_name(name, entity=False):
        """
        List all the Instrument Lists available on DSS
        :param str name: Name of the instrument list
        :param bool entity: if True, it will try to access the Entity endpoint
        :return: a JSON response with the list of the field available for Composite
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('instrument_lists_by_name') % name if not entity else \
            DSS.get('endpoints').get('get_fields').get('entity_lists_by_name') % name
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()

    @staticmethod
    def list_available_instrument_within_instrument_list(list_id, entity=False):
        """
        List all the Instruments within a specific Instrument List on DSS
        :param str list_id: Id of the instrument list
        :param bool entity: if True, it will try to access the Entity endpoint
        :return: a JSON response with the list of the field available for Composite
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('instrument_lists_content') % list_id if not entity else \
            DSS.get('endpoints').get('get_fields').get('entity_lists_content') % list_id
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()

    @staticmethod
    def list_available_templates():
        """
        List all the Templates available on DSS
        :return: a JSON response with the list of the field available for Composite
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('templates')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()

    @staticmethod
    def list_all_extractions():
        """
        List all extractions available on DSS
        :return: a JSON response with the list of the field available for all the extractions
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('extractions')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()

    @staticmethod
    def list_completed_extractions():
        """
        List all the completed extractions available on DSS
        :return: a JSON response with the list of the field available for completed extractions
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('get_fields').get('completed_extractions')
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()


class Requests:
    """Group all the functions that request data"""

    @staticmethod
    def request_eod_pricing(sec_list):
        """
        Request EOD Pricing for the securities in the Tuple
        :param list sec_list: List of tuples with pair (identifier, identifierType)
        :return: a JSON object with the data queried from Refinitiv
        :rtype: dict or str
        """
        instr_identifiers = [{"Identifier": i[0], "IdentifierType": i[1]} for i in sec_list]
        eod_pricing = json.loads(open(os.path.join(JSON_REQUESTS, "eod_prices_request.json")).read())
        url = DSS.get('endpoints').get('extraction')
        eod_pricing["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = instr_identifiers
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=eod_pricing, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 202]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        elif response.status_code == 202:
            print("The query has been switched to async. Please find the link where to download it:")
            pprint(response.headers)
            return "Please run the request_async_extraction function to download the data"
        values = response.json()["Contents"]
        return values

    @staticmethod
    def request_price_history_data(sec_list, start_date=False, end_date=False):
        """
        Request Price History for the securities in the Tuple
        :param list or tuple sec_list: List of tuples with pair (identifier, identifierType)
        :param str start_date: Date from where to start the extraction, with format YYYYMMDD
        :param str end_date: If not specified, this will be equal to today's date
        :return: a JSON object with the data queried from Refinitiv
        :rtype: dict or str
        """
        start_date = start_date if start_date else str(datetime.now() - timedelta(days=1440))
        instr_identifiers = [{"Identifier": i[0], "IdentifierType": i[1]} for i in sec_list] if type(sec_list) is list \
            else [{"Identifier": sec_list[0], "IdentifierType": sec_list[1]}]
        price_history = json.loads(open(os.path.join(JSON_REQUESTS, "price_history_request.json")).read())
        url = DSS.get('endpoints').get('extraction')
        price_history["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = instr_identifiers
        price_history["ExtractionRequest"]["Condition"]["QueryStartDate"] = str(parser.parse(start_date).isoformat()
                                                                                ) + "Z"
        price_history["ExtractionRequest"]["Condition"]["QueryEndDate"] = datetime.now().isoformat() + "Z" if not \
            end_date else str(parser.parse(end_date).isoformat()) + "Z"
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=price_history, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 202]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        elif response.status_code == 202:
            print("The query has been switched to async. Please find the link where to download it:")
            pprint(response.headers)
            return "Please run the request_async_extraction function to download the data"
        values = response.json()["Contents"]
        return values

    @staticmethod
    def request_ca_events(sec_list, prev_days=None, next_days=None):
        """
        Request Corporate Actions Data Template
        :param list sec_list: List of tuples with pair (identifier, identifierType)
        :param int prev_days: Number of days to go back in time when pulling-up Corporate Action events
        :param int next_days: Number of days to go ahead in time when pulling-up Corporate Action events
        :return: a JSON object with the data queried from Refinitiv
        :rtype: dict or str
        """
        if prev_days and next_days:
            next_days = None
            print("You cannot have prev_days and next_days both populated. Setting next_days = None")
        if not prev_days and not next_days:
            next_days = 7
            print("You cannot have prev_days and next_days both None. Setting next_days = 7")
        instr_identifiers = [{"Identifier": i[0], "IdentifierType": i[1]} for i in sec_list]
        ca_events = json.loads(open(os.path.join(JSON_REQUESTS, "corporate_action_request.json")).read())
        url = DSS.get('endpoints').get('extraction')
        ca_events["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = instr_identifiers
        ca_events["ExtractionRequest"]["Condition"]["PreviousDays"] = prev_days if prev_days is not None else None
        ca_events["ExtractionRequest"]["Condition"]["NextDays"] = next_days if next_days is not None else None
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=ca_events, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 202]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        elif response.status_code == 202:
            print("The query has been switched to async. Please find the link where to download it:")
            pprint(response.headers)
            return "Please run the request_async_extraction function to download the data"
        values = response.json()["Contents"]
        return values

    @staticmethod
    def request_ownership_data(sec_list):
        """
        Request Ownership Data Template
        :param list sec_list: List of tuples with pair (identifier, identifierType)
        :return: a JSON object with the data queried from Refinitiv
        :rtype: dict or str
        """
        instr_identifiers = [{"Identifier": i[0], "IdentifierType": i[1]} for i in sec_list]
        ownership_data = json.loads(open(os.path.join(JSON_REQUESTS, "ownership_data_request.json")).read())
        url = DSS.get('endpoints').get('extraction')
        ownership_data["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = instr_identifiers
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=ownership_data, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 202]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        elif response.status_code == 202:
            print("The query has been switched to async. Please find the link where to download it:")
            pprint(response.headers)
            return "Please run the request_async_extraction function to download the data"
        values = response.json()
        return values

    @staticmethod
    def request_tc_data(sec_list):
        """
        Request Terms and Conditions Data Template
        :param list sec_list: List of tuples with pair (identifier, identifierType)
        :return: a JSON object with the data queried from Refinitiv
        :rtype: dict or str
        """
        instr_identifiers = [{"Identifier": i[0], "IdentifierType": i[1]} for i in sec_list]
        tc_data = json.loads(open(os.path.join(JSON_REQUESTS, "terms_and_conditions_request.json")).read())
        url = DSS.get('endpoints').get('extraction')
        tc_data["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = instr_identifiers
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=tc_data, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 202]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        elif response.status_code == 202:
            print("The query has been switched to async. Please find the link where to download it:")
            pprint(response.headers)
            return "Please run the request_async_extraction function to download the data"
        values = response.json()["Contents"]
        return values

    @staticmethod
    def request_composite_data(sec_list):
        """
        Request Composite Data Template
        :param list sec_list: List of tuples with pair (identifier, identifierType)
        :return: a JSON object with the data queried from Refinitiv
        :rtype: dict or str
        """
        instr_identifiers = [{"Identifier": i[0], "IdentifierType": i[1]} for i in sec_list]
        composite_data = json.loads(open(os.path.join(JSON_REQUESTS, "composite_request.json")).read())
        url = DSS.get('endpoints').get('extraction')
        composite_data["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"] = instr_identifiers
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=composite_data, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 202]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        elif response.status_code == 202:
            print("The query has been switched to async. Please find the link where to download it:")
            pprint(response.headers)
            return "Please run the request_async_extraction function to download the data"
        values = response.json()
        return values

    @staticmethod
    def request_async_extraction(extraction_id):
        """
        Post the async url along with the headers to get the data running asynchronously
        :param str extraction_id: extraction_id gotten from the header of the async request
        :return: a JSON object with the async'ed response
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('extraction_results') % extraction_id
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 202]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        try:
            values = response.json()["Contents"]
        except KeyError:
            values = response.json()
        except JSONDecodeError:
            values = response.text
        return values

    @staticmethod
    def request_components_of_chain_ric(ric):
        """
        Request the securities within a Chain RIC
        :param str ric: Chain RIC to be searched. The RIC could be in the full format starting with "0#" or not
        :return: a JSON object with the async'ed response
        :rtype: dict or str
        """
        chain_ric_request = json.loads(open(os.path.join(JSON_REQUESTS, "chain_ric_request.json")).read())
        url = DSS.get('endpoints').get('extraction')
        chain_ric_value = ric if ric[:2] == "0#" else "0#" + ric
        chain_ric_request["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"][0]["Identifier"] = \
            chain_ric_value
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=chain_ric_request, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 202]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        elif response.status_code == 202:
            print("The query has been switched to async. Please find the link where to download it:")
            pprint(response.headers)
            return "Please run the request_async_extraction function to download the data"
        values = response.json()
        return values


class Searches:
    """Group all the functions that perform Searches"""

    @staticmethod
    def instrument_search(identifier_type, identifier, preferred_return_type, instrument_type_groups=None):
        """
        Search for a given list of identifiers and returns a JSON response with the results
        :param str identifier_type: Type of the identifier(s) passed in identifiers_list
        :param str identifier: List of identifiers with the same type as identifier_type
        :param str preferred_return_type: specify what identifiers the response should return
        :param list instrument_type_groups: list of asset classes where to perform the research
        :return: a JSON response with the results (if any)
        :rtype: dict or str
        """
        search_request = json.loads(open(os.path.join(JSON_REQUESTS, "search_request.json")).read())
        url = DSS.get('endpoints').get('searches').get('generic_search')
        search_request["SearchRequest"]["IdentifierType"] = identifier_type
        search_request["SearchRequest"]["Identifier"] = identifier
        search_request["SearchRequest"]["PreferredIdentifierType"] = preferred_return_type
        instrument_type_groups = instrument_type_groups if not None else \
            ["CollatetizedMortgageObligations", "Commodities", "Equities", "FuturesAndOptions",
             "GovCorp", "MortgageBackedSecurities", "Money", "Municipals", "Funds"]
        search_request["SearchRequest"]["InstrumentTypeGroups"] = instrument_type_groups
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=search_request, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        values = response.json()
        return values

    @staticmethod
    def search_futures_and_options(id_type=None, pref_identifier=None, identifier=None, strike_from=None,
                                   strike_to=None, expiry=None, underlying=None, currency_codes=None,
                                   exchange_code=None, comparison_operator=None, put_call=None,
                                   futures_or_options="Futures", asset_status="Active"):
        """
        Search for Futures and Options securities in the Database
        :param str id_type: Which identifier type has been used to search
        :param str pref_identifier: Preferred identifier to be returned within the query
        :param str identifier: Value of the identifier with the same type of id_type
        :param float strike_from: Lower range of the strike search
        :param float strike_to: Upper range of the strike search
        :param str expiry: Expiration date in format YYYYMMDD
        :param str underlying: Underlying security of the futures or the option
        :param str currency_codes: List of the currency codes denominating the securities
        :param str exchange_code: List of exchange codes to be used to filter out results
        :param str comparison_operator: Set of values within LessThan, LessThanEquals, Equals, NotEquals,
        GreaterThanEquals, GreaterThan used to compare the value in expiry
        :param str put_call: either Call or Put or None
        :param str futures_or_options: choose if search for Futures, Options or Futures on Options
        :param str asset_status: choose between Active and Inactive
        :return: a JSON response with the securities found with the given parameters
        :rtype: dict or str
        """
        if futures_or_options not in ["Futures", "FuturesOnOptions", "Options"]:
            return "futures_or_option parameter accepts only values within: Futures, FuturesOnOptions, Options"
        search_fo_request = json.loads(open(os.path.join(JSON_REQUESTS, "search_futures_and_options.json")).read())
        url = DSS.get('endpoints').get('searches').get('search_future_options')
        search_fo_request["SearchRequest"]["FuturesAndOptionsType"] = futures_or_options
        search_fo_request["SearchRequest"]["AssetStatus"] = asset_status
        search_fo_request["SearchRequest"]["CurrencyCodes"] = currency_codes.split(",") if currency_codes is not None \
            else None
        search_fo_request["SearchRequest"]["ExchangeCodes"] = exchange_code.split(",") if exchange_code is not None \
            else None
        if strike_to:
            strike_range = {
                "@odata.type": "#ThomsonReuters.Dss.Api.Search.NumericRangeComparison",
                "From": strike_from,
                "To": strike_to
            }
            search_fo_request["SearchRequest"]["StrikePrice"] = strike_range
        if expiry:
            expiry_json = {
                "@odata.type": "#ThomsonReuters.Dss.Api.Search.DateValueComparison",
                "ComparisonOperator": comparison_operator if comparison_operator in [
                    "LessThan", "LessThanEquals", "Equals", "NotEquals", "GreaterThanEquals", "GreaterThan"
                ] else "GreaterThanEquals",
                "Value": str(parser.parse(expiry).isoformat()) + "Z"
            }
            search_fo_request["SearchRequest"]["ExpirationDate"] = expiry_json
        if put_call not in ["Put", "Call", None]:
            put_call = None
        search_fo_request["SearchRequest"]["PutCall"] = put_call
        search_fo_request["SearchRequest"]["IdentifierType"] = id_type
        search_fo_request["SearchRequest"]["Identifier"] = identifier
        search_fo_request["SearchRequest"]["PreferredIdentifierType"] = pref_identifier
        search_fo_request["SearchRequest"]["UnderlyingRic"] = underlying
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=search_fo_request, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        values = response.json()
        return values

    @staticmethod
    def search_equities(ticker=None, pref_id_type=None, id_type=None, identifier=None, org_id=None, exchange_codes=None,
                        description=None, company_name=None, currency_codes=None, asset_cat=None, gics_codes=None,
                        sub_type_codes=None):
        """
        Search for Equities securities within Refinitiv Database
        :param str ticker: Value of the ticker to be looked for
        :param str pref_id_type: Preferred identifier to be returned within the query
        :param str id_type: Which identifier type has been used to search
        :param str identifier: Value of the identifier with the same type of id_type
        :param int org_id: OrgId of the entity
        :param str exchange_codes: List with a set of valid exchange codes
        :param str description: String with the description of the company
        :param str company_name: Name of the company
        :param str currency_codes: List with a set of valid currency codes
        :param str asset_cat: List of asset types
        :param str gics_codes: List of GICS Codes
        :param str sub_type_codes: List of Sub-Type codes
        :return: A JSON valid response with all the results
        :rtype: dict or str or None
        """
        search_equity = json.loads(open(os.path.join(JSON_REQUESTS, "search_equity.json")).read())
        url = DSS.get('endpoints').get('searches').get('equity_search')
        search_equity["SearchRequest"]["CurrencyCodes"] = currency_codes.split(",") if currency_codes is not None \
            else None
        search_equity["SearchRequest"]["CompanyName"] = company_name if company_name is not None else None
        search_equity["SearchRequest"]["Description"] = description if description is not None else None
        search_equity["SearchRequest"]["ExchangeCodes"] = exchange_codes.split(",") if exchange_codes is not None \
            else None
        search_equity["SearchRequest"]["OrgId"] = org_id if org_id is not None else None
        search_equity["SearchRequest"]["Ticker"] = ticker if ticker is not None else None
        search_equity["SearchRequest"]["AssetCategoryCodes"] = asset_cat.split(",") if asset_cat is not None \
            else None
        search_equity["SearchRequest"]["GicsCodes"] = gics_codes.split(",") if gics_codes is not None else None
        search_equity["SearchRequest"]["SubTypeCodes"] = sub_type_codes.split(",") if sub_type_codes is not None \
            else None
        search_equity["SearchRequest"]["Identifier"] = identifier if identifier is not None else None
        search_equity["SearchRequest"]["IdentifierType"] = id_type if id_type is not None else None
        search_equity["SearchRequest"]["PreferredIdentifierType"] = pref_id_type if pref_id_type is not None else None
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=search_equity, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        values = response.json()
        return values

    @staticmethod
    def search_govcorp(currency_codes=False, country_code=False, org_id=False, ticker=False, id_type=False, ids=False,
                       pref_id=False, call=False, put=False, convertible=False, maturity=None, issued=None, coupon=None,
                       next_pay_date=None, group=None):
        """
        Search Datascope database for Govt/Corp securities with the given characteristics
        :param str currency_codes: currency codes for the bond denomination
        :param str country_code: 2-Digit ISO Country code
        :param str org_id: OrgID of the issuer
        :param str ticker: Ticker of the issuer
        :param str id_type: ID Type used to query the database
        :param str ids: list of ids given in input
        :param str pref_id: preferred ID to sort the results
        :param bool call: boolean flag to look for callable bonds
        :param bool put: boolean flag to look for putable bonds
        :param bool convertible: boolean flag to look for convertible bonds
        :param dict maturity: dictionary with the maturity properties to search
        :param dict issued: dictionary with the issue properties to search
        :param dict coupon: dictionary with the coupon properties to search
        :param dict next_pay_date: dictionary with the next pay date properties to search
        :param dict group: sets which group of bond will be returned by the search
        :return: a JSON with the list of the results (if any)
        :rtype: dict or str
        """
        govcorp_search = json.loads(open(os.path.join(JSON_REQUESTS, "search_govcorp.json")).read())
        url = DSS.get('endpoints').get('searches').get('govcorp_search')
        govcorp_search["SearchRequest"]["CurrencyCodes"] = currency_codes.split(",") if currency_codes else None
        govcorp_search["SearchRequest"]["CountryCode"] = country_code if country_code else None
        govcorp_search["SearchRequest"]["OrgId"] = org_id if org_id else None
        govcorp_search["SearchRequest"]["Ticker"] = ticker if ticker else None
        govcorp_search["SearchRequest"]["IdentifierType"] = id_type if id_type else None
        govcorp_search["SearchRequest"]["Identifier"] = ids if ids else None
        govcorp_search["SearchRequest"]["PreferredIdentifierType"] = pref_id if pref_id else None
        govcorp_search["SearchRequest"]["Group"] = group if group else None
        govcorp_search["SearchRequest"]["MaturityDate"] = Utility.validate_and_format_date_objects(maturity, False) if \
            maturity else None
        govcorp_search["SearchRequest"]["IssueDate"] = Utility.validate_and_format_date_objects(issued, False) if \
            issued else None
        govcorp_search["SearchRequest"]["NextPayDate"] = Utility.validate_and_format_date_objects(next_pay_date, False)\
            if next_pay_date else None
        govcorp_search["SearchRequest"]["Coupon"] = Utility.validate_and_format_date_objects(coupon) if coupon else None
        govcorp_search["SearchRequest"]["Callable"] = call
        govcorp_search["SearchRequest"]["Convertable"] = convertible
        govcorp_search["SearchRequest"]["Putable"] = put
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=govcorp_search, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        values = response.json()
        return values

    @staticmethod
    def search_otc_instruments(identifier_type, identifier):
        """
        Search for a given list of identifiers and returns a JSON response with the results
        :param str identifier_type: Type of the identifier(s) passed in identifiers_list
        :param str identifier: List of identifiers with the same type as identifier_type
        :return: a JSON response with the results (if any)
        :rtype: dict or str
        """
        search_otc = json.loads(open(os.path.join(JSON_REQUESTS, "search_otc_request.json")).read())
        url = DSS.get('endpoints').get('searches').get('otc_search')
        search_otc["SearchRequest"]["IdentifierType"] = identifier_type
        search_otc["SearchRequest"]["Identifier"] = identifier
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=search_otc, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        values = response.json()
        return values

    @staticmethod
    def search_mortgages(id_type, pref_id, agency_code=None, amortization_type=None, asset_statuses=None,
                         coupon_from=None, coupon_to=None, identifier=None, pool_number=None, pool_type_code=None,
                         sec_group=None, settle_month=None):
        """
        Search for MBS securities in DSS database
        :param str id_type: Identifier type to return
        :param str pref_id: Preferred identifier to return
        :param str agency_code: exadecimal code of the agency
        :param str amortization_type: amortization type of the security
        :param list asset_statuses: arr
        :param float coupon_from: coupon lower bound
        :param float coupon_to: coupon higher bound
        :param str identifier: identifier to search
        :param str pool_number: pool number of the security
        :param str pool_type_code: pool code
        :param str sec_group: security group of the security
        :param str settle_month: month of settlement
        :return: a JSON response with the results (if any)
        :rtype: dict or str
        """
        search_mortgage = json.loads(open(os.path.join(JSON_REQUESTS, "search_mortgage.json")).read())
        url = DSS.get('endpoints').get('searches').get('mortgage')
        search_mortgage["SearchRequest"]["IdentifierType"] = id_type
        search_mortgage["SearchRequest"]["PreferredIdentifierType"] = pref_id
        search_mortgage["SearchRequest"]["AgencyCode"] = agency_code if agency_code else None
        search_mortgage["SearchRequest"]["AmortizationType"] = amortization_type if amortization_type else None
        search_mortgage["SearchRequest"]["AssetStatuses"] = asset_statuses if asset_statuses else None
        if coupon_from and coupon_to:
            coupon_range = {
                "@odata.type": "#ThomsonReuters.Dss.Api.Search.NumericRangeComparison",
                "From": coupon_from,
                "To": coupon_to
            }
            search_mortgage["SearchRequest"]["CouponRate"] = coupon_range
        search_mortgage["SearchRequest"]["Identifier"] = identifier if identifier else None
        search_mortgage["SearchRequest"]["PoolNumber"] = pool_number if pool_number else None
        search_mortgage["SearchRequest"]["PoolTypeCode"] = pool_type_code if pool_type_code else None
        search_mortgage["SearchRequest"]["SecurityGroup"] = sec_group if sec_group else None
        search_mortgage["SearchRequest"]["SettleMonth"] = settle_month if settle_month else None
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=search_mortgage, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        values = response.json()
        return values

    @staticmethod
    def search_us_municipals(asset_statuses=None, call=True, coupon=None, identifier=None, id_type=None,
                             issuer_desc=None, maturity=None, pref_id=None, put=None, sinkable=None,
                             state_code=None):
        """
        Search for Municipal Bonds in Refinitiv
        :param list asset_statuses: list of statuses to search
        :param bool call: include callability or not
        :param dict coupon: dictionary with the coupon search details
        :param str identifier: identifier to search
        :param str id_type: specify the type of the identifier to search
        :param str issuer_desc: description of the issuer
        :param dict maturity: dictionary with the maturity search details
        :param str pref_id: specify which identifier to be returned
        :param bool put: include putability or not
        :param bool sinkable: include sinkability or not
        :param str state_code: two-digit US State code
        :return: a JSON with the results found or an error or informative string
        :rtype: dict or str
        """
        search_muni = json.loads(open(os.path.join(JSON_REQUESTS, "search_us_municipal.json")).read())
        url = DSS.get('endpoints').get('searches').get('us_municipals')
        search_muni["SearchRequest"]["IdentifierType"] = id_type
        search_muni["SearchRequest"]["PreferredIdentifierType"] = pref_id
        search_muni["SearchRequest"]["AssetStatuses"] = asset_statuses if asset_statuses else None
        search_muni["SearchRequest"]["Identifier"] = identifier if identifier else None
        search_muni["SearchRequest"]["CouponRate"] = Utility.validate_and_format_date_objects(coupon) if coupon \
            else None
        search_muni["SearchRequest"]["MaturityDate"] = Utility.validate_and_format_date_objects(maturity, False) \
            if maturity else None
        search_muni["SearchRequest"]["IssuerDescription"] = issuer_desc
        search_muni["SearchRequest"]["Callable"] = call
        search_muni["SearchRequest"]["Putable"] = put
        search_muni["SearchRequest"]["Sinkable"] = sinkable
        search_muni["SearchRequest"]["StateCode"] = state_code
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=search_muni, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        values = response.json()
        return values

    @staticmethod
    def search_loan(active_only=True, base_rate_codes=None, bid_price=None, company_name=None, currency_codes=None,
                    domicile_codes=None, facility_type_codes=None, identifier=None, id_type=None, industry_codes=None,
                    margin=None, maturity_date=None, pref_id=None, ticker=None):
        """
        Search Loans in Refinitiv
        :param bool active_only: shows only active securities if True
        :param str or list base_rate_codes: base rate associated with the loan(s)
        :param dict bid_price: dictionary with the Comparison or Range options
        :param str company_name: name of the company which issued the loan
        :param str or list currency_codes: codes of the currency(ies) of the loan(s)
        :param str or list domicile_codes: code where the loan is domiciled
        :param str or list facility_type_codes: the type of the loan
        :param str identifier: market code to identify the loan
        :param str id_type: type of the identifier provided
        :param str or list industry_codes: Refinitiv Business Code of the issuer of the loan
        :param dict margin: dictionary with the Comparison or Range options
        :param dict maturity_date: dictionary with the Comparison or Range options
        :param str pref_id: preferred id returned from the request
        :param str ticker: ticker of the loan
        :return: a JSON with the results found or an error or informative string
        :rtype: dict or str
        """
        search_loan = json.loads(open(os.path.join(JSON_REQUESTS, "search_loan.json")).read())
        url = DSS.get('endpoints').get('searches').get('loans')
        search_loan["SearchRequest"]["IdentifierType"] = id_type
        search_loan["SearchRequest"]["PreferredIdentifierType"] = pref_id
        search_loan["SearchRequest"]["ActiveOnly"] = "true" if active_only else "false"
        search_loan["SearchRequest"]["Identifier"] = identifier if identifier else None
        search_loan["SearchRequest"]["BidPrice"] = Utility.validate_and_format_date_objects(bid_price) if bid_price \
            else None
        search_loan["SearchRequest"]["MaturityDate"] = Utility.validate_and_format_date_objects(maturity_date, False) \
            if maturity_date else None
        search_loan["SearchRequest"]["Margin"] = Utility.validate_and_format_date_objects(margin) \
            if margin else None
        search_loan["SearchRequest"]["FacilityTypeCodes"] = Utility.transform_in_list_of_elements(facility_type_codes)
        search_loan["SearchRequest"]["BaseRateCodes"] = Utility.transform_in_list_of_elements(base_rate_codes)
        search_loan["SearchRequest"]["CurrencyCodes"] = Utility.transform_in_list_of_elements(currency_codes)
        search_loan["SearchRequest"]["IndustryCodes"] = Utility.transform_in_list_of_elements(industry_codes)
        search_loan["SearchRequest"]["DomicileCodes"] = Utility.transform_in_list_of_elements(domicile_codes)
        search_loan["SearchRequest"]["CompanyName"] = company_name
        search_loan["SearchRequest"]["Ticker"] = ticker
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=search_loan, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        values = response.json()
        return values

    @staticmethod
    def search_abs_cmo(asset_statuses=None, coupon=None, currency_codes=None, identifier=None, id_type=None, issue=None,
                       pref_id=None, security_group=None, series=None, tranche=None):
        """
        Search CMO and ABS in Refinitiv
        :param list asset_statuses: list with the statuses of the assets
        :param dict coupon: dictionary with the Comparison or Range options
        :param str or list currency_codes: currency codes of the securities
        :param str identifier: market code to identify the security
        :param str id_type: type of the identifier provided
        :param str issue: name of the issue
        :param str pref_id: preferred id returned in the response
        :param dict security_group: if passed will search only for the securities sub-types specified
        :param str series: series of the CMO/ABS
        :param str tranche: tranche of the CMO/ABS
        :return: a JSON with the results found or an error or informative string
        :rtype: dict or str
        """
        search_abs_cmo = json.loads(open(os.path.join(JSON_REQUESTS, "search_abs_cmo.json")).read())
        url = DSS.get('endpoints').get('searches').get('cmo_abs')
        security_group = security_group if security_group is not None else {
            "Agency": "true",
            "AssetBacked": "true",
            "Cdo": "true",
            "Cmbs": "true",
            "WholeLoan": "true",
            "SubGroupTypeCode": None
        }
        search_abs_cmo["SearchRequest"]["IdentifierType"] = id_type
        search_abs_cmo["SearchRequest"]["PreferredIdentifierType"] = pref_id
        search_abs_cmo["SearchRequest"]["AssetStatuses"] = asset_statuses
        search_abs_cmo["SearchRequest"]["Identifier"] = identifier if identifier else None
        search_abs_cmo["SearchRequest"]["CouponRate"] = Utility.validate_and_format_date_objects(coupon) if coupon \
            else None
        search_abs_cmo["SearchRequest"]["CurrencyCodes"] = Utility.transform_in_list_of_elements(currency_codes)
        search_abs_cmo["SearchRequest"]["Issue"] = issue
        search_abs_cmo["SearchRequest"]["Series"] = series
        search_abs_cmo["SearchRequest"]["Tranche"] = tranche
        search_abs_cmo["SearchRequest"]["SecurityGroup"] = security_group
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=search_abs_cmo, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        values = response.json()
        return values


class GUIOperations:
    """Group all the requests to perform operations in DSS GUI"""

    @staticmethod
    def create_instrument_list(name, entity=False):
        """
        Create an instrument list in the GUI
        :param str name: Name of the list to be created
        :param bool entity: if True, it will try to access the Entity endpoint
        :return: A JSON response with the ListId and other parameters
        :rtype: dict or str
        """
        username = DatashelfClass.dss.get('login').get('username')
        json_to_read = "gui_new_instrument_list.json" if not entity else "gui_new_entity_list.json"
        create_instr_list = json.loads(open(os.path.join(JSON_REQUESTS, json_to_read)).read())
        create_instr_list["Name"] = name
        url = DSS.get('endpoints').get('gui').get('create_instrument_list') if not entity else \
            DSS.get('endpoints').get('gui').get('create_entity_list')
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=create_instr_list, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 201]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        print(f"{name} list successfully created in GUI for account {username}")
        return response.json()

    @staticmethod
    def add_securities_to_instrument_list(list_of_securities, list_id, source=None, entity=False):
        """
        Add instruments to a existing list in GUI
        :param list list_of_securities: List of tuples with pair (identifier, identifierType)
        :param str list_id: Hexadecimal Id representing the List to update
        :param str source: Parameter to pass sources to get prices and volumes. For all sources, pass "*"
        :param bool entity: if True, it will try to access the Entity endpoint
        :return: a JSON response with the securities added and other representative information
        :rtype: dict or str
        """
        username = DatashelfClass.dss.get('login').get('username')
        add_instr_list = json.loads(open(os.path.join(JSON_REQUESTS, "gui_add_securities_to_list.json")).read())
        instr_identifiers = [{"Identifier": i[0], "IdentifierType": i[1], "Source": source} if not entity else
                             {"Identifier": i[0], "IdentifierType": i[1]} for i in list_of_securities]
        if entity:
            add_instr_list["IncludeParentAndUltimateParent"] = False
            add_instr_list["KeepDuplicates"] = False
        add_instr_list["Identifiers"] = instr_identifiers
        url = DSS.get('endpoints').get('gui').get('add_instruments_to_list') % list_id if not entity else \
            DSS.get('endpoints').get('gui').get('add_entity_to_list') % list_id
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=add_instr_list, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 201]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        print(f"Securities added to ListId {list_id} for account {username}")
        return response.json()

    @staticmethod
    def create_template(template, fields, name, exchanges=None, events=None,
                        days=30, start_date="20190101", end_date=None, look_back=None):
        """
        Add instruments to a existing list in GUI
        :param str template: Type of the Template to be created
        :param list fields: List of fields to add to the template
        :param str name: Name of the report to be created
        :param int days: Number of days for which return data for Corporate Actions
        :param str exchanges: List of Exchanges for which retrieve the IPO list
        :param str events: List of ISO15022 Corporate Action Events to look for
        :param str start_date: Start date to be used for Price History Pricing
        :param str end_date: End date to be used for the Price History Pricing
        :param int look_back: days of look_back to get prices
        :return: a JSON response with the securities added and other representative information
        :rtype: dict or str
        """
        if template not in ["EndOfDayPricingReportTemplate", "TermsAndConditionsReportTemplate",
                            "CorporateActionsStandardReportTemplate", "CorporateActionsIpoReportTemplate",
                            "CorporateActionsIsoReportTemplate", "PriceHistoryReportTemplate"]:
            return "Template MUST be one of the following templates: EndOfDayPricingReportTemplate\n" \
                   "TermsAndConditionsReportTemplate\nCorporateActionsStandardReportTemplate\n" \
                   "CorporateActionsIpoReportTemplate\nCorporateActionsIsoReportTemplate\nPriceHistoryReportTemplate\n"
        username = DatashelfClass.dss.get('login').get('username')
        create_template = json.loads(open(os.path.join(JSON_REQUESTS, "gui_create_template.json")).read())
        formatted_fields = [{"FieldName": i, "Format": None} for i in fields]
        create_template["ContentFields"] = formatted_fields
        create_template["@odata.type"] = create_template["@odata.type"] % template
        create_template["Name"] = name
        if template == "CorporateActionsIpoReportTemplate":
            create_template["Condition"] = dict()
            create_template["Condition"]["ReportDateRangeType"] = "Range"
            create_template["Condition"]["PreviousDays"] = days
            create_template["Condition"]["ExchangeTypes"] = exchanges.split(",")
            create_template["Condition"]["IncludeInstrumentsWithNoEvents"] = "false"
        elif template == "CorporateActionsStandardReportTemplate":
            create_template["Condition"] = dict()
            create_template["Condition"]["ReportDateRangeType"] = "Range"
            create_template["Condition"]["PreviousDays"] = days
            create_template["Condition"]["ExcludeDeletedEvents"] = "true"
            create_template["Condition"]["IncludeCapitalChangeEvents"] = "true"
            create_template["Condition"]["IncludeDividendEvents"] = "true"
            create_template["Condition"]["IncludeEarningsEvents"] = "true"
            create_template["Condition"]["IncludeMergersAndAcquisitionsEvents"] = "true"
            create_template["Condition"]["IncludeNominalValueEvents"] = "true"
            create_template["Condition"]["IncludePublicEquityOfferingsEvents"] = "true"
            create_template["Condition"]["IncludeSharesOutstandingEvents"] = "true"
            create_template["Condition"]["IncludeVotingRightsEvents"] = "true"
        elif template == "CorporateActionsIsoReportTemplate":
            create_template["OutputFormat"] = "IsoFormat"
            create_template["ContentFields"] = None
            create_template["Condition"] = dict()
            create_template["Condition"]["ReportIsoEvents"] = events.split(",")
            create_template["Condition"]["ReportDateRangeType"] = "Init"
            create_template["Condition"]["ExcludeNilPaidFromPaymentEvents"] = "true"
            create_template["Condition"]["GrossAmountOnlyForPaymentEvents"] = "true"
        elif template == "PriceHistoryReportTemplate":
            create_template["CompressionType"] = "Zip"
            create_template["Condition"] = dict()
            if look_back:
                dt_now = datetime.now()
                start_date = dt_now - timedelta(days=look_back)
                start_date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                start_date = str(parser.parse(start_date).isoformat()) + "Z"
            create_template["Condition"]["QueryStartDate"] = start_date
            create_template["Condition"]["QueryEndDate"] = str(parser.parse(end_date).isoformat()) + "Z" if end_date \
                else str(datetime.now().isoformat()) + "Z"
        url = DSS.get('endpoints').get('gui').get('create_template') % template + "s"
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=create_template, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 201]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        print(f"{template} successfully created for account {username} under name {name}")
        return response.json()

    @staticmethod
    def schedule_immediate_extraction(name, list_id, report_id):
        """
        Schedule an immediate extraction given a list_id and a report_id
        :param str name: Name of the extraction
        :param str list_id: Hexadecimal value with the id of the instrument list
        :param str report_id: Hexadecimal value with the id of the report
        :return: A JSON response with the information on the extraction
        :rtype: dict or str
        """
        username = DatashelfClass.dss.get('login').get('username')
        imm_extr = json.loads(open(os.path.join(JSON_REQUESTS, "gui_immediate_schedule.json")).read())
        url = DSS.get('endpoints').get('gui').get('schedules')
        imm_extr["Name"] = name
        imm_extr["ListId"] = list_id
        imm_extr["ReportTemplateId"] = report_id
        response = requests.post(url=url, headers=DatashelfClass.dss_headers,
                                 json=imm_extr, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 201]:
            return f"There was an error while getting the data. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        print(f"Extraction {name} scheduled for list_id {list_id} and report_id {report_id} for account {username}")
        return response.json()

    @staticmethod
    def check_scheduled_extraction(schedule_id):
        """
        Check the scheduled extraction given a schedule_id
        :param str schedule_id: Hexadecimal identifier of the schedule
        :return: a JSON response with the result of the schedule
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('gui').get('check_extraction') % schedule_id
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if not response.json()["value"]:
            attempt = 1
            flag = True
            print("\nExtraction is not completed yet. The script will try until it will be reported as complete.")
            while flag:
                response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy,
                                        verify=False)
                if not response.json()["value"]:
                    print(f"\tAttempt {attempt} - Extraction {schedule_id} not completed yet. "
                          f"Retrying again in 30 seconds.")
                    attempt += 1
                    time.sleep(30)
                else:
                    break
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()

    @staticmethod
    def get_extraction_report(report_extr_id):
        """
        Get the data and the notes information from a completed extraction
        :param str report_extr_id: Id of the extraction report
        :return: a JSON response with the result of the content of the extraction
        :rtype: dict or str
        """
        url = DSS.get('endpoints').get('gui').get('extraction_report') % report_extr_id
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.json()["value"]

    @staticmethod
    def get_extracted_data_or_notes(file_id):
        """
        Get the data from a completed extraction. Data can be either notes or data
        :param str file_id: filename of the extracted data
        :return: a string content with the extraction results
        :rtype: str or bytes
        """
        url = DSS.get('endpoints').get('gui').get('data_and_notes_extraction') % file_id
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code != 200:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.content

    @staticmethod
    def delete_extraction_schedule(schedule_id):
        """
        Delete a schedule given a schedule_id
        :param str schedule_id: Hexadecimal identifier of the schedule
        :return: a JSON response with the result of the schedule
        :rtype: str or bytes
        """
        url = DSS.get('endpoints').get('gui').get('delete_schedule') % schedule_id
        response = requests.delete(url=url, headers=DatashelfClass.dss_headers,
                                   proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 204]:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.content

    @staticmethod
    def delete_template(template_id):
        """
        Delete a schedule given a schedule_id
        :param str template_id: Hexadecimal identifier of the template
        :return: a JSON response with the result of the schedule
        :rtype: str or bytes
        """
        url = DSS.get('endpoints').get('gui').get('delete_template') % template_id
        response = requests.delete(url=url, headers=DatashelfClass.dss_headers,
                                   proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 204]:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.content

    @staticmethod
    def delete_instrument_list(instr_id, entity=False):
        """
        Delete a schedule given a schedule_id
        :param str instr_id: Hexadecimal identifier of the instrument list
        :param bool entity: if True, it will try to access the Entity endpoint
        :return: a JSON response with the result of the schedule
        :rtype: str or bytes
        """
        url = DSS.get('endpoints').get('gui').get('delete_instrument_list') % instr_id if not entity else \
            DSS.get('endpoints').get('gui').get('delete_entity_list') % instr_id
        response = requests.delete(url=url, headers=DatashelfClass.dss_headers,
                                   proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 204]:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.content

    @staticmethod
    def add_content_field(report_id, name_of_the_field):
        """
        Add a field to an existing template
        :param str report_id: Report ID of the template to modify
        :param str name_of_the_field: name of the field to add
        :return: a JSON with the outcome of the operation
        :rtype: str or dict
        """
        url = DSS.get('endpoints').get('gui').get('add_content') % report_id
        modify_template = json.loads(open(os.path.join(JSON_REQUESTS, "modify_template.json")).read())
        modify_template["ContentField"]["FieldName"] = name_of_the_field
        response = requests.post(url=url, headers=DatashelfClass.dss_headers, json=modify_template,
                                 proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 204]:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.content

    @staticmethod
    def remove_content_field(report_id, name_of_the_field):
        """
        Remove a field from an existing template
        :param str report_id: Report ID of the template to modify
        :param str name_of_the_field: name of the field to add
        :return: a JSON with the outcome of the operation
        :rtype: str or dict
        """
        url = DSS.get('endpoints').get('gui').get('remove_content') % report_id
        modify_template = json.loads(open(os.path.join(JSON_REQUESTS, "modify_template.json")).read())
        modify_template["ContentField"]["FieldName"] = name_of_the_field
        response = requests.post(url=url, headers=DatashelfClass.dss_headers, json=modify_template,
                                 proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 204]:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.content

    @staticmethod
    def get_all_instruments_in_instr_list(list_id, entity=False):
        """
        Return all the instruments within a given instrument list
        :param str list_id: ListId to lookup in DSS
        :param bool entity: if True, it will try to access the Entity endpoint
        :return: a JSON with all the instruments in the given instrument list
        """
        url = DSS.get('endpoints').get('gui').get('get_all_instruments') % list_id if not entity else \
            DSS.get('endpoints').get('gui').get('get_all_entities') % list_id
        response = requests.get(url=url, headers=DatashelfClass.dss_headers, proxies=DatashelfClass.proxy, verify=False)
        if response.status_code not in [200, 204]:
            return f"There was an error while processing this request. Error Code: {str(response.status_code)}: " \
                   f"{str(response.content)}"
        return response.content


class Operations:
    """This class includes all the most common operations performed with DSS"""

    @staticmethod
    def create_list_template_extract_data(securities, template_type, fields, start_date=None):
        """
        Compound operation to create an instrument list, add securities to it,
        create a template with the specified fields and run an extraction
        :param list or str securities: list or comma separated string of all the securities to pull up
        :param str template_type: template name
        :param list fields: list of fields to be included in the template
        :param str start_date: optional field that may be passed in input when creating PriceHistory templates
        :return: a JSON with the details of the extraction
        :rtype: dict
        """
        now = re.sub(r":", "", str(datetime.now().isoformat()))
        formatted_securities = Utility.format_identifiers(securities)
        list_id = GUIOperations.create_instrument_list(now + "_api_client_automatically_created_list")["ListId"]
        print(f"Instrument list successfully created with Id {list_id}")
        add_securities_to_instrument_list = GUIOperations.add_securities_to_instrument_list(formatted_securities,
                                                                                            list_id)
        print(f"Successfully added securities to the instrument list. "
              f"Showing the first 10 securities added: {formatted_securities[:10]}")
        pprint(add_securities_to_instrument_list)
        template_name = now + "_api_client_" + template_type + "_automatically_created"
        if template_type == "PriceHistoryReportTemplate" and start_date:
            template_id = GUIOperations.create_template(template_type, fields,
                                                        template_name, start_date=start_date)["ReportTemplateId"]
        else:
            template_id = GUIOperations.create_template(template_type, fields, template_name)["ReportTemplateId"]
        print(f"Template successfully created with Id {template_id}")
        extraction_name = now + "_api_client_immediate_extraction"
        run_extraction = GUIOperations.schedule_immediate_extraction(extraction_name, list_id, template_id)
        print(f"Immediate extraction successfully ran:")
        pprint(run_extraction)
        return run_extraction

    @staticmethod
    def create_list_extraction_with_existing_template(securities, template_name):
        """
        Compound operation to create an instrument list, add securities in it and use an existing template to run an
        immediate extraction
        :param list or str securities: list or comma separated string of all the securities to pull up
        :param str template_name: name of the existing template
        :return: a JSON with the details of the extraction
        :rtype: dict
        """
        now = re.sub(r":", "", str(datetime.now().isoformat()))
        formatted_securities = Utility.format_identifiers(securities)
        list_id = GUIOperations.create_instrument_list(now + "_api_client_automatically_created_list")["ListId"]
        print(f"Instrument list successfully created with Id {list_id}")
        add_securities_to_instrument_list = GUIOperations.add_securities_to_instrument_list(formatted_securities,
                                                                                            list_id)
        print(f"Successfully added securities to the instrument list. "
              f"Showing the first 10 securities added: {formatted_securities[:10]}")
        pprint(add_securities_to_instrument_list)
        template_id = ListFields.list_available_templates_by_name(template_name)["ReportTemplateId"]
        print(f"Template successfully found with Id {template_id}")
        extraction_name = now + "_api_client_immediate_extraction"
        run_extraction = GUIOperations.schedule_immediate_extraction(extraction_name, list_id, template_id)
        print(f"Immediate extraction successfully ran:")
        pprint(run_extraction)
        return run_extraction

    @staticmethod
    def create_template_extraction_with_existing_instrument_list(list_name, template_type, fields, start_date=None):
        """
        Compound request to create a template with the given fields and run an
        immediate schedule with an existing instrument list
        :param str list_name: name of the list to pull up
        :param str template_type: name of the type of template to use for the extraction
        :param list fields: list of fields to use to create the template for the extraction
        :param str start_date: optional field that may be passed in input when creating PriceHistory templates
        :return: a JSON with the details of the extraction
        :rtype: dict
        """
        now = re.sub(r":", "", str(datetime.now().isoformat()))
        list_id = ListFields.list_available_instrument_lists_by_name(list_name)["ListId"]
        print(f"Instrument list successfully found with Id {list_id}")
        template_name = now + "_api_client_" + template_type + "_automatically_created"
        if template_type == "PriceHistoryReportTemplate" and start_date:
            template_id = GUIOperations.create_template(template_type, fields,
                                                        template_name, start_date=start_date)["ReportTemplateId"]
        else:
            template_id = GUIOperations.create_template(template_type, fields, template_name)["ReportTemplateId"]
        print(f"Template successfully created with Id {template_id}")
        extraction_name = now + "_api_client_immediate_extraction"
        run_extraction = GUIOperations.schedule_immediate_extraction(extraction_name, list_id, template_id)
        print(f"Immediate extraction successfully ran:")
        pprint(run_extraction)
        return run_extraction

    @staticmethod
    def download_extraction_in_dataframe(report_extraction_id):
        """
        Download and eventually unzips locally the file from an extraction
        :param str report_extraction_id: extraction ID to be used to download the file
        :return: a DataFrame with the parsed CSV file
        :rtype: pandas.DataFrame
        """
        extraction_response = GUIOperations.get_extraction_report(report_extraction_id)
        filename = extraction_response[0]["ExtractedFileName"]
        file_id = extraction_response[0]["ExtractedFileId"]
        file_in_bytes = GUIOperations.get_extracted_data_or_notes(file_id)
        with open(filename, 'wb') as w:
            w.write(file_in_bytes)
        if filename.split(".")[-1] == "csv":
            return Utility.read_csv(filename)
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_filename = zip_ref.filename
            zipped_file = zip_ref.namelist()[0]
            zip_ref.extract(zipped_file)
        os.remove(zip_filename)
        return Utility.read_csv(zipped_file)

    @staticmethod
    def download_data_end_to_end(securities, template_type, fields, start_date=None):
        """
        Replicates the whole process from the creation of a temporary list, to the template till the extraction in a DF
        :param list or str securities: list or comma separated string of all the securities to pull up
        :param str template_type: template name
        :param list fields: list of fields to be included in the template
        :param str start_date: optional field that may be passed in input when creating PriceHistory templates
        :return: a DataFrame or a message of error
        :rtype: pandas.DataFrame or str
        """
        response_obj = Operations.create_list_template_extract_data(securities, template_type, fields, start_date)
        schedule_id = response_obj["ScheduleId"]
        report_id = GUIOperations.check_scheduled_extraction(schedule_id)["value"][0]["ReportExtractionId"]
        downloaded_file = Operations.download_extraction_in_dataframe(report_id)
        return downloaded_file

    @staticmethod
    def upload_results_to_db(dataframe, table_name="RefinitivResults", db_conn=None):
        """
        Reads the results from a Pandas DataFrame and uploads them in a Database.
        The default location is dbLeo/public/RefinitivResults
        :param pandas.DataFrame dataframe: Pandas DataFrame to feed the Database
        :param str table_name: name of the table where to store the results
        within the public space of the Postgres endpoint
        :param str db_conn: Postgres endpoint where to upload the data. Default 10.115.104.190:8025/dbLeo
        :return: a string with the results of the UPSERT in the Database
        :rtype: None
        """
        postgres = PostgresClass(db_conn)
        try:
            dataframe.to_sql(table_name, postgres.get_engine())
        except ValueError:
            postgres.get_cursor().execute(f"""DROP TABLE \"{table_name}\"""")
            postgres.get_connection().commit()
            postgres.get_connection().close()
            dataframe.to_sql(table_name, postgres.get_engine())
        print(f"Results have been correctly uploaded to {postgres.get_db_conn()}/{table_name}")
        return None
