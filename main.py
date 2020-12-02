import pickle
import time

from multiprocessing import Pool
from pprint import pprint

from quandl_lib import QuandlBondDownloader
from RefinitivAPIClient import Refinitiv, Utility

# bond_list = list(set(QuandlBondDownloader("CHORD7", "BD").request_data("20191122")["20191122"]["cusip"].to_list()))
# bond_formatted = Utility.format_identifiers(bond_list)
instrument_list = pickle.load(open("instrument_list.p", "rb"))
instrument_list_of_lists = Utility.list_of_lists_converter(instrument_list, 5)
if __name__ == "__main__":
    pool = Pool(processes=8)
    start_time = time.time()
    results = pool.map(Refinitiv.request_data.request_price_history_data, instrument_list_of_lists)
    pool.close()
    pool.join()
    time_taken = time.time() - start_time
    print(f"The process took {str(time_taken)} seconds")
    pickle.dump(results, open("results.p", "wb"))
