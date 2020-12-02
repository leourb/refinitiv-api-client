# Refinitiv API Client

## Abstract

This client has the advantage of standardizing some of the most frequent operations that are carried out by Refinitiv
Enterprise data users. The client aims to simply and optimize the workflow of Python users and to let them use 
Refinitiv data easily in their scripts

## Package Structure

The package is made by two main classes `dss.Refinitiv` and `datashelf.Utility`

### Utility Class

The `Utility` class aggregates a set of both private and public functions which are used by other classes (like the 
`Refinitiv` class) or can be used as a stand-alone operations.

One notable function in this class is the `format_identifiers`: it takes as input either a string or an array of strings
and it returns an array of tuples with the combination `(id, id_type)`.

This may be extremely useful when working with a set of un-tagged identifiers. It is able to recognize:

- Ric (not all Ric may be recognized as there are no public specs on how they're constructed)
- Isin
- Cusip
- Sedol
- Exchange Ticker (this may be incorrect as Exchange tickers follow different conventions)

#### Usage Example

```python
from RefinitivAPIClient.utility import Utility

identifiers = Utility.format_identifiers(["AAPL.O", "US5949181045", "30303M102"])
```

### Refinitiv Class

This is the main class of the package. For the sake of simplicity, it has 5 subclasses:

1. ListFields()
2. Requests()
3. Searches()
4. GUIOperations()
5. Operations()

#### ListFields

`ListFields()` main purposes are to:

- List all the fields from a specific template
- Available instrument lists
- Instruments within a specific instrument list
- Data extractions available

#### Requests

`Requests()` main purposes are to:
 
- Request EOD price data
- Request Time-Series data (using the PriceHistory template)
- Request Corporate Action events
- Request Ownership data
- Request Terms&Conditions data
- Request Composite data
- Resume an Async Request
- Request Components of a Chain RIC

#### Searches
 
`Searches()` main purposes are to:

- Generic Instrument Search
- Futures and Options Search
- Equity Search
- GovCorp Search
- OTC Instrument Search
- Mortgages Search
- Muni Search
- Loan Search
- ABS/CMO Search

#### GUIOperations

`GUIOperations()` main purposes are to:

- Create an Instrument List
- Add Securities to an Instrument List
- Create a Template
- Schedule an Immediate Extraction
- Check Scheduled Extractions
- Get an Extraction Report
- Get Extracted Data or Notes
- Delete an Extraction Schedule
- Delete a Template
- Delete an Instrument List
- Add a Field to an Existing Template
- Remove a Field to an Existing Template
- Get all the instruments in a given instrument list

#### Operations

`Operations()` is a _special_ class, as it uses the other classes in order to create complex operations which resemble a
workflow. For example, starting from a raw list of unformatted instruments (and using the `Utility` class) it can:

- Format the instruments
- Create an instrument list
- Add those instruments to an instrument list
- Create a template
- Run an extraction

...all in one go! In addition to that, it can also:

- Download an Extraction in a `pandas.DataFrame`
- Upload the results to a Database

## Contacts

This is only a summary of all the functions of this package. However, there is much more _under the hood_ which could
be used to create more complex or customized operations.

Feel free to contact or drop a comment for any feedback or suggestion you may have.
 
