"""Module containing the standard requests"""

import os

ENDPOINT = "https://hosted.datascopeapi.reuters.com/RestApi/v1/"

JSON_REQUESTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_requests")

DSS = {
    'headers': {
        'Prefer': 'odata.maxpagesize={}; respond-async',
        'Content-Type': 'application/json',
        'Authorization': None
    },
    'endpoints': {
        'extraction': f"{ENDPOINT}Extractions/ExtractWithNotes",
        'extraction_results': f"{ENDPOINT}Extractions/ExtractWithNotesResult(ExtractionId='%s')",
        'get_fields': {
            'price_history': f"{ENDPOINT}Extractions/GetValidContentFieldTypes(ReportTemplateType=ThomsonReuters.Dss."
                             f"Api.Extractions.ReportTemplates.ReportTemplateTypes'PriceHistory')",
            'eod': f"{ENDPOINT}Extractions/GetValidContentFieldTypes(ReportTemplateType=ThomsonReuters.Dss.Api."
                   f"Extractions.ReportTemplates.ReportTemplateTypes'EndOfDayPricing')",
            'ca': f"{ENDPOINT}Extractions/GetValidContentFieldTypes(ReportTemplateType=ThomsonReuters.Dss.Api."
                  f"Extractions.ReportTemplates.ReportTemplateTypes'CorporateActions')",
            'ownership_data': f"{ENDPOINT}Extractions/GetValidContentFieldTypes(ReportTemplateType=ThomsonReuters.Dss."
                              f"Api.Extractions.ReportTemplates.ReportTemplateTypes'Owners')",
            'tc': f"{ENDPOINT}Extractions/GetValidContentFieldTypes(ReportTemplateType=ThomsonReuters.Dss.Api."
                  f"Extractions.ReportTemplates.ReportTemplateTypes'TermsAndConditions')",
            'composite': f"{ENDPOINT}Extractions/GetValidContentFieldTypes(ReportTemplateType=ThomsonReuters.Dss.Api."
                         f"Extractions.ReportTemplates.ReportTemplateTypes'Composite')",
            'templates_by_name': f"{ENDPOINT}Extractions/ReportTemplateGetByName(Name='%s')",
            'instrument_lists': f"{ENDPOINT}Extractions/InstrumentLists",
            'entity_lists': f"{ENDPOINT}Extractions/EntityLists",
            'instrument_lists_by_name': f"{ENDPOINT}Extractions/InstrumentListGetByName(ListName='%s')",
            'entity_lists_by_name': f"{ENDPOINT}Extractions/EntityListGetByName(ListName='%s')",
            'instrument_lists_content': f"{ENDPOINT}Extractions/InstrumentLists('%s')/ThomsonReuters.Dss.Api."
                                        f"Extractions.InstrumentListGetAllInstruments",
            'entity_lists_content': f"{ENDPOINT}Extractions/EntityLists('%s')/ThomsonReuters.Dss.Api."
                                    f"Extractions.EntityListGetAllEntities",
            'templates': f"{ENDPOINT}Extractions/ReportTemplates",
            'extractions': f"{ENDPOINT}Extractions/ExtractedFiles",
            'completed_extractions': f"{ENDPOINT}Extractions/ReportExtractionGetCompleted"
        },
        'searches': {
            'generic_search': f"{ENDPOINT}Search/InstrumentSearch",
            'search_future_options': f"{ENDPOINT}Search/FuturesAndOptionsSearch",
            'equity_search': f"{ENDPOINT}Search/EquitySearch",
            'govcorp_search': f"{ENDPOINT}Search/GovCorpSearch",
            'otc_search': f"{ENDPOINT}Search/OtcsSearch",
            'mortgage': f"{ENDPOINT}Search/MortgageSearch",
            'us_municipals': f"{ENDPOINT}Search/UsMunicipalSearch",
            'loans': f"{ENDPOINT}Search/LoanSearch",
            'cmo_abs': f"{ENDPOINT}Search/CmoAbsSearch"
        },
        'gui': {
            'create_instrument_list': f"{ENDPOINT}Extractions/InstrumentLists",
            'create_entity_list': f"{ENDPOINT}Extractions/EntityLists",
            'add_instruments_to_list': f"{ENDPOINT}Extractions/InstrumentLists('%s')/"
                                       f"ThomsonReuters.Dss.Api.Extractions.InstrumentListAppendIdentifiers",
            'add_entity_to_list': f"{ENDPOINT}Extractions/EntityLists('%s')/"
                                  f"ThomsonReuters.Dss.Api.Extractions.EntityListAppendIdentifiers",
            'create_template': f"{ENDPOINT}Extractions/%s",
            'schedules': f"{ENDPOINT}Extractions/Schedules",
            'check_extraction': f"{ENDPOINT}Extractions/Schedules('%s')/CompletedExtractions",
            'extraction_report': f"{ENDPOINT}Extractions/ReportExtractions('%s')/Files",
            'data_and_notes_extraction': f"{ENDPOINT}Extractions/ExtractedFiles('%s')/$value",
            'delete_schedule': f"{ENDPOINT}Extractions/Schedules('%s')",
            'delete_template': f"{ENDPOINT}Extractions/ReportTemplates('%s')",
            'delete_instrument_list': f"{ENDPOINT}Extractions/InstrumentLists('%s')",
            'delete_entity_list': f"{ENDPOINT}Extractions/EntityLists('%s')",
            'add_content': f"{ENDPOINT}Extractions/ReportTemplates('%s')/ThomsonReuters.Dss.Api.Extractions."
                           f"ReportTemplateAddContentField",
            'remove_content': f"{ENDPOINT}Extractions/ReportTemplates('%s')/ThomsonReuters.Dss.Api.Extractions."
                              f"ReportTemplateRemoveContentField",
            'get_all_instruments': f"{ENDPOINT}Extractions/InstrumentLists('%s')/"
                                   f"ThomsonReuters.Dss.Api.Extractions.InstrumentListGetAllInstruments",
            'get_all_entities': f"{ENDPOINT}Extractions/EntityLists('%s')/"
                                f"ThomsonReuters.Dss.Api.Extractions.EntityListGetAllEntities"
        }
    }
}
