from datetime import datetime
from typing import Dict, Union, List, Tuple

import singer
from dateutil.relativedelta import relativedelta
from singer import Transformer, CatalogEntry

from appstoreconnect.api import APIError
from appstoreconnect import Api

LOGGER = singer.get_logger()

KEY_PROPERTIES = ['_line_id', '_time_extracted', '_api_report_date']

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

SALES_API_REQUEST_FIELDS = {
    'subscription_event_report': {
        'reportType': 'SUBSCRIPTION_EVENT',
        'frequency': 'DAILY',
        'reportSubType': 'SUMMARY',
        'version': '1_3'
    },
    'subscriber_report': {
        'reportType': 'SUBSCRIBER',
        'frequency': 'DAILY',
        'reportSubType': 'DETAILED',
        'version': '1_3'
    },
    'subscription_report': {
        'reportType': 'SUBSCRIPTION',
        'frequency': 'DAILY',
        'reportSubType': 'SUMMARY',
        'version': '1_3'
    },
    'sales_report': {
        'reportType': 'SALES',
        'frequency': 'DAILY',
        'reportSubType': 'SUMMARY',
        'version': '1_0'
    },
    'subscription_offer_code_redemption_report': {
        'reportType': 'SUBSCRIPTION_OFFER_CODE_REDEMPTION',
        'frequency': 'DAILY',
        'reportSubType': 'SUMMARY',
        'version': '1_0'
    },
    'newsstand_report': {
        'reportType': 'NEWSSTAND',
        'frequency': 'DAILY',
        'reportSubType': 'DETAILED',
        'version': '1_0'
    },
    'pre_order_report': {
        'reportType': 'PRE_ORDER',
        'frequency': 'DAILY',
        'reportSubType': 'SUMMARY',
        'version': '1_0'
    }
}


class Stream:
    """
    A base class representing tap-appstore streams.
    """
    key_properties = KEY_PROPERTIES
    delta = relativedelta(days=1)
    report_date_format = '%Y-%m-%d'

    def __init__(self, name, client: Api, config: Dict[str, any], state):
        self.api = client
        self.config = config
        self.state = state
        self.name = name

    def get_bookmark(self):
        bookmark = singer.get_bookmark(self.state, self.name, 'start_date', self.config.get('start_date'))
        return datetime.strptime(bookmark, DATE_FORMAT)

    def update_bookmark(self, value: datetime):
        singer.write_bookmark(self.state, self.name, 'start_date', value.strftime(DATE_FORMAT))

    def get_api_request_fields(self, report_date: datetime) -> Dict[str, any]:
        """Get fields to be used in appstore API request """
        return {
            'reportDate': report_date.strftime(self.report_date_format),
            'vendorNumber': f"{self.vendor}"
        }

    def _attempt_download_report(self, report_filters: Dict[str, any]) -> Union[List[Dict], None]:
        """
        Attempt to download the report from the API. If the API returns an error, log it and return None.
        """
        pass

    def get_report(self, report_date: datetime):
        LOGGER.info("Requesting Appstore data for: %s on %s", self.name, report_date.strftime(self.report_date_format))
        filters = self.get_api_request_fields(report_date)
        return self._attempt_download_report(filters)

    @property
    def vendor(self):
        return self.config['vendor']

    @staticmethod
    def parse_api_response(response_tsv):
        if isinstance(response_tsv, dict):
            raise Exception(f"Received a JSON response instead of the report: {response_tsv}")

        lines = response_tsv.split('\n')
        header = [s.lower().replace(' ', '_').replace('-', '_') for s in lines[0].split('\t')]
        data = []
        for line in lines[1:]:
            if len(line) == 0:
                continue
            line_obj = {}
            line_cols = line.split('\t')
            for i, column in enumerate(header):
                if i < len(line_cols):
                    line_obj[column] = line_cols[i].strip()
            data.append(line_obj)
        return data

    def query_report(self, schema_dict: Dict):
        # get bookmark from when data will be pulled
        iterator = self.get_bookmark().astimezone()

        extraction_time = singer.utils.now().astimezone()
        self.update_bookmark(iterator)

        with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
            while iterator + self.delta <= extraction_time:
                # setting report filters for each stream
                rep = self.get_report(iterator)

                # write records
                for index, line in enumerate(rep, start=1):
                    data = {
                        '_line_id': index,
                        '_time_extracted': extraction_time.strftime(DATE_FORMAT),
                        '_api_report_date': iterator.strftime(self.report_date_format),
                        'vendor_number': self.vendor,
                        **line
                    }
                    rec = transformer.transform(data, schema_dict)

                    singer.write_record(self.name, rec, time_extracted=extraction_time)

                self.update_bookmark(iterator + self.delta)

                singer.write_state(self.state)
                iterator += self.delta
        singer.write_state(self.state)


class SalesReportStream(Stream):
    def _attempt_download_report(self, report_filters: Dict[str, any]) -> Union[List[Dict], None]:
        try:
            rep_tsv = self.api.download_sales_and_trends_reports(filters=report_filters)
        except APIError as e:
            LOGGER.error(e)
            return None

        return self.parse_api_response(rep_tsv)

    def get_api_request_fields(self, report_date) -> Dict[str, any]:
        """Get fields to be used in appstore API request """
        report_filters = super().get_api_request_fields(report_date)
        try:
            report_filters.update(SALES_API_REQUEST_FIELDS[self.name])
        except KeyError:
            LOGGER.error(f'API request fields not set to stream "{self.name}"')
        return report_filters


class FinancialReportStream(Stream):
    delta = relativedelta(months=1)
    report_date_format = '%Y-%m'

    def _attempt_download_report(self, report_filters: Dict[str, any]) -> Union[List[Dict], None]:
        # fetch data from appstore api
        try:
            rep_tsv = self.api.download_finance_reports(filters=report_filters)
        except APIError as e:
            LOGGER.error(e)
            return None

        return self.parse_api_response(rep_tsv)

    def get_bookmark(self):
        return super().get_bookmark().replace(day=1, hour=0, minute=0, second=0, microsecond=0)


# Dictionary of the stream classes
STREAMS = {
    "subscription_event_report": SalesReportStream,
    "subscriber_report": SalesReportStream,
    "subscription_report": SalesReportStream,
    "sales_report": SalesReportStream,
    "subscription_offer_code_redemption_report": SalesReportStream,
    "newsstand_report": SalesReportStream,
    "pre_order_report": SalesReportStream,
    "financial_report": FinancialReportStream,
}