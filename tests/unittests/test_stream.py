from datetime import datetime
from unittest import TestCase, mock

from tap_appstore.streams import FinancialReportStream, DATE_FORMAT


def mock_financial_reports(param):
    existing_reports = {
        '2023-05': [{'test': '2023-05', 'apple_identifier': '1'},
                    {'test': '2023-05', 'apple_identifier': '1'}],
        '2023-06': [{'test': '2023-06', 'apple_identifier': '1'}],
    }
    return existing_reports.get(param.strftime('%Y-%m'))


@mock.patch("singer.utils.now")
@mock.patch("singer.write_bookmark")
@mock.patch("singer.write_record")
@mock.patch("tap_appstore.streams.Stream.get_bookmark")
@mock.patch("tap_appstore.streams.Stream.get_report")
@mock.patch("appstoreconnect.Api")
class TestStream(TestCase):
    def test_query_report(self, mock_api, mock_get_report, mock_get_bookmark, mock_write_record, mock_write_bookmark, mock_now):
        now = datetime(2023, 7, 1).astimezone()

        mock_now.return_value = now
        mock_get_bookmark.return_value = datetime(2023, 5, 1)
        mock_get_report.side_effect = mock_financial_reports

        financial_report_stream = FinancialReportStream('financial_report', mock_api.return_value, {'vendor': '123'}, {})
        financial_report_stream.query_report({})

        expected_calls_write_bookmark = [
            mock.call(mock.ANY, "financial_report", 'start_date', '2023-05-01T00:00:00Z'),
            mock.call(mock.ANY, "financial_report", 'start_date', '2023-06-01T00:00:00Z'),
            mock.call(mock.ANY, "financial_report", 'start_date', '2023-07-01T00:00:00Z'),
        ]

        # Verify `write_bookmark` is called correctly
        self.assertEqual(mock_write_bookmark.call_count, 3)
        self.assertIn(mock_write_bookmark.mock_calls[0], expected_calls_write_bookmark)
        self.assertIn(mock_write_bookmark.mock_calls[1], expected_calls_write_bookmark)
        self.assertIn(mock_write_bookmark.mock_calls[2], expected_calls_write_bookmark)

        expected_calls_write_records = [
            mock.call('financial_report', {
                '_line_id': 1, '_time_extracted': now.strftime(DATE_FORMAT), '_api_report_date': '2023-05',
                'vendor_number': '123', 'test': '2023-05', 'apple_identifier': '1'}, time_extracted=now),
            mock.call('financial_report', {
                '_line_id': 2, '_time_extracted': now.strftime(DATE_FORMAT), '_api_report_date': '2023-05',
                'vendor_number': '123', 'test': '2023-05', 'apple_identifier': '1'}, time_extracted=now),
            mock.call('financial_report', {
                '_line_id': 1, '_time_extracted': now.strftime(DATE_FORMAT), '_api_report_date': '2023-06',
                'vendor_number': '123', 'test': '2023-06', 'apple_identifier': '1'}, time_extracted=now),
        ]

        # Verify `write_records` is called correctly
        self.assertEqual(mock_write_record.call_count, 3)
        self.assertIn(mock_write_record.mock_calls[0], expected_calls_write_records)
        self.assertIn(mock_write_record.mock_calls[1], expected_calls_write_records)
        self.assertIn(mock_write_record.mock_calls[2], expected_calls_write_records)

