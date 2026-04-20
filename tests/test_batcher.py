"""Tests for the SheetBatcher module."""

from unittest.mock import MagicMock

import pytest

from src.sheets.batcher import SheetBatcher


@pytest.fixture
def mock_spreadsheet():
    """Create a mock gspread Spreadsheet object."""
    spreadsheet = MagicMock()
    spreadsheet.id = 'test_spreadsheet_id'

    worksheet = MagicMock()
    worksheet.id = 0
    worksheet.title = 'Sheet1'
    spreadsheet.worksheet.return_value = worksheet

    spreadsheet.values_batch_update.return_value = {'responses': []}
    spreadsheet.batch_update.return_value = {'replies': []}

    return spreadsheet


@pytest.fixture
def mock_worksheet():
    """Create a mock gspread Worksheet object."""
    worksheet = MagicMock()
    worksheet.id = 0
    worksheet.title = 'Sheet1'
    return worksheet


class TestSheetBatcher:
    def test_init(self, mock_spreadsheet):
        batcher = SheetBatcher(mock_spreadsheet)
        assert batcher.spreadsheet == mock_spreadsheet
        assert batcher._value_updates == []
        assert batcher._batch_requests == []

    def test_context_manager(self, mock_spreadsheet):
        with SheetBatcher(mock_spreadsheet) as batcher:
            batcher.queue_values('Sheet1!A1', [['test']])

        mock_spreadsheet.values_batch_update.assert_called_once()

    def test_context_manager_no_flush_on_exception(self, mock_spreadsheet):
        try:
            with SheetBatcher(mock_spreadsheet) as batcher:
                batcher.queue_values('Sheet1!A1', [['test']])
                raise ValueError('Test error')
        except ValueError:
            pass

        mock_spreadsheet.values_batch_update.assert_not_called()


class TestQueueValues:
    def test_queue_values_static(self, mock_spreadsheet):
        batcher = SheetBatcher(mock_spreadsheet)
        values = [['a', 'b'], ['c', 'd']]

        batcher.queue_values('Sheet1!A1:B2', values)

        assert len(batcher._value_updates) == 1
        assert batcher._value_updates[0]['range'] == 'Sheet1!A1:B2'
        assert batcher._value_updates[0]['values'] == values

    def test_queue_values_callable(self, mock_spreadsheet):
        batcher = SheetBatcher(mock_spreadsheet)
        call_count = [0]

        def get_values():
            call_count[0] += 1
            return [['dynamic', call_count[0]]]

        batcher.queue_values('Sheet1!A1', get_values)
        assert call_count[0] == 0

        batcher.flush()
        assert call_count[0] == 1

    def test_flush_values_creates_correct_payload(self, mock_spreadsheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.queue_values('Sheet1!A1:B2', [['a', 'b'], ['c', 'd']])
        batcher.queue_values('Sheet1!C1:D2', [['e', 'f'], ['g', 'h']])

        batcher.flush()

        mock_spreadsheet.values_batch_update.assert_called_once()
        call_args = mock_spreadsheet.values_batch_update.call_args[0][0]

        assert call_args['valueInputOption'] == 'USER_ENTERED'
        assert len(call_args['data']) == 2
        assert call_args['data'][0]['range'] == 'Sheet1!A1:B2'
        assert call_args['data'][1]['range'] == 'Sheet1!C1:D2'


class TestQueueFormat:
    def test_queue_format(self, mock_spreadsheet, mock_worksheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.register_worksheet(mock_worksheet)

        batcher.queue_format('A1:B2', {'bold': True}, mock_worksheet)

        assert len(batcher._batch_requests) == 1
        assert 'repeatCell' in batcher._batch_requests[0]

    def test_queue_format_with_number_format(self, mock_spreadsheet, mock_worksheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.register_worksheet(mock_worksheet)

        format_dict = {'numberFormat': {'type': 'CURRENCY', 'pattern': '$#,##0.00'}}
        batcher.queue_format('A1:A10', format_dict, mock_worksheet)

        assert len(batcher._batch_requests) == 1
        request = batcher._batch_requests[0]
        assert 'numberFormat' in request['repeatCell']['cell']['userEnteredFormat']


class TestQueueBorder:
    def test_queue_border(self, mock_spreadsheet, mock_worksheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.register_worksheet(mock_worksheet)

        batcher.queue_border(
            'A1:B2',
            {'top': {'style': 'SOLID'}, 'bottom': {'style': 'SOLID'}},
            mock_worksheet,
        )

        assert len(batcher._batch_requests) == 1
        request = batcher._batch_requests[0]
        assert 'updateBorders' in request
        assert 'top' in request['updateBorders']
        assert 'bottom' in request['updateBorders']


class TestQueueColumnWidth:
    def test_queue_column_width_letter(self, mock_spreadsheet, mock_worksheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.register_worksheet(mock_worksheet)

        batcher.queue_column_width('A', 100, mock_worksheet)

        assert len(batcher._batch_requests) == 1
        request = batcher._batch_requests[0]
        assert request['updateDimensionProperties']['properties']['pixelSize'] == 100

    def test_queue_column_width_index(self, mock_spreadsheet, mock_worksheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.register_worksheet(mock_worksheet)

        batcher.queue_column_width(0, 150, mock_worksheet)

        request = batcher._batch_requests[0]
        assert request['updateDimensionProperties']['range']['startIndex'] == 0
        assert request['updateDimensionProperties']['range']['endIndex'] == 1

    def test_queue_column_width_letter_uses_zero_based_index(
        self, mock_spreadsheet, mock_worksheet
    ):
        """Verify column letters are converted to 0-based indices for Sheets API.

        gspread's column_letter_to_index returns 1-based (A=1, Z=26), but the
        Google Sheets API expects 0-based indices (A=0, Z=25).
        """
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.register_worksheet(mock_worksheet)

        # Column A should map to index 0
        batcher.queue_column_width('A', 100, mock_worksheet)
        request_a = batcher._batch_requests[0]
        assert request_a['updateDimensionProperties']['range']['startIndex'] == 0
        assert request_a['updateDimensionProperties']['range']['endIndex'] == 1

        # Column Z should map to index 25 (not 26)
        batcher.queue_column_width('Z', 100, mock_worksheet)
        request_z = batcher._batch_requests[1]
        assert request_z['updateDimensionProperties']['range']['startIndex'] == 25
        assert request_z['updateDimensionProperties']['range']['endIndex'] == 26


class TestQueueNotes:
    def test_queue_notes(self, mock_spreadsheet, mock_worksheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.register_worksheet(mock_worksheet)

        batcher.queue_notes({'A1': 'Note 1', 'B2': 'Note 2'}, mock_worksheet)

        assert len(batcher._batch_requests) == 2


class TestFlush:
    def test_flush_empty(self, mock_spreadsheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.flush()

        mock_spreadsheet.values_batch_update.assert_not_called()
        mock_spreadsheet.batch_update.assert_not_called()

    def test_flush_clears_queues(self, mock_spreadsheet, mock_worksheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.register_worksheet(mock_worksheet)

        batcher.queue_values('Sheet1!A1', [['test']])
        batcher.queue_format('A1', {'bold': True}, mock_worksheet)

        assert len(batcher._value_updates) == 1
        assert len(batcher._batch_requests) == 1

        batcher.flush()

        assert len(batcher._value_updates) == 0
        assert len(batcher._batch_requests) == 0


class TestRetryLogic:
    def test_retry_on_503(self, mock_spreadsheet):
        from gspread.exceptions import APIError

        mock_response = MagicMock()
        mock_response.status_code = 503

        mock_spreadsheet.values_batch_update.side_effect = [
            APIError(mock_response),
            APIError(mock_response),
            {'responses': []},
        ]

        batcher = SheetBatcher(mock_spreadsheet, base_delay=0.01)
        batcher.queue_values('Sheet1!A1', [['test']])
        batcher.flush()

        assert mock_spreadsheet.values_batch_update.call_count == 3

    def test_retry_on_429(self, mock_spreadsheet):
        from gspread.exceptions import APIError

        mock_response = MagicMock()
        mock_response.status_code = 429

        mock_spreadsheet.values_batch_update.side_effect = [
            APIError(mock_response),
            {'responses': []},
        ]

        batcher = SheetBatcher(mock_spreadsheet, base_delay=0.01)
        batcher.queue_values('Sheet1!A1', [['test']])
        batcher.flush()

        assert mock_spreadsheet.values_batch_update.call_count == 2

    def test_no_retry_on_400(self, mock_spreadsheet):
        from gspread.exceptions import APIError

        mock_response = MagicMock()
        mock_response.status_code = 400

        mock_spreadsheet.values_batch_update.side_effect = APIError(mock_response)

        batcher = SheetBatcher(mock_spreadsheet, base_delay=0.01)
        batcher.queue_values('Sheet1!A1', [['test']])

        with pytest.raises(APIError):
            batcher.flush()

        assert mock_spreadsheet.values_batch_update.call_count == 1

    def test_max_retries_exceeded(self, mock_spreadsheet):
        from gspread.exceptions import APIError

        mock_response = MagicMock()
        mock_response.status_code = 503

        mock_spreadsheet.values_batch_update.side_effect = APIError(mock_response)

        batcher = SheetBatcher(mock_spreadsheet, max_retries=3, base_delay=0.01)
        batcher.queue_values('Sheet1!A1', [['test']])

        with pytest.raises(APIError):
            batcher.flush()

        assert mock_spreadsheet.values_batch_update.call_count == 3


class TestDynamicValues:
    def test_dynamic_values_resolved_at_flush(self, mock_spreadsheet):
        batcher = SheetBatcher(mock_spreadsheet)
        timestamp = [0]

        def get_timestamp_values():
            timestamp[0] += 1
            return [[f'Time: {timestamp[0]}']]

        batcher.queue_values('Sheet1!A1', get_timestamp_values)
        assert timestamp[0] == 0

        batcher.flush()
        assert timestamp[0] == 1

        call_args = mock_spreadsheet.values_batch_update.call_args[0][0]
        assert call_args['data'][0]['values'] == [['Time: 1']]


class TestApiCallReduction:
    def test_many_operations_few_calls(self, mock_spreadsheet, mock_worksheet):
        batcher = SheetBatcher(mock_spreadsheet)
        batcher.register_worksheet(mock_worksheet)

        for i in range(10):
            batcher.queue_values(f'Sheet1!A{i + 1}', [[f'value_{i}']])

        for i in range(20):
            batcher.queue_format(f'A{i + 1}', {'bold': True}, mock_worksheet)

        for col in ['A', 'B', 'C', 'D', 'E']:
            batcher.queue_column_width(col, 100, mock_worksheet)

        for i in range(10):
            batcher.queue_notes({f'A{i + 1}': f'Note {i}'}, mock_worksheet)

        batcher.flush()

        # With batching: 2 API calls (1 values_batch_update + 1 batch_update)
        assert mock_spreadsheet.values_batch_update.call_count == 1
        assert mock_spreadsheet.batch_update.call_count == 1

        values_call = mock_spreadsheet.values_batch_update.call_args[0][0]
        assert len(values_call['data']) == 10

        batch_call = mock_spreadsheet.batch_update.call_args[0][0]
        assert len(batch_call['requests']) == 35  # 20 formats + 5 widths + 10 notes
