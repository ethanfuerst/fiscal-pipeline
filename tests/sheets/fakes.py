class FakeDuckDB:
    def __init__(self, tables):
        self._tables = tables

    def get_table(self, name, where=None):
        return self._tables[name].copy()


class RecordingWorksheet:
    def __init__(self):
        self.format_calls: list[tuple[str, dict]] = []
        self.values_writes: list[tuple[str, list]] = []
        self.resize_calls: list[tuple[int | None, int | None]] = []

    def format_range(self, range_name, fmt):
        self.format_calls.append((range_name, fmt))

    def write_values(self, range_name, values):
        self.values_writes.append((range_name, values))

    def resize_sheet(self, rows=None, columns=None):
        self.resize_calls.append((rows, columns))
