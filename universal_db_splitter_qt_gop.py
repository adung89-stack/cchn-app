import sys
import os
import re
import io
import math
import sqlite3
import shutil
import subprocess
import tempfile
import hashlib
import zipfile
import posixpath
import threading
import itertools
import unicodedata
import numbers
import datetime as dt
import xml.etree.ElementTree as ET
from collections import namedtuple
from pathlib import Path
from copy import copy

import duckdb

# PySide6 Imports
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QGroupBox, QLabel, QLineEdit, QPushButton,
    QFileDialog, QMessageBox, QTabWidget, QScrollArea, QCheckBox,
    QSpinBox, QTableWidget, QTableWidgetItem, QAbstractItemView, QToolButton, QHeaderView
)
from PySide6.QtCore import Qt, QObject, Signal, Slot, QTimer
from PySide6.QtGui import QBrush, QColor

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    PYARROW_AVAILABLE = False

try:
    import openpyxl
    from openpyxl.utils import get_column_letter
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Font, Border, Side, Alignment
    OPENPYXL_AVAILABLE = True
except ImportError:
    openpyxl = None
    get_column_letter = None
    WriteOnlyCell = None
    OPENPYXL_AVAILABLE = False

# python-calamine (Rust) is by far the fastest Excel reader available for
# Python. It is used for all bulk data reading when installed.
try:
    from python_calamine import CalamineWorkbook
    CALAMINE_AVAILABLE = True
except ImportError:
    CalamineWorkbook = None
    CALAMINE_AVAILABLE = False

try:
    import xlrd
    XLRD_AVAILABLE = True
except ImportError:
    xlrd = None
    XLRD_AVAILABLE = False


SAMPLE_DATA_ROWS = 3        # sample data rows shown in the preview table
HEADER_SCAN_ROWS = 30       # rows scanned to auto-detect the header block
HEAD_LOAD_ROWS = HEADER_SCAN_ROWS + SAMPLE_DATA_ROWS + 5
INGEST_CHUNK_ROWS = 50_000
FETCH_CHUNK_ROWS = 20_000
EXCEL_MAX_CELL_CHARS = 32767
MAX_NAME_LEN = 150

EXCEL_EXTS = ('.xlsx', '.xlsm', '.xls')
DB_EXTS = ('.db', '.sqlite', '.sqlite3')
SUPPORTED_EXTS = EXCEL_EXTS + DB_EXTS + ('.csv',)
ARCHIVE_EXTS = ('.zip', '.rar')

# Column-name keys (after normalisation) that identify the MA_CSKCB column.
MA_COLUMN_KEYS = {'MACSKCB', 'MACOSOKCB'}


# ===========================================================================
# 1. Cell value codec  (data integrity core)
# ===========================================================================
# Every Excel cell is stored in DuckDB as a pair:
#   c<i>  VARCHAR   canonical text  -> used for filtering / file naming
#   k<i>  UTINYINT  type code       -> used to rebuild the ORIGINAL type on export
# So numbers stay numbers, dates stay dates, "00123" stays text, and nothing
# turns into "79001.0" or "nan" on the way through.

T_EMPTY, T_STR, T_INT, T_FLOAT, T_BOOL, T_DATETIME, T_DATE, T_TIME, T_TDELTA, T_STR_EQ = range(10)

_ILLEGAL_XML_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')


def _round_ms(value):
    """Round a datetime/time to milliseconds (Excel's real precision)."""
    ms, rem = divmod(value.microsecond, 1000)
    if rem >= 500:
        ms += 1
    if ms >= 1000:
        if isinstance(value, dt.datetime):
            return value.replace(microsecond=0) + dt.timedelta(seconds=1)
        ms = 999
    return value.replace(microsecond=ms * 1000)


def _datetime_text(value):
    if value.tzinfo is not None:
        value = value.replace(tzinfo=None)
    value = _round_ms(value)
    if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
        return value.strftime('%Y-%m-%d')
    text = value.strftime('%Y-%m-%d %H:%M:%S')
    if value.microsecond:
        text += f".{value.microsecond // 1000:03d}"
    return text


def _time_text(value):
    value = _round_ms(value)
    text = value.strftime('%H:%M:%S')
    if value.microsecond:
        text += f".{value.microsecond // 1000:03d}"
    return text


def encode_cell(value, stats=None):
    """Return (canonical_text or None, type_code)."""
    if value is None:
        return None, T_EMPTY
    if isinstance(value, str):
        if not value:
            return None, T_EMPTY
        if not value.isprintable():
            cleaned = _ILLEGAL_XML_RE.sub('', value)
            if cleaned != value:
                if stats is not None:
                    stats['cleaned'] += 1
                value = cleaned
                if not value:
                    return None, T_EMPTY
        if len(value) > EXCEL_MAX_CELL_CHARS:
            if stats is not None:
                stats['truncated'] += 1
            value = value[:EXCEL_MAX_CELL_CHARS]
        return value, (T_STR_EQ if value[0] == '=' else T_STR)
    if isinstance(value, bool):
        return ('TRUE' if value else 'FALSE'), T_BOOL
    if isinstance(value, int):
        return str(value), T_INT
    if isinstance(value, float):
        if not math.isfinite(value):
            return None, T_EMPTY
        if value.is_integer() and -1e15 < value < 1e15:
            return str(int(value)), T_INT
        return repr(value), T_FLOAT
    if isinstance(value, dt.datetime):
        return _datetime_text(value), T_DATETIME
    if isinstance(value, dt.date):
        return value.isoformat(), T_DATE
    if isinstance(value, dt.time):
        return _time_text(value), T_TIME
    if isinstance(value, dt.timedelta):
        return repr(round(value.total_seconds(), 3)), T_TDELTA
    if isinstance(value, numbers.Integral):
        return str(int(value)), T_INT
    if isinstance(value, numbers.Real):
        return encode_cell(float(value), stats)
    return encode_cell(str(value), stats)


def canonical_text(value):
    text, _ = encode_cell(value)
    return text if text is not None else ''


def decode_cell(text, code):
    """Rebuild the original Python value from (text, code)."""
    if text is None or code == T_EMPTY:
        return None
    if code == T_STR or code == T_STR_EQ:
        return text
    if code == T_INT:
        return int(text)
    if code == T_FLOAT:
        return float(text)
    if code == T_BOOL:
        return text == 'TRUE'
    if code == T_DATETIME:
        return dt.datetime.fromisoformat(text)
    if code == T_DATE:
        return dt.date.fromisoformat(text)
    if code == T_TIME:
        return dt.time.fromisoformat(text)
    if code == T_TDELTA:
        return dt.timedelta(seconds=float(text))
    return text


def excel_out_value(ws, text, code):
    """Value/cell to hand to a write-only worksheet for one data cell."""
    if text is None or code == T_EMPTY:
        return None
    if code == T_STR:
        return text
    if code == T_STR_EQ:
        # Text that starts with "=" must stay text, not become a formula.
        cell = WriteOnlyCell(ws, value=text)
        cell.data_type = 's'
        return cell
    value = decode_cell(text, code)
    if code == T_DATETIME:
        cell = WriteOnlyCell(ws, value=value)
        cell.number_format = 'dd/mm/yyyy' if len(text) == 10 else 'dd/mm/yyyy hh:mm:ss'
        return cell
    if code == T_DATE:
        cell = WriteOnlyCell(ws, value=value)
        cell.number_format = 'dd/mm/yyyy'
        return cell
    if code == T_TIME:
        cell = WriteOnlyCell(ws, value=value)
        cell.number_format = 'hh:mm:ss'
        return cell
    if code == T_TDELTA:
        cell = WriteOnlyCell(ws, value=value)
        cell.number_format = '[h]:mm:ss'
        return cell
    return value


class RowDigest:
    """Order-sensitive fingerprint of the non-empty data rows of a sheet,
    computed on both sides (while writing and when re-reading the output)."""

    def __init__(self):
        self.h = hashlib.blake2b(digest_size=16)
        self.count = 0

    def add(self, texts):
        n = len(texts)
        while n and texts[n - 1] == '':
            n -= 1
        if n:
            self.h.update('\x1f'.join(texts[:n]).encode('utf-8', 'surrogatepass'))
            self.h.update(b'\x1e')
            self.count += 1

    def hexdigest(self):
        return self.h.hexdigest()


# ===========================================================================
# 2. Naming helpers
# ===========================================================================

def normalize_key(text):
    s = unicodedata.normalize('NFD', str(text))
    s = ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')
    s = s.replace('đ', 'd').replace('Đ', 'D')
    return re.sub(r'[^A-Z0-9]', '', s.upper())


def detect_ma_in_names(names):
    for name in names:
        if normalize_key(name) in MA_COLUMN_KEYS:
            return name
    return None


def detect_ma_in_grid(grid, max_col):
    """Index (0-based) of the first column whose header cells contain MA_CSKCB."""
    for c in range(max_col):
        for row in grid:
            v = row[c] if c < len(row) else None
            if v is not None and normalize_key(v) in MA_COLUMN_KEYS:
                return c
    return None


_INVALID_FN_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_RESERVED_NAMES = {'CON', 'PRN', 'AUX', 'NUL'} | {f'COM{i}' for i in range(1, 10)} | {f'LPT{i}' for i in range(1, 10)}


def sanitize_name_part(text):
    s = unicodedata.normalize('NFC', str(text))
    s = _INVALID_FN_RE.sub('_', s)
    s = re.sub(r'\s+', ' ', s).strip().strip('.')
    return s


def build_output_basename(ma_value, sheet_part):
    parts = []
    if ma_value:
        parts.append(sanitize_name_part(ma_value))
    if sheet_part:
        parts.append(sanitize_name_part(sheet_part))
    name = '_'.join(p for p in parts if p) or 'output'
    if len(name) > MAX_NAME_LEN:
        name = name[:MAX_NAME_LEN].rstrip(' ._')
    if name.split('.')[0].upper() in _RESERVED_NAMES:
        name = '_' + name
    return name


class NameAllocator:
    """Never overwrites: 79001_Sheet1.xlsx, 79001_Sheet1_2.xlsx, ..."""

    def __init__(self, out_dir):
        self.out_dir = out_dir
        self.used = set()

    def allocate(self, base, ext):
        n = 1
        while True:
            candidate = base if n == 1 else f"{base}_{n}"
            path = os.path.join(self.out_dir, candidate + ext)
            key = os.path.normcase(path)
            if key not in self.used and not os.path.exists(path):
                self.used.add(key)
                return path
            n += 1


# ===========================================================================
# 3. SQL helpers
# ===========================================================================

Cond = namedtuple('Cond', 'scope col op values key origin')
# scope = sheet name, or None for a global filter; origin = sheet it was typed on
# key   = normalised header name of the column, used to find the matching
#         column in OTHER sheets for global filters (Excel columns are
#         positional, so position alone is not reliable across sheets).


def column_key(label):
    """Normalised leaf header name ('Thông tin > Mã CSKCB' -> 'MACSKCB')."""
    leaf = str(label).split(' > ')[-1]
    key = normalize_key(leaf)
    return 'MACSKCB' if key in MA_COLUMN_KEYS else key


BATCH_ORIGIN = '\x00batch'   # origin of filters typed in batch mode (no real sheet)


def resolve_named_col(cond, cols):
    """Column name for a CSV/DB filter: same name, else same normalised key."""
    if cond.col in cols:
        return cond.col
    for c in cols:
        if column_key(c) == cond.key:
            return c
    return None


def ma_split_tasks(values, make_cond, report):
    values = sorted({v for v in values if v})
    if not values:
        report.warn("Auto-split by MA_CSKCB: no MA_CSKCB value found -> exported as one file.")
        return [("full export (no MA_CSKCB value found)", [])]
    return [(f"MA_CSKCB = {v}", [make_cond(v)]) for v in values]


def sql_str(value):
    return "'" + str(value).replace("'", "''") + "'"


def qident(name):
    return '"' + str(name).replace('"', '""') + '"'


def build_where(conds, col_expr):
    """col_expr(cond) -> SQL text expression, or None if the column is missing.
    EXACT  : equality ignoring surrounding spaces.
    CLOSEST: case-insensitive "contains" (no LIKE wildcards, so '_' and '%'
             in codes are matched literally)."""
    clauses, missing = [], []
    for cond in conds:
        expr = col_expr(cond)
        if expr is None:
            missing.append(cond.key or cond.col)
            continue
        ors = []
        for v in cond.values:
            if cond.op == 'EXACT':
                ors.append(f"trim({expr}) = {sql_str(v)}")
            else:
                ors.append(f"contains(lower({expr}), lower({sql_str(v)}))")
        if ors:
            clauses.append('(' + ' OR '.join(ors) + ')')
    return ' AND '.join(clauses), missing


def where_sql(where):
    return f"WHERE {where}" if where else ""


def applicable_conds(task_conds, sheet_name):
    return [c for c in task_conds if c.scope is None or c.scope == sheet_name]


def ma_from_filters(task_conds, ma_by_scope):
    """Fallback MA value: the filter value(s) the user typed on the MA column."""
    values = []
    for c in task_conds:
        is_ma = c.key == 'MACSKCB' or (c.scope is not None and c.col == ma_by_scope.get(c.scope))
        if is_ma:
            for v in c.values:
                if v not in values:
                    values.append(v)
    return '_'.join(values) if values else None


def pick_ma_value(distinct_values, task_conds, ma_by_scope):
    distinct_values = {v for v in distinct_values if v}
    if len(distinct_values) == 1:
        return next(iter(distinct_values))
    return ma_from_filters(task_conds, ma_by_scope)


# ===========================================================================
# 4. Fast Excel "head" loading (header block + first rows, with styles)
# ===========================================================================
# openpyxl.load_workbook() on a big file parses EVERY cell (minutes, GBs of
# RAM) even though only the first few rows are needed for the header. Here we
# build an in-memory copy of the .xlsx whose sheets keep only their first N
# rows (plus merges / column widths / styles), trim sharedStrings to the
# entries those rows use, and let openpyxl load THAT tiny copy.

_ZIP_CHUNK = 1 << 20
_PREFIX = rb'((?:[A-Za-z_][\w.\-]*:)?)'
_SHEETDATA_OPEN_RE = re.compile(rb'<' + _PREFIX + rb'sheetData\b[^>]*?(/?)>')
_WORKSHEET_OPEN_RE = re.compile(rb'<' + _PREFIX + rb'worksheet\b')
_ROW_R_RE = re.compile(rb'\sr="(\d+)"')
_SST_CELL_RE = re.compile(rb'<(?:[A-Za-z_][\w.\-]*:)?c\b[^>]*?\st="s"[^>]*>\s*<(?:[A-Za-z_][\w.\-]*:)?v>\s*(\d+)\s*<')
_MERGECELL_RE = re.compile(rb'<(?:[A-Za-z_][\w.\-]*:)?mergeCell\b[^>]*?\sref="([A-Z]+)(\d+)(?::[A-Z]+\d+)?"[^>]*/>')
_HYPERLINKS_RE = re.compile(rb'<((?:[A-Za-z_][\w.\-]*:)?)hyperlinks\b.*?</\1hyperlinks>', re.S)
_SST_OPEN_RE = re.compile(rb'<' + _PREFIX + rb'sst\b[^>]*>')

_NS_MAIN = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
_NS_REL_ID = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id'
_NS_PKG_REL = '{http://schemas.openxmlformats.org/package/2006/relationships}'


def _trim_sheet_xml(stream, max_rows):
    """Return (trimmed_xml_bytes, set_of_shared_string_indices_used)."""
    buf = b''
    while True:
        m = _SHEETDATA_OPEN_RE.search(buf)
        if m:
            break
        chunk = stream.read(_ZIP_CHUNK)
        if not chunk:
            return buf, set()
        buf += chunk

    prefix, self_closing = m.group(1), m.group(2) == b'/'
    head = buf[:m.end()]
    rest = buf[m.end():]
    ws_m = _WORKSHEET_OPEN_RE.search(head)
    ws_prefix = ws_m.group(1) if ws_m else b''
    end_tag = b'</' + prefix + b'sheetData>'

    if self_closing:
        return head + rest + stream.read(), set()

    if max_rows <= 0:
        # Nothing needed from this sheet: close it without reading the rest.
        return head + end_tag + b'</' + ws_prefix + b'worksheet>', set()

    row_open_re = re.compile(rb'<' + re.escape(prefix) + rb'row\b([^>]*?)(/?)>')
    row_close = b'</' + prefix + b'row>'
    kept = []
    buf, pos, last_r = rest, 0, 0
    tail = None
    while True:
        m = row_open_re.search(buf, pos)
        e = buf.find(end_tag, pos)
        if e != -1 and (m is None or e < m.start()):
            tail = buf[e:] + stream.read()
            break
        if m is None:
            chunk = stream.read(_ZIP_CHUNK)
            if not chunk:
                tail = end_tag + b'</' + ws_prefix + b'worksheet>'
                break
            buf, pos = buf[pos:] + chunk, 0
            continue
        rm = _ROW_R_RE.search(b' ' + m.group(1))
        r = int(rm.group(1)) if rm else last_r + 1
        if m.group(2) == b'/':
            row_end = m.end()
        else:
            ce = buf.find(row_close, m.end())
            if ce == -1:
                chunk = stream.read(_ZIP_CHUNK)
                if not chunk:
                    tail = end_tag + b'</' + ws_prefix + b'worksheet>'
                    break
                buf, pos = buf[m.start():] + chunk, 0
                continue
            row_end = ce + len(row_close)
        if r > max_rows:
            # Skip the remaining rows cheaply, only looking for </sheetData>.
            seg = buf[m.start():]
            while True:
                e = seg.find(end_tag)
                if e != -1:
                    tail = seg[e:] + stream.read()
                    break
                chunk = stream.read(_ZIP_CHUNK)
                if not chunk:
                    tail = end_tag + b'</' + ws_prefix + b'worksheet>'
                    break
                seg = seg[-len(end_tag):] + chunk
            break
        kept.append(buf[m.start():row_end])
        last_r = r
        pos = row_end

    # Drop merges that start below the kept rows, and hyperlinks (they would
    # make openpyxl create cells far outside the header area).
    def _merge_filter(mm):
        return mm.group(0) if int(mm.group(2)) <= max_rows else b''
    tail = _MERGECELL_RE.sub(_merge_filter, tail)
    tail = _HYPERLINKS_RE.sub(b'', tail)

    rows_xml = b''.join(kept)
    sst_used = {int(x) for x in _SST_CELL_RE.findall(rows_xml)}
    return head + rows_xml + tail, sst_used


def _trim_sst_xml(stream, count):
    buf = b''
    while True:
        m = _SST_OPEN_RE.search(buf)
        if m:
            break
        chunk = stream.read(_ZIP_CHUNK)
        if not chunk:
            return buf
        buf += chunk
    if m.group(0).endswith(b'/>'):
        return buf[:m.end()]
    prefix = m.group(1)
    si_end_re = re.compile(rb'</' + re.escape(prefix) + rb'si>|<' + re.escape(prefix) + rb'si\b[^>]*/>')
    out = [buf[:m.end()]]
    buf, pos, n = buf[m.end():], 0, 0
    while n < count:
        mm = si_end_re.search(buf, pos)
        if mm is None:
            chunk = stream.read(_ZIP_CHUNK)
            if not chunk:
                break
            out.append(buf[:pos])
            buf, pos = buf[pos:] + chunk, 0
            continue
        pos = mm.end()
        n += 1
    out.append(buf[:pos])
    out.append(b'</' + prefix + b'sst>')
    return b''.join(out)


def _xlsx_sheet_parts(zf):
    """Map sheet name -> zip part name of its worksheet XML."""
    names = set(zf.namelist())
    wb_part = 'xl/workbook.xml'
    try:
        root_rels = ET.fromstring(zf.read('_rels/.rels'))
        for rel in root_rels.iter(_NS_PKG_REL + 'Relationship'):
            if rel.get('Type', '').endswith('/officeDocument'):
                wb_part = rel.get('Target', wb_part).lstrip('/')
                break
    except Exception:
        pass
    wb_dir = posixpath.dirname(wb_part)
    rels_part = posixpath.join(wb_dir, '_rels', posixpath.basename(wb_part) + '.rels')
    targets = {}
    for rel in ET.fromstring(zf.read(rels_part)).iter(_NS_PKG_REL + 'Relationship'):
        target = rel.get('Target', '')
        if target.startswith('/'):
            part = target.lstrip('/')
        else:
            part = posixpath.normpath(posixpath.join(wb_dir, target))
        targets[rel.get('Id')] = part
    result, sst_part = {}, None
    for sheet in ET.fromstring(zf.read(wb_part)).iter(_NS_MAIN + 'sheet'):
        part = targets.get(sheet.get(_NS_REL_ID))
        if part in names:
            result[sheet.get('name')] = part
    for rid, part in targets.items():
        if part.lower().endswith('sharedstrings.xml') and part in names:
            sst_part = part
    return result, sst_part


def _load_head_xlsx(path, rows_by_sheet, default_rows):
    with zipfile.ZipFile(path) as zin:
        sheet_parts, sst_part = _xlsx_sheet_parts(zin)
        part_rows = {part: rows_by_sheet.get(name, default_rows) for name, part in sheet_parts.items()}
        out_io = io.BytesIO()
        sst_used = set()
        with zipfile.ZipFile(out_io, 'w', zipfile.ZIP_STORED) as zout:
            for info in zin.infolist():
                if info.filename == sst_part:
                    continue
                if info.filename in part_rows:
                    with zin.open(info) as stream:
                        data, used = _trim_sheet_xml(stream, part_rows[info.filename])
                    sst_used |= used
                    zout.writestr(info.filename, data)
                else:
                    zout.writestr(info.filename, zin.read(info))
            if sst_part:
                with zin.open(sst_part) as stream:
                    zout.writestr(sst_part, _trim_sst_xml(stream, (max(sst_used) + 1) if sst_used else 0))
    out_io.seek(0)
    return openpyxl.load_workbook(out_io, data_only=True)


def _clean_header_value(value):
    if isinstance(value, str):
        return _ILLEGAL_XML_RE.sub('', value)
    return value


def _load_head_from_values(path, rows_by_sheet, default_rows):
    """For .xls: build a small openpyxl workbook from values + merges."""
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    wb._plain_header = True
    sheets = []
    if CALAMINE_AVAILABLE:
        cwb = CalamineWorkbook.from_path(path)
        try:
            for name in cwb.sheet_names:
                n = rows_by_sheet.get(name, default_rows)
                s = cwb.get_sheet_by_name(name)
                rows = s.to_python(skip_empty_area=False, nrows=n) if n > 0 else []
                merges = [(r1, c1, r2, c2) for (r1, c1), (r2, c2) in (s.merged_cell_ranges or [])]
                sheets.append((name, rows[:n], merges))
        finally:
            cwb.close()
    elif XLRD_AVAILABLE:
        book = xlrd.open_workbook(path, formatting_info=True, on_demand=True)
        try:
            for name in book.sheet_names():
                n = rows_by_sheet.get(name, default_rows)
                sh = book.sheet_by_name(name)
                rows = [_xlrd_row(book, sh, r) for r in range(min(n, sh.nrows))]
                merges = [(rlo, clo, rhi - 1, chi - 1) for rlo, rhi, clo, chi in sh.merged_cells]
                sheets.append((name, rows, merges))
        finally:
            book.release_resources()
    else:
        raise ImportError("Reading .xls needs 'python-calamine' (recommended) or 'xlrd'.")

    for name, rows, merges in sheets:
        ws = wb.create_sheet(title=name[:31])
        for r, row in enumerate(rows, start=1):
            for c, v in enumerate(row, start=1):
                if v is None or v == '':
                    continue
                cell = ws.cell(row=r, column=c, value=_clean_header_value(v))
                if isinstance(cell.value, str) and cell.value.startswith('='):
                    cell.data_type = 's'
        for r1, c1, r2, c2 in merges:
            if r1 < len(rows):
                r2 = min(r2, len(rows) - 1)
                if r2 > r1 or c2 > c1:
                    ws.merge_cells(start_row=r1 + 1, start_column=c1 + 1, end_row=r2 + 1, end_column=c2 + 1)
    return wb


def load_head_workbook(path, rows_by_sheet=None, default_rows=HEAD_LOAD_ROWS):
    """openpyxl workbook containing only the first rows of each sheet.
    rows_by_sheet: {sheet_name: n_rows}; other sheets get default_rows."""
    rows_by_sheet = rows_by_sheet or {}
    ext = os.path.splitext(path)[1].lower()
    if ext == '.xls':
        return _load_head_from_values(path, rows_by_sheet, default_rows)
    try:
        return _load_head_xlsx(path, rows_by_sheet, default_rows)
    except Exception:
        # Unusual package layout: fall back to the slow full load.
        return openpyxl.load_workbook(path, data_only=True)


# ===========================================================================
# 5. Complex Excel header helpers
# ===========================================================================

def detect_header_rows(ws, max_scan=HEADER_SCAN_ROWS, numeric_threshold=0.3):
    ncols = ws.max_column or 1
    max_r = min(max_scan, ws.max_row or 1)
    for r in range(1, max_r + 1):
        values = [ws.cell(row=r, column=c).value for c in range(1, ncols + 1)]
        non_null = [v for v in values if v is not None and str(v).strip() != '']
        if not non_null:
            continue
        numeric_count = sum(1 for v in non_null
                            if isinstance(v, (int, float, dt.date, dt.time)) and not isinstance(v, bool))
        if numeric_count >= 2 and (numeric_count / len(non_null)) >= numeric_threshold:
            return max(r - 1, 1)
    return 1


def build_effective_grid(ws, header_rows, max_col):
    grid = [[ws.cell(row=r, column=c).value for c in range(1, max_col + 1)]
            for r in range(1, header_rows + 1)]
    for m in ws.merged_cells.ranges:
        if m.min_row > header_rows:
            continue
        top_val = ws.cell(row=m.min_row, column=m.min_col).value
        for r in range(m.min_row, min(m.max_row, header_rows) + 1):
            for c in range(m.min_col, min(m.max_col, max_col) + 1):
                grid[r - 1][c - 1] = top_val
    return grid


def build_column_labels(grid, max_col):
    labels = []
    for c in range(max_col):
        seen = []
        for row in grid:
            distinct = set(x for x in row if x is not None)
            if len(distinct) <= 1 and len(row) > 1:
                continue
            v = row[c]
            if v is None:
                continue
            s = str(v).strip()
            if s and (not seen or seen[-1] != s):
                seen.append(s)
        labels.append(" > ".join(seen) if seen else f"Column_{c + 1}")
    return labels


def scan_sheet_layout(ws, header_rows=None):
    max_col = ws.max_column or 1
    max_row = ws.max_row or 1
    if header_rows is None:
        header_rows = detect_header_rows(ws)
    header_rows = max(0, min(header_rows, max_row))

    grid = build_effective_grid(ws, header_rows, max_col) if header_rows else []
    labels = build_column_labels(grid, max_col) if header_rows else \
        [f"Column_{c + 1}" for c in range(max_col)]
    merges = [
        (m.min_row, min(m.max_row, header_rows), m.min_col, m.max_col)
        for m in ws.merged_cells.ranges if header_rows and m.min_row <= header_rows
    ]
    sample_rows = []
    for r in range(header_rows + 1, min(header_rows + SAMPLE_DATA_ROWS, max_row) + 1):
        sample_rows.append([ws.cell(row=r, column=c).value for c in range(1, max_col + 1)])

    return {
        'labels': labels, 'header_rows': header_rows, 'max_col': max_col,
        'grid': grid, 'merges': merges, 'sample_rows': sample_rows,
        'ma_index': detect_ma_in_grid(grid, max_col),
    }


def snapshot_header_block(ws, header_rows, max_col, plain_source=False):
    """Capture header values + styles + merges + row heights + column widths."""
    plain_font = Font(bold=True)
    thin = Side(style='thin')
    plain_border = Border(left=thin, right=thin, top=thin, bottom=thin)
    plain_align = Alignment(horizontal='center', vertical='center', wrap_text=True)

    rows = []
    for r in range(1, header_rows + 1):
        row_cells = []
        for c in range(1, max_col + 1):
            src = ws.cell(row=r, column=c)
            if plain_source:
                style = (plain_font, None, plain_border, plain_align, 'General')
            else:
                style = (copy(src.font), copy(src.fill), copy(src.border),
                         copy(src.alignment), src.number_format)
            row_cells.append((_clean_header_value(src.value),) + style)
        rows.append(row_cells)

    merges = []
    for m in ws.merged_cells.ranges:
        if m.min_row > header_rows:
            continue
        min_r, max_r = m.min_row, min(m.max_row, header_rows)
        min_c, max_c = m.min_col, min(m.max_col, max_col)
        if max_r > min_r or max_c > min_c:
            merges.append((min_r, max_r, min_c, max_c))

    row_heights = {r: ws.row_dimensions[r].height for r in range(1, header_rows + 1)
                   if ws.row_dimensions[r].height}
    col_widths = {}
    for c in range(1, max_col + 1):
        letter = get_column_letter(c)
        if letter in ws.column_dimensions and ws.column_dimensions[letter].width:
            col_widths[letter] = ws.column_dimensions[letter].width

    grid = build_effective_grid(ws, header_rows, max_col) if header_rows else []
    return {'rows': rows, 'merges': merges, 'row_heights': row_heights,
            'col_widths': col_widths, 'grid': grid, 'max_col': max_col}


def snapshot_from_grid(grid, merges, max_col):
    """Fallback snapshot (values only) when the styled header can't be read."""
    rows = [[(_clean_header_value(row[c] if c < len(row) else None), None, None, None, None, 'General')
             for c in range(max_col)] for row in grid]
    return {'rows': rows, 'merges': [m for m in merges if m[1] > m[0] or m[3] > m[2]],
            'row_heights': {}, 'col_widths': {}, 'grid': grid, 'max_col': max_col}


def write_header_snapshot(ws, snapshot):
    """Write the header block into a write-only worksheet (must be called
    before any data row is appended)."""
    for letter, width in snapshot['col_widths'].items():
        ws.column_dimensions[letter].width = width
    for r, height in snapshot['row_heights'].items():
        ws.row_dimensions[r].height = height
    for row_cells in snapshot['rows']:
        out = []
        for value, font, fill, border, alignment, number_format in row_cells:
            cell = WriteOnlyCell(ws, value=value)
            if isinstance(value, str) and value.startswith('='):
                cell.data_type = 's'
            if font is not None:
                cell.font = font
            if fill is not None:
                cell.fill = fill
            if border is not None:
                cell.border = border
            if alignment is not None:
                cell.alignment = alignment
            if number_format:
                cell.number_format = number_format
            out.append(cell)
        ws.append(out)
    for (min_r, max_r, min_c, max_c) in snapshot['merges']:
        ws.merged_cells.add(f"{get_column_letter(min_c)}{min_r}:{get_column_letter(max_c)}{max_r}")


# ===========================================================================
# 6. Streaming Excel data readers
# ===========================================================================

def _xlrd_row(book, sh, r):
    out = []
    for cell in sh.row(r):
        ct = cell.ctype
        if ct == xlrd.XL_CELL_DATE:
            try:
                out.append(xlrd.xldate.xldate_as_datetime(cell.value, book.datemode))
            except Exception:
                out.append(cell.value)
        elif ct == xlrd.XL_CELL_BOOLEAN:
            out.append(bool(cell.value))
        elif ct in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK, xlrd.XL_CELL_ERROR):
            out.append(None)
        else:
            out.append(cell.value)
    return out


def open_sheet_rows(path, sheet_name, skip_rows):
    """Return (row_iterator, width). Rows are lists aligned to column A and
    start at absolute row index `skip_rows` (0-based), i.e. just below the
    header block."""
    ext = os.path.splitext(path)[1].lower()

    if CALAMINE_AVAILABLE:
        cwb = CalamineWorkbook.from_path(path)
        sheet = cwb.get_sheet_by_name(sheet_name)
        start = sheet.start
        if start is None:
            cwb.close()
            return iter(()), 0
        r0, c0 = start
        width = c0 + sheet.width

        def gen():
            try:
                pad = [None] * c0
                for i, row in enumerate(sheet.iter_rows()):
                    if r0 + i < skip_rows:
                        continue
                    yield (pad + row) if c0 else row
            finally:
                cwb.close()

        # Rows between skip_rows and r0 (leading blank rows) don't exist in
        # the calamine range; emit them as empty so row order is preserved.
        def gen_with_gap():
            for _ in range(max(0, r0 - skip_rows)):
                yield []
            yield from gen()
        return gen_with_gap(), width

    if ext == '.xls':
        if not XLRD_AVAILABLE:
            raise ImportError("Reading .xls needs 'python-calamine' (recommended) or 'xlrd'.")
        book = xlrd.open_workbook(path, on_demand=True)
        sh = book.sheet_by_name(sheet_name)

        def gen_xls():
            try:
                for r in range(skip_rows, sh.nrows):
                    yield _xlrd_row(book, sh, r)
            finally:
                book.release_resources()
        return gen_xls(), sh.ncols

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet_name]
    width = ws.max_column or 0

    def gen_ro():
        try:
            for row in ws.iter_rows(min_row=skip_rows + 1, values_only=True):
                yield list(row)
        finally:
            wb.close()
    return gen_ro(), width


# ===========================================================================
# 7. DuckDB ingestion
# ===========================================================================

def _register_chunk(con, columns):
    if PYARROW_AVAILABLE:
        arrays, names = [], []
        for name, values, kind in columns:
            names.append(name)
            if kind == 'rid':
                arrays.append(pa.array(values, type=pa.int64()))
            elif kind == 'text':
                arrays.append(pa.array(values, type=pa.string()))
            else:
                arrays.append(pa.array(values, type=pa.uint8()))
        return pa.Table.from_arrays(arrays, names=names)
    frame = pd.DataFrame({name: values for name, values, _ in columns})
    return frame


def ingest_excel_sheet(con, table, path, sheet_name, header_rows, min_cols, stats, status_cb):
    rows, width = open_sheet_rows(path, sheet_name, header_rows)
    ncols = max(width, min_cols, 1)

    def create_cols(start, end):
        return ', '.join(f"c{i} VARCHAR, k{i} UTINYINT" for i in range(start, end))

    con.execute(f'CREATE TABLE {qident(table)} (__rid BIGINT, {create_cols(0, ncols)})')

    rids, texts, codes = [], [[] for _ in range(ncols)], [[] for _ in range(ncols)]
    rid, pending_empty, total = 0, 0, 0

    def flush():
        if not rids:
            return
        cols = [('__rid', rids, 'rid')]
        for i in range(ncols):
            cols.append((f'c{i}', texts[i], 'text'))
            cols.append((f'k{i}', codes[i], 'code'))
        chunk = _register_chunk(con, cols)
        con.register('_ingest_chunk', chunk)
        con.execute(f'INSERT INTO {qident(table)} SELECT * FROM _ingest_chunk')
        con.unregister('_ingest_chunk')
        rids.clear()
        for i in range(ncols):
            texts[i].clear()
            codes[i].clear()

    def append_row(row_t, row_k):
        nonlocal rid
        rids.append(rid)
        rid += 1
        n = len(row_t)
        for i in range(ncols):
            if i < n:
                texts[i].append(row_t[i])
                codes[i].append(row_k[i])
            else:
                texts[i].append(None)
                codes[i].append(T_EMPTY)

    for row in rows:
        if len(row) > ncols:
            flush()
            for i in range(ncols, len(row)):
                con.execute(f'ALTER TABLE {qident(table)} ADD COLUMN c{i} VARCHAR')
                con.execute(f'ALTER TABLE {qident(table)} ADD COLUMN k{i} UTINYINT DEFAULT 0')
                texts.append([])
                codes.append([])
            ncols = len(row)

        row_t, row_k, any_value = [], [], False
        for v in row:
            if v.__class__ is str and v and v.isprintable() and v[0] != '=' and len(v) <= EXCEL_MAX_CELL_CHARS:
                t, k = v, T_STR
            else:
                t, k = encode_cell(v, stats)
            row_t.append(t)
            row_k.append(k)
            if k:
                any_value = True

        if not any_value:
            pending_empty += 1        # only kept if a data row follows
            continue
        for _ in range(pending_empty):
            append_row([], [])
        pending_empty = 0
        append_row(row_t, row_k)
        total = rid
        if len(rids) >= INGEST_CHUNK_ROWS:
            flush()
            status_cb(f"Reading sheet '{sheet_name}': {total:,} rows...")
    flush()
    return ncols, rid


# ===========================================================================
# 8. Excel output
# ===========================================================================

def verify_excel_output(path, expectations):
    """expectations: list of (sheet_title, header_rows, count, digest).
    Returns list of problem strings (empty = OK)."""
    problems = []
    for title, header_rows, exp_count, exp_digest in expectations:
        try:
            rows, _ = open_sheet_rows(path, title, header_rows)
            d = RowDigest()
            for row in rows:
                d.add([canonical_text(v) for v in row])
        except Exception as e:
            problems.append(f"sheet '{title}': could not re-read ({e})")
            continue
        if d.count != exp_count:
            problems.append(f"sheet '{title}': expected {exp_count} data rows, file has {d.count}")
        elif d.hexdigest() != exp_digest:
            problems.append(f"sheet '{title}': row count matches ({d.count}) but cell contents differ")
    return problems


def run_excel_job(job, status_cb):
    source_path, out_dir = job['source_path'], job['out_dir']
    sheets, tasks = job['sheets'], job['tasks']
    report = Report(job)
    temp_dir = os.path.join(out_dir, "duckdb_raw_layer")
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    db_path = os.path.join(temp_dir, 'raw_preservation.db')
    for p in (db_path, db_path + '.wal'):
        if os.path.exists(p):
            os.remove(p)
    con = duckdb.connect(db_path)
    try:
        _configure_duckdb(con, temp_dir)

        # --- header snapshots from a tiny "head" copy of the workbook -------
        status_cb("Reading header blocks...")
        snapshots = {}
        head_wb = None
        try:
            head_wb = load_head_workbook(source_path, {s['name']: s['header_rows'] for s in sheets}, default_rows=0)
        except Exception as e:
            report.warn(f"Could not read styled header blocks ({e}); headers are exported as plain values.")
        plain = bool(getattr(head_wb, '_plain_header', False)) if head_wb else True
        for s in sheets:
            hr = s['header_rows']
            if hr == 0:
                snapshots[s['name']] = snapshot_from_grid([], [], 0)
                continue
            snap = None
            if head_wb is not None and s['name'] in head_wb.sheetnames:
                ws = head_wb[s['name']]
                max_col = max(s['max_col'], ws.max_column or 0)
                try:
                    snap = snapshot_header_block(ws, hr, max_col, plain_source=plain)
                except Exception as e:
                    report.warn(f"Sheet '{s['name']}': header styling not preserved ({e}).")
            if snap is None:
                grid = s.get('grid') or []
                snap = snapshot_from_grid(grid, s.get('merges') or [], s['max_col'])
            snapshots[s['name']] = snap
        if head_wb is not None:
            head_wb.close()

        # --- MA_CSKCB column per sheet (from the actual header rows) -------
        ma_by_scope = {}
        for s in sheets:
            snap = snapshots[s['name']]
            idx = detect_ma_in_grid(snap['grid'], snap['max_col']) if s['header_rows'] else None
            s['ma_index'] = idx
            labels = build_column_labels(snap['grid'], snap['max_col']) if s['header_rows'] else \
                [f"Column_{c + 1}" for c in range(snap['max_col'])]
            s['key_index'] = {}
            for ci, lab in enumerate(labels):
                s['key_index'].setdefault(column_key(lab), ci)
            if idx is not None:
                s['key_index']['MACSKCB'] = idx
                ma_by_scope[s['name']] = f"col_{idx}"

        # --- ingest data ----------------------------------------------------
        stats = {'cleaned': 0, 'truncated': 0}
        for idx, s in enumerate(sheets):
            s['table'] = f"t{idx}"
            status_cb(f"Reading sheet '{s['name']}'...")
            ncols, nrows = ingest_excel_sheet(con, s['table'], source_path, s['name'], s['header_rows'],
                                              max(s['max_col'], snapshots[s['name']]['max_col']), stats, status_cb)
            s['ncols'], s['nrows'] = ncols, nrows
            report.source_rows[s['name']] = nrows
        if stats['cleaned']:
            report.warn(f"{stats['cleaned']} text cell(s) contained invisible control characters that Excel "
                        f"cannot store; those characters were removed.")
        if stats['truncated']:
            report.warn(f"{stats['truncated']} text cell(s) were longer than Excel's 32,767-character limit "
                        f"and were truncated.")

        def col_expr_for(sheet):
            def col_expr(cond):
                if cond.scope is None and sheet['name'] != cond.origin:
                    idx = sheet['key_index'].get(cond.key)
                else:
                    m = re.fullmatch(r'col_(\d+)', cond.col)
                    idx = int(m.group(1)) if m else None
                if idx is None or idx >= sheet['ncols']:
                    return None
                return f"c{idx}"
            return col_expr

        if job.get('auto_split_ma'):
            values = set()
            for s in sheets:
                i = s['ma_index']
                if i is not None and i < s['ncols']:
                    values |= {v for (v,) in con.execute(
                        f"SELECT DISTINCT trim(c{i}) FROM {qident(s['table'])} WHERE trim(c{i}) <> ''").fetchall()}
            tasks = ma_split_tasks(values, lambda v: Cond(None, '__ma__', 'EXACT', (v,), 'MACSKCB', BATCH_ORIGIN),
                                   report)

        # --- tasks ------------------------------------------------------------
        namer = NameAllocator(out_dir)
        total = len(tasks)
        for ti, (task_label, task_conds) in enumerate(tasks, start=1):
            status_cb(f"Processing part {ti} of {total}: {task_label}")
            wb = openpyxl.Workbook(write_only=True)
            written, expectations, ma_values, sheet_counts = [], [], set(), {}

            for s in sheets:
                name = s['name']
                conds = applicable_conds(task_conds, name)
                where, missing = build_where(conds, col_expr_for(s))
                force_empty = bool(missing)
                if missing:
                    report.warn_once(f"Sheet '{name}' has no column {', '.join(missing)} used by a global filter "
                                     f"-> treated as no match.")
                count = 0 if force_empty else con.execute(
                    f'SELECT COUNT(*) FROM {qident(s["table"])} {where_sql(where)}').fetchone()[0]
                s.setdefault('wheres', []).append(None if force_empty else where)

                if count == 0:
                    if s['drop_empty']:
                        report.note(task_label, f"sheet '{name}': no matching rows (sheet skipped)")
                        continue
                    report.note(task_label, f"sheet '{name}': no matching rows (empty sheet kept)")

                ws = wb.create_sheet(title=name[:31])
                snap = snapshots[name]
                write_header_snapshot(ws, snap)
                digest = RowDigest()
                if count:
                    ncols = s['ncols']
                    included = [i not in s['excluded'] for i in range(ncols)]
                    select_cols = ', '.join(f"c{i}, k{i}" for i in range(ncols))
                    cur = con.execute(f'SELECT {select_cols} FROM {qident(s["table"])} '
                                      f'{where_sql(where)} ORDER BY __rid')
                    done = 0
                    while True:
                        batch = cur.fetchmany(FETCH_CHUNK_ROWS)
                        if not batch:
                            break
                        for rec in batch:
                            out, htexts = [], []
                            for i in range(ncols):
                                t = rec[2 * i]
                                if t is None or not included[i]:
                                    out.append(None)
                                    htexts.append('')
                                else:
                                    out.append(excel_out_value(ws, t, rec[2 * i + 1]))
                                    htexts.append(t)
                            ws.append(out)
                            digest.add(htexts)
                        done += len(batch)
                        if count > FETCH_CHUNK_ROWS:
                            status_cb(f"Part {ti}/{total}: writing '{name}' {done:,}/{count:,} rows...")
                    if s['ma_index'] is not None and s['ma_index'] < ncols:
                        ma_col = f"c{s['ma_index']}"
                        extra = f"{' AND ' if where else 'WHERE '}trim({ma_col}) <> ''"
                        for (v,) in con.execute(f'SELECT DISTINCT trim({ma_col}) FROM {qident(s["table"])} '
                                                f'{where_sql(where)} {extra} LIMIT 2').fetchall():
                            ma_values.add(v)
                written.append(name)
                sheet_counts[name] = count
                expectations.append((name[:31], s['header_rows'], digest.count, digest.hexdigest()))

            if not written:
                report.add_file(task_label, None, {}, "no sheet had matching rows -> no file created")
                continue

            ma_value = pick_ma_value(ma_values, task_conds, ma_by_scope)
            base = build_output_basename(ma_value, '_'.join(written))
            final_path = namer.allocate(base, '.xlsx')
            tmp_path = os.path.join(out_dir, f"~splitting_{os.getpid()}_{ti}.xlsx")
            status_cb(f"Part {ti}/{total}: saving {os.path.basename(final_path)}...")
            wb.save(tmp_path)
            os.replace(tmp_path, final_path)

            verify_msg = None
            if job['verify']:
                status_cb(f"Part {ti}/{total}: verifying {os.path.basename(final_path)}...")
                problems = verify_excel_output(final_path, expectations)
                verify_msg = "OK" if not problems else "FAILED: " + "; ".join(problems)
            report.add_file(task_label, final_path, sheet_counts, verify_msg)

        # --- coverage ---------------------------------------------------------
        if any(t[1] for t in tasks):
            for s in sheets:
                wheres = s.get('wheres', [])
                report.coverage[s['name']] = _coverage(con, qident(s['table']), s['nrows'], wheres)
    finally:
        con.close()
        _cleanup_temp(temp_dir, db_path)
    return report


# ===========================================================================
# 9. CSV / SQLite outputs
# ===========================================================================

def _configure_duckdb(con, temp_dir):
    con.execute("SET memory_limit='4GB';")
    con.execute(f"SET threads={max(1, min(4, os.cpu_count() or 2))};")
    con.execute("SET preserve_insertion_order=true;")
    con.execute(f"SET temp_directory={sql_str(temp_dir.replace(chr(92), '/'))};")


def _cleanup_temp(temp_dir, db_path=None):
    try:
        if db_path:
            for p in (db_path, db_path + '.wal'):
                if os.path.exists(p):
                    os.remove(p)
        if os.path.exists(temp_dir) and not os.listdir(temp_dir):
            os.rmdir(temp_dir)
    except Exception:
        pass


def _coverage(con, table_sql, total_rows, wheres):
    """How many source rows ended up in at least one output file."""
    if any(w == '' for w in wheres):
        return total_rows, total_rows
    real = sorted({w for w in wheres if w})
    if not real:
        return total_rows, 0
    union = ' OR '.join(f'({w})' for w in real)
    covered = con.execute(f'SELECT COUNT(*) FROM {table_sql} WHERE {union}').fetchone()[0]
    return total_rows, covered


def csv_source_sql(path):
    return f"read_csv_auto({sql_str(path)}, ALL_VARCHAR=TRUE)"


def read_csv_columns(path):
    con = duckdb.connect()
    try:
        return [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {csv_source_sql(path)}").fetchall()]
    finally:
        con.close()


def run_csv_job(job, status_cb):
    source_path, out_dir = job['source_path'], job['out_dir']
    sheet = job['sheets'][0]
    report = Report(job)
    temp_dir = os.path.join(out_dir, "duckdb_raw_layer")
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    db_path = os.path.join(temp_dir, 'raw_preservation.db')
    for p in (db_path, db_path + '.wal'):
        if os.path.exists(p):
            os.remove(p)
    con = duckdb.connect(db_path)
    try:
        _configure_duckdb(con, temp_dir)
        status_cb("Ingesting CSV...")
        con.execute(f"CREATE TABLE t_csv AS SELECT * FROM {csv_source_sql(source_path)}")
        cols = [r[0] for r in con.execute("DESCRIBE t_csv").fetchall()]
        nrows = con.execute("SELECT COUNT(*) FROM t_csv").fetchone()[0]
        report.source_rows[sheet['name']] = nrows

        selected = [c for c in cols if c not in sheet['excluded_names']] or cols
        select_sql = ', '.join(qident(c) for c in selected)
        ma_col = detect_ma_in_names(cols)
        ma_by_scope = {sheet['name']: ma_col} if ma_col else {}
        stem = os.path.splitext(os.path.basename(source_path))[0]
        def col_expr(c):
            name = resolve_named_col(c, cols)
            return qident(name) if name else None
        tasks = job['tasks']
        if job.get('auto_split_ma') and ma_col:
            vals = [v for (v,) in con.execute(f"SELECT DISTINCT trim({qident(ma_col)}) FROM t_csv "
                                              f"WHERE trim({qident(ma_col)}) <> ''").fetchall()]
            tasks = ma_split_tasks(vals, lambda v: Cond(sheet['name'], ma_col, 'EXACT', (v,), 'MACSKCB',
                                                        sheet['name']), report)
        hash_expr = 'sum(hash(' + ', '.join(f"coalesce({qident(c)}, '')" for c in selected) + '))'

        namer = NameAllocator(out_dir)
        wheres = []
        for ti, (task_label, task_conds) in enumerate(tasks, start=1):
            status_cb(f"Processing part {ti} of {len(tasks)}: {task_label}")
            where, missing = build_where(applicable_conds(task_conds, sheet['name']), col_expr)
            if missing:
                report.warn_once(f"CSV has no column {', '.join(missing)} -> treated as no match.")
                wheres.append(None)
                count = 0
            else:
                wheres.append(where)
                count = con.execute(f"SELECT COUNT(*) FROM t_csv {where_sql(where)}").fetchone()[0]
            if count == 0 and sheet['drop_empty']:
                report.add_file(task_label, None, {}, "no matching rows -> no file created")
                continue

            ma_value = None
            if ma_col and count and not missing:
                extra = f"{' AND ' if where else 'WHERE '}trim({qident(ma_col)}) <> ''"
                vals = {v for (v,) in con.execute(f"SELECT DISTINCT trim({qident(ma_col)}) FROM t_csv "
                                                  f"{where_sql(where)} {extra} LIMIT 2").fetchall()}
                ma_value = pick_ma_value(vals, task_conds, ma_by_scope)
            elif ma_col:
                ma_value = ma_from_filters(task_conds, ma_by_scope)

            final_path = namer.allocate(build_output_basename(ma_value, None if ma_value else stem), '.csv')
            tmp_path = os.path.join(out_dir, f"~splitting_{os.getpid()}_{ti}.tmp")
            query = f"SELECT {select_sql} FROM t_csv {'WHERE FALSE' if missing else where_sql(where)}"
            con.execute(f"COPY ({query}) TO {sql_str(tmp_path)} (HEADER, DELIMITER ',', QUOTE '\"')")
            # Prepend a UTF-8 BOM so Excel shows Vietnamese text correctly.
            with open(final_path, 'wb') as fout, open(tmp_path, 'rb') as fin:
                fout.write(b'\xef\xbb\xbf')
                while True:
                    chunk = fin.read(_ZIP_CHUNK)
                    if not chunk:
                        break
                    fout.write(chunk)
            os.remove(tmp_path)

            verify_msg = None
            if job['verify']:
                exp = con.execute(f"SELECT COUNT(*), {hash_expr} FROM ({query})").fetchone()
                rel = (f"read_csv({sql_str(final_path)}, header=true, all_varchar=true, delim=',', "
                       f"quote='\"', names=[{', '.join(sql_str(c) for c in selected)}])")
                got = con.execute(f"SELECT COUNT(*), {hash_expr} FROM {rel}").fetchone()
                verify_msg = "OK" if tuple(exp) == tuple(got) else \
                    f"FAILED: expected {exp[0]} rows, file has {got[0]} (or contents differ)"
            report.add_file(task_label, final_path, {sheet['name']: count}, verify_msg)

        if any(t[1] for t in tasks):
            report.coverage[sheet['name']] = _coverage(con, 't_csv', nrows, wheres)
    finally:
        con.close()
        _cleanup_temp(temp_dir, db_path)
    return report


class JobCancelled(Exception):
    pass


class RawText(bytes):
    """A TEXT value that is not valid UTF-8. Kept as raw bytes and written
    back byte-for-byte as TEXT, so nothing is lost or altered."""
    __slots__ = ()

    def __repr__(self):
        return 'RawText(' + bytes.__repr__(self) + ')'


def _safe_text_factory(b):
    try:
        return b.decode('utf-8')
    except UnicodeDecodeError:
        return RawText(b)


def _is_decode_error(e):
    return isinstance(e, sqlite3.OperationalError) and 'decode' in str(e).lower()


def _vn_contains(blob, needle_lower):
    """Unicode-aware, case-insensitive 'contains' (SQLite's lower() is ASCII only)."""
    if blob is None:
        return 0
    s = blob.decode('utf-8', 'replace') if isinstance(blob, bytes) else str(blob)
    return 1 if needle_lower in s.lower() else 0


def _sqlite_key_expr(col):
    """Canonical text of a cell, identical to the Excel codec: 79001.0 -> '79001',
    surrounding spaces removed."""
    c = qident(col)
    return (f"(CASE WHEN typeof({c})='real' AND abs({c}) < 1e15 AND {c} = CAST({c} AS INTEGER) "
            f"THEN CAST(CAST({c} AS INTEGER) AS TEXT) ELSE trim(CAST({c} AS TEXT)) END)")


def _open_sqlite_source(path, safe=False):
    try:
        con = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)
        con.execute('SELECT 1 FROM sqlite_master LIMIT 1').fetchall()
    except sqlite3.Error:
        con = sqlite3.connect(path)
    con.execute('PRAGMA query_only=1')
    con.execute('PRAGMA cache_size=-32768')
    con.create_function('vn_contains', 2, _vn_contains, deterministic=True)
    if safe:
        con.text_factory = _safe_text_factory
    return con


def _row_checksum_sql(cols):
    """Per-row fingerprint computed inside SQLite (cheap, no character counting):
    for every cell its storage type, integer/real value, text/blob byte length
    and first character, weighted by column position."""
    parts = []
    for i, c in enumerate(cols, start=1):
        q = qident(c)
        e = (f"CASE typeof({q}) WHEN 'null' THEN 1 "
             f"WHEN 'integer' THEN 2 + 5*({q} % 1000003) "
             f"WHEN 'real' THEN 3 + 7*(CAST({q}*1000 AS INTEGER) % 1000003) "
             f"WHEN 'text' THEN 4 + 5*length(CAST({q} AS BLOB)) + 7*coalesce(unicode({q}),0) "
             f"ELSE 6 + 5*length({q}) END")
        parts.append(f"{i}*({e})")
    return f"(({' + '.join(parts)}) % 1000000007)"


class _NativeOut:
    """One output .db file. Rows are copied by SQLite itself (source attached
    read-only), so values are never converted or decoded by Python."""

    def __init__(self, path, src_uri, create_sql, cache_kb, cancel):
        self.path = path
        if os.path.exists(path):
            os.remove(path)
        self.conn = sqlite3.connect(path, isolation_level=None, uri=True)
        for pragma in ('journal_mode=OFF', 'synchronous=OFF', 'main.locking_mode=EXCLUSIVE',
                       'temp_store=MEMORY', f'cache_size=-{int(cache_kb)}'):
            self.conn.execute(f'PRAGMA {pragma}')
        self.conn.execute(create_sql)
        self.conn.execute('ATTACH DATABASE ? AS src', (src_uri,))
        self.conn.execute('PRAGMA src.cache_size=-1024')
        self.conn.create_function('vn_contains', 2, _vn_contains, deterministic=True)
        if cancel is not None:
            self.conn.set_progress_handler(lambda: 1 if cancel.is_set() else 0, 200_000)
        self.conn.execute('BEGIN')
        self.count = 0           # rows inserted (from SQLite)
        self.exp_count = 0       # rows the filter matched (independent count)
        self.exp_sum = 0
        self.ma = set()

    def insert(self, sql, params):
        self.count += self.conn.execute(sql, params).rowcount

    def finish(self):
        self.conn.execute('COMMIT')
        self.conn.execute('DETACH DATABASE src')

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def abort(self):
        self.close()
        try:
            os.remove(self.path)
        except OSError:
            pass


def _fmt_eta(seconds):
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


def run_db_job(job, status_cb):
    """Filter / split one SQLite table reading the source ONCE, sequentially.

    The table is processed in rowid chunks of ~64 MB. For each chunk SQLite
    itself (C speed) counts/fingerprints the matching rows and copies them into
    every output file; the chunk is read from disk once and re-used from the
    OS cache. Python never touches individual rows, values are copied exactly."""
    source_db, out_dir = job['source_path'], job['out_dir']
    sheet = job['sheets'][0]
    table = sheet['name']
    cancel = job.get('cancel')
    verify = job['verify']
    report = Report(job)
    tmp_tag = f"~splitting_{os.getpid()}_{threading.get_ident()}"
    src_uri = Path(source_db).resolve().as_uri() + '?mode=ro'
    T = qident(table)

    fast = _open_sqlite_source(source_db, safe=True)
    if cancel is not None:
        fast.set_progress_handler(lambda: 1 if cancel.is_set() else 0, 200_000)
    outs = []
    try:
        info = fast.execute(f"PRAGMA table_info({sql_str(table)})").fetchall()
        if not info:
            raise ValueError(f"table '{table}' not found")
        cols = [r[1] for r in info]
        types = {r[1]: (r[2] or '') for r in info}
        create_src = (fast.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                                   (table,)).fetchone() or [''])[0] or ''
        without_rowid = bool(re.search(r'\bWITHOUT\s+ROWID\b', create_src, re.I))
        lower_cols = {c.lower() for c in cols}
        rid = next((n for n in ('rowid', '_rowid_', 'oid') if n not in lower_cols), None)
        if rid is None:
            without_rowid = True

        selected = [c for c in cols if c not in sheet['excluded_names']] or cols
        sel_sql = ', '.join(qident(c) for c in selected)
        ma_col = detect_ma_in_names(cols)
        ma_by_scope = {table: ma_col} if ma_col else {}
        K_ma = _sqlite_key_expr(ma_col) if ma_col else None
        auto = bool(job.get('auto_split_ma')) and ma_col is not None
        if job.get('auto_split_ma') and ma_col is None:
            report.warn("Auto-split by MA_CSKCB: table has no MA_CSKCB column -> exported as one file.")

        # ---- tasks -> SQL conditions -------------------------------------------
        def cond_sql(name, op, values):
            k = _sqlite_key_expr(name)
            if op == 'EXACT':
                return f"{k} IN ({', '.join(sql_str(v) for v in values)})"
            return '(' + ' OR '.join(f"vn_contains(CAST({k} AS BLOB), {sql_str(v.lower())})"
                                     for v in values) + ')'

        tasks = [] if auto else (job['tasks'] or [("full export (no filter)", [])])
        task_where = {}            # task index -> SQL condition
        for i, (label, conds) in enumerate(tasks):
            parts, ok = [], True
            for c in applicable_conds(conds, table):
                name = resolve_named_col(c, cols)
                if name is None:
                    report.warn_once(f"Table '{table}' has no column '{c.key or c.col}' -> treated as no match.")
                    ok = False
                    break
                parts.append(cond_sql(name, c.op, tuple(c.values)))
            if ok:
                task_where[i] = ' AND '.join(f'({p})' for p in parts) or '1'
        any_where = ' OR '.join(f'({w})' for w in task_where.values()) or '0'
        rowsum = _row_checksum_sql(selected) if verify else None

        col_defs = ', '.join(f"{qident(c)} {types[c]}".rstrip() for c in selected)
        create_sql = f"CREATE TABLE {T} ({col_defs})"

        def new_out(cache_kb):
            o = _NativeOut(os.path.join(out_dir, f"{tmp_tag}_{len(outs)}.db"), src_uri, create_sql, cache_kb, cancel)
            outs.append(o)
            return o

        task_out = {}
        if not auto:
            ck = max(2048, 524288 // max(1, len(task_where)))
            for i in range(len(tasks)):
                if i in task_where or not sheet['drop_empty']:
                    task_out[i] = new_out(ck)
        auto_out = {}
        totals = {'rows': 0, 'covered': 0, 'no_ma': 0}

        # ---- statements ------------------------------------------------------------
        rng = f"{rid} BETWEEN ? AND ?" if not without_rowid else "1"
        order = f" ORDER BY {rid}" if not without_rowid else ""
        ins_task = {i: f"INSERT INTO main.{T} ({sel_sql}) SELECT {sel_sql} FROM src.{T} "
                       f"WHERE {rng} AND ({w}){order}" for i, w in task_where.items()}
        ins_auto = (f"INSERT INTO main.{T} ({sel_sql}) SELECT {sel_sql} FROM src.{T} "
                    f"WHERE {rng} AND {K_ma} = CAST(? AS TEXT){order}") if auto else None

        if auto:
            stat_sql = (f"SELECT {K_ma}, count(*)" + (f", sum({rowsum})" if verify else "") +
                        f" FROM {T} WHERE {rng} GROUP BY 1")
        else:
            cols_stat = ["count(*)", f"sum(CASE WHEN {any_where} THEN 1 ELSE 0 END)"]
            for i in sorted(task_where):
                w = task_where[i]
                cols_stat.append(f"sum(CASE WHEN {w} THEN 1 ELSE 0 END)")
                if verify:
                    cols_stat.append(f"sum(CASE WHEN {w} THEN {rowsum} END)")
                if K_ma:
                    cols_stat.append(f"min(CASE WHEN ({w}) AND {K_ma} <> '' THEN {K_ma} END)")
                    cols_stat.append(f"max(CASE WHEN ({w}) AND {K_ma} <> '' THEN {K_ma} END)")
            stat_sql = f"SELECT {', '.join(cols_stat)} FROM {T} WHERE {rng}"

        def process_range(params):
            # 1) independent count + fingerprint of the rows each output must get
            if auto:
                for key, n, *rest in fast.execute(stat_sql, params).fetchall():
                    totals['rows'] += n
                    if key is None or key == '':
                        totals['no_ma'] += n
                        continue
                    o = auto_out.get(key)
                    if o is None:
                        o = auto_out[key] = new_out(4096)
                    o.exp_count += n
                    if verify:
                        o.exp_sum += rest[0] or 0
                    totals['covered'] += n
                    # 2) copy (inside SQLite)
                    o.insert(ins_auto, params + (bytes(key) if isinstance(key, bytes) else key,))
                return
            row = fast.execute(stat_sql, params).fetchone()
            totals['rows'] += row[0] or 0
            totals['covered'] += row[1] or 0
            pos = 2
            for i in sorted(task_where):
                n = row[pos] or 0
                pos += 1
                o = task_out[i]
                o.exp_count += n
                if verify:
                    o.exp_sum += row[pos] or 0
                    pos += 1
                if K_ma:
                    for v in (row[pos], row[pos + 1]):
                        if v and not isinstance(v, bytes) and len(o.ma) < 2:
                            o.ma.add(v)
                    pos += 2
                if n:
                    o.insert(ins_task[i], params)

        # ---- the single pass ----------------------------------------------------------
        file_size = os.path.getsize(source_db)
        started = dt.datetime.now()
        if task_where or auto:
            if without_rowid:
                status_cb(f"Reading '{table}' (WITHOUT ROWID table: no progress percentage)...")
                process_range(())
            else:
                lo = fast.execute(f"SELECT min({rid}) FROM {T}").fetchone()[0]   # two separate queries:
                hi = fast.execute(f"SELECT max({rid}) FROM {T}").fetchone()[0]   # min()+max() together = full scan
                if lo is not None:
                    width = hi - lo + 1
                    bytes_per_row = max(1.0, file_size / width)
                    span = int(min(1_000_000, max(2_000, (64 << 20) / bytes_per_row, width / 5000)))
                    pos = lo
                    while pos <= hi:
                        if cancel is not None and cancel.is_set():
                            raise JobCancelled()
                        end = min(hi, pos + span - 1)
                        process_range((pos, end))
                        pos = end + 1
                        frac = (end - lo + 1) / width
                        el = (dt.datetime.now() - started).total_seconds()
                        eta = f", ETA {_fmt_eta(el / frac - el)}" if frac > 0.002 else ""
                        status_cb(f"Reading '{table}': {frac * 100:.1f}% (~{frac * file_size / 2**30:.1f} of "
                                  f"{file_size / 2**30:.1f} GB), {totals['covered']:,} matching rows, "
                                  f"elapsed {_fmt_eta(el)}{eta}")

        # ---- finish ----------------------------------------------------------------------
        status_cb(f"Finishing output files for '{table}'...")
        for o in outs:
            o.finish()

        namer = NameAllocator(out_dir)

        def place(o, ma_value, label):
            if o.count != o.exp_count:
                msg = f"FAILED: filter matched {o.exp_count} rows but {o.count} were written"
            else:
                msg = None
            if o.count == 0 and sheet['drop_empty'] and msg is None:
                o.abort()
                report.add_file(label, None, {}, "no matching rows -> no file created")
                return
            if verify and msg is None:
                status_cb(f"Verifying {label}...")
                n, sm = o.conn.execute(f"SELECT count(*), sum({rowsum}) FROM main.{T}").fetchone()
                if n != o.exp_count:
                    msg = f"FAILED: expected {o.exp_count} rows, file has {n}"
                elif (sm or 0) != o.exp_sum:
                    msg = f"FAILED: row count matches ({n}) but cell contents differ"
                else:
                    msg = "OK"
            o.close()
            final = namer.allocate(build_output_basename(ma_value, None if ma_value else table), '.db')
            os.replace(o.path, final)
            o.path = final
            report.add_file(label, final, {table: o.count}, msg)

        if auto:
            if not auto_out:
                report.warn("Auto-split by MA_CSKCB: no MA_CSKCB value found -> no file created.")
            for key in sorted(auto_out, key=lambda k: (isinstance(k, bytes), k)):
                name = key.decode('utf-8', 'replace') if isinstance(key, bytes) else key
                place(auto_out[key], name, f"MA_CSKCB = {name}")
        else:
            for i, (label, conds) in enumerate(tasks):
                o = task_out.get(i)
                if o is None:
                    report.add_file(label, None, {}, "filter column missing -> no file created")
                    continue
                place(o, pick_ma_value(o.ma, conds, ma_by_scope) if ma_col else None, label)
        outs = []

        report.source_rows[table] = totals['rows']
        if task_where or auto:
            report.coverage[table] = (totals['rows'], totals['covered'])
        if totals['no_ma']:
            report.warn(f"{totals['no_ma']:,} row(s) have an empty MA_CSKCB -> not placed in any file.")
        return report
    except sqlite3.OperationalError as e:
        if 'interrupt' in str(e).lower() and cancel is not None and cancel.is_set():
            for o in outs:
                o.abort()
            outs = []
            raise JobCancelled()
        raise
    finally:
        for o in outs:
            o.abort()
        fast.close()


# ===========================================================================
# 9b. Batch mode: folders and .zip / .rar archives
# ===========================================================================

_SKIP_DIR_PREFIXES = ('~split_work_', 'duckdb_raw_layer', 'duckdb_temp_workspace')


def is_batch_source(path):
    return os.path.isdir(path) or os.path.splitext(path)[1].lower() in ARCHIVE_EXTS


def _is_supported_name(name):
    base = os.path.basename(name)
    if base.startswith(('~$', '~splitting_', '.')):
        return False
    return os.path.splitext(base)[1].lower() in SUPPORTED_EXTS


def _safe_rel_parts(rel):
    """Split an archive/folder relative path into safe path parts (no '..')."""
    parts = []
    for p in re.split(r'[\\/]+', rel):
        if p in ('', '.', '..'):
            continue
        p = sanitize_name_part(p)
        if p:
            parts.append(p[:MAX_NAME_LEN])
    return parts


def iter_batch_units(root, recursive, out_root=None):
    """Yield ('file' | 'archive', relative_display_path, absolute_path)."""
    if os.path.isfile(root):
        yield 'archive', os.path.basename(root), root
        return
    out_abs = os.path.normcase(os.path.abspath(out_root)) if out_root else None
    root_abs = os.path.normcase(os.path.abspath(root))
    for dirpath, dirnames, filenames in os.walk(root):
        keep = []
        for d in sorted(dirnames):
            full = os.path.normcase(os.path.abspath(os.path.join(dirpath, d)))
            if d.startswith(_SKIP_DIR_PREFIXES) or d == '_split_output':
                continue        # temp folders / default output folder of earlier runs
            if out_abs and out_abs != root_abs and full == out_abs:
                continue        # never re-read our own output folder
            keep.append(d)
        dirnames[:] = keep if recursive else []
        for f in sorted(filenames):
            full = os.path.join(dirpath, f)
            rel = os.path.relpath(full, root)
            ext = os.path.splitext(f)[1].lower()
            if ext in ARCHIVE_EXTS and not f.startswith('~$'):
                yield 'archive', rel, full
            elif _is_supported_name(f):
                yield 'file', rel, full


# --- ZIP -------------------------------------------------------------------

def _zip_member_name(info):
    name = info.filename
    if info.flag_bits & 0x800:          # UTF-8 flag set
        return name
    try:
        raw = name.encode('cp437')
    except UnicodeEncodeError:
        return name
    for enc in ('utf-8', 'cp1258', 'cp1252'):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return name


def _zip_list(path):
    with zipfile.ZipFile(path) as zf:
        return [(_zip_member_name(i), i.file_size) for i in zf.infolist()
                if not i.is_dir() and _is_supported_name(_zip_member_name(i))]


def _zip_extract(path, dest):
    out = []
    with zipfile.ZipFile(path) as zf:
        for info in zf.infolist():
            name = _zip_member_name(info)
            if info.is_dir() or not _is_supported_name(name):
                continue
            parts = _safe_rel_parts(name)
            if not parts:
                continue
            target = os.path.join(dest, *parts)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with zf.open(info) as src, open(target, 'wb') as dst:
                shutil.copyfileobj(src, dst, _ZIP_CHUNK)
            out.append(('/'.join(parts), target))
    return out


# --- RAR (needs 7-Zip, WinRAR's UnRAR or the 'rarfile' package) -------------

def _find_rar_tool():
    candidates = [('7z', shutil.which('7z')), ('7z', shutil.which('7zz')), ('7z', shutil.which('7za')),
                  ('unrar', shutil.which('unrar')), ('unrar', shutil.which('UnRAR'))]
    for base in (os.environ.get('ProgramFiles'), os.environ.get('ProgramFiles(x86)'),
                 r'C:\Program Files', r'C:\Program Files (x86)'):
        if base:
            candidates.append(('7z', os.path.join(base, '7-Zip', '7z.exe')))
            candidates.append(('unrar', os.path.join(base, 'WinRAR', 'UnRAR.exe')))
    # macOS: an app launched from Finder has a minimal PATH, so Homebrew/MacPorts folders are not searched by shutil.which
    for base in ('/opt/homebrew/bin', '/usr/local/bin', '/opt/local/bin'):
        for name in ('7zz', '7z', '7za'):
            candidates.append(('7z', os.path.join(base, name)))
        candidates.append(('unrar', os.path.join(base, 'unrar')))
    for kind, exe in candidates:
        if exe and os.path.isfile(exe):
            return kind, exe
    return None, None


def _run_tool(cmd):
    flags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    res = subprocess.run(cmd, capture_output=True, creationflags=flags)
    if res.returncode != 0:
        msg = (res.stderr or res.stdout or b'').decode('utf-8', 'replace').strip().splitlines()
        raise RuntimeError("archive tool failed: " + (msg[-1] if msg else f"exit code {res.returncode}"))
    return res.stdout


_RAR_HELP = ("Reading .rar needs 7-Zip or WinRAR installed (default install folder; on macOS: 'brew install sevenzip'), "
             "or 'pip install rarfile' plus an unrar tool.")


def _rar_list(path):
    kind, exe = _find_rar_tool()
    if kind == '7z':
        text = _run_tool([exe, 'l', '-slt', '-sccUTF-8', path]).decode('utf-8', 'replace')
        body = text.split('\n----------', 1)[-1]
        out = []
        for block in re.split(r'\r?\n\r?\n', body):
            fields = dict(line.split(' = ', 1) for line in block.splitlines() if ' = ' in line)
            name = fields.get('Path')
            if name and fields.get('Folder', '-') != '+' and _is_supported_name(name):
                out.append((name.replace('\\', '/'), int(fields.get('Size') or 0)))
        return out
    if kind == 'unrar':
        raw = _run_tool([exe, 'lb', path])
        try:
            text = raw.decode('utf-8')
        except UnicodeDecodeError:
            text = raw.decode('mbcs' if os.name == 'nt' else 'latin-1', 'replace')
        return [(n.strip().replace('\\', '/'), 0) for n in text.splitlines() if _is_supported_name(n.strip())]
    try:
        import rarfile
        with rarfile.RarFile(path) as rf:
            return [(i.filename, i.file_size) for i in rf.infolist()
                    if not i.is_dir() and _is_supported_name(i.filename)]
    except ImportError:
        raise RuntimeError(_RAR_HELP)


def _rar_extract(path, dest):
    kind, exe = _find_rar_tool()
    if kind == '7z':
        _run_tool([exe, 'x', '-y', '-bso0', '-bsp0', f'-o{dest}', path])
    elif kind == 'unrar':
        _run_tool([exe, 'x', '-y', '-o+', '-idq', path, dest + os.sep])
    else:
        try:
            import rarfile
        except ImportError:
            raise RuntimeError(_RAR_HELP)
        with rarfile.RarFile(path) as rf:
            for info in rf.infolist():
                if info.is_dir() or not _is_supported_name(info.filename):
                    continue
                parts = _safe_rel_parts(info.filename)
                target = os.path.join(dest, *parts)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with rf.open(info) as src, open(target, 'wb') as dst:
                    shutil.copyfileobj(src, dst, _ZIP_CHUNK)
    out = []
    for dirpath, _dirs, files in os.walk(dest):
        for f in sorted(files):
            if _is_supported_name(f):
                full = os.path.join(dirpath, f)
                out.append((os.path.relpath(full, dest).replace(os.sep, '/'), full))
    return sorted(out)


def archive_list(path):
    return _zip_list(path) if path.lower().endswith('.zip') else _rar_list(path)


def archive_extract(path, dest):
    return _zip_extract(path, dest) if path.lower().endswith('.zip') else _rar_extract(path, dest)


def list_batch_entries(root, recursive, out_root=None):
    """Rows for the batch file list: (display_path, type, size_bytes, error)."""
    rows = []
    for kind, rel, path in iter_batch_units(root, recursive, out_root):
        if kind == 'file':
            rows.append((rel, os.path.splitext(rel)[1].lower(), os.path.getsize(path), None))
            continue
        prefix = '' if os.path.isfile(root) else rel + ' > '
        try:
            members = archive_list(path)
            if not members:
                rows.append((rel, 'archive', os.path.getsize(path), "no supported file inside"))
            for name, size in members:
                rows.append((prefix + name, os.path.splitext(name)[1].lower(), size, None))
        except Exception as e:
            rows.append((rel, 'archive', os.path.getsize(path), str(e)))
    return rows


def batch_ma_tasks(text):
    """Parse the batch MA_CSKCB filter box (same syntax as the column filters)."""
    text = text.strip()
    if not text:
        return []
    op = 'EXACT'
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ('"', "'"):
        op = 'CLOSEST'
        text = text[1:-1].strip()
    if ',' in text:
        groups = [(v,) for v in dict.fromkeys(v.strip() for v in text.split(',')) if v]
    elif ';' in text:
        vals = tuple(v for v in dict.fromkeys(v.strip() for v in text.split(';')) if v)
        groups = [vals] if vals else []
    else:
        groups = [(text,)]
    sign = '=' if op == 'EXACT' else 'contains'
    return [(f"MA_CSKCB {sign} {' OR '.join(g)}", [Cond(None, '__ma__', op, g, 'MACSKCB', BATCH_ORIGIN)])
            for g in groups]


def build_auto_jobs(path, out_dir, opts):
    """Jobs for one source file with default settings (auto-detected headers,
    every sheet/table exported, empty results dropped)."""
    ext = os.path.splitext(path)[1].lower()
    tasks = opts['tasks'] or [("full export (no filter)", [])]
    base = {'source_path': path, 'out_dir': out_dir, 'tasks': tasks, 'verify': opts['verify'],
            'auto_split_ma': opts['auto_split'] and not opts['tasks']}
    if ext in EXCEL_EXTS:
        if not OPENPYXL_AVAILABLE:
            raise ImportError("openpyxl is required for Excel files.")
        wb = load_head_workbook(path)
        try:
            sheets = []
            for ws in wb.worksheets:
                info = scan_sheet_layout(ws)
                sheets.append({'name': ws.title, 'drop_empty': True, 'excluded': set(), 'excluded_names': set(),
                               'header_rows': info['header_rows'], 'max_col': info['max_col'],
                               'grid': info['grid'], 'merges': info['merges']})
        finally:
            wb.close()
        return [(run_excel_job, dict(base, sheets=sheets))]
    if ext == '.csv':
        sheet = {'name': os.path.basename(path), 'drop_empty': True, 'excluded': set(), 'excluded_names': set()}
        return [(run_csv_job, dict(base, sheets=[sheet]))]
    if ext in DB_EXTS:
        conn = sqlite3.connect(path)
        try:
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()]
        finally:
            conn.close()
        if not tables:
            raise ValueError("no tables found in the database")
        return [(run_db_job, dict(base, report_tag=t if len(tables) > 1 else None,
                                  sheets=[{'name': t, 'drop_empty': True, 'excluded': set(),
                                           'excluded_names': set()}])) for t in tables]
    raise ValueError(f"unsupported file type {ext}")


def run_batch(opts, status_cb, cancel_event):
    """opts: source, out_dir, recursive, separate_dirs, tasks, auto_split, verify."""
    root, out_root = opts['source'], opts['out_dir']
    Path(out_root).mkdir(parents=True, exist_ok=True)
    results = []      # dict(source, status, files, failed, report, error)
    counter = {'n': 0}

    def out_dir_for(rel_parts):
        if not opts['separate_dirs'] or not rel_parts:
            return out_root
        parts = list(rel_parts)
        parts[-1] = os.path.splitext(parts[-1])[0] or parts[-1]
        return os.path.join(out_root, *parts)

    def process_file(display, rel_parts, path):
        counter['n'] += 1
        n = counter['n']
        status_cb(f"[file {n}] {display}: preparing...")
        target = out_dir_for(rel_parts)
        try:
            Path(target).mkdir(parents=True, exist_ok=True)
            for runner, job in build_auto_jobs(path, target, opts):
                if cancel_event.is_set():
                    return
                job['display'] = display
                job['cancel'] = cancel_event
                rep = runner(job, lambda m: status_cb(f"[file {n}] {display}: {m}"))
                results.append({'source': display + (f" [{job['report_tag']}]" if job.get('report_tag') else ''),
                                'status': 'FAILED CHECK' if rep.failed_checks else 'OK',
                                'files': [f[1] for f in rep.created], 'warnings': list(rep.warnings),
                                'failed': rep.failed_checks, 'error': None})
        except JobCancelled:
            results.append({'source': display, 'status': 'STOPPED', 'files': [], 'warnings': [],
                            'failed': [], 'error': "stopped by user (partial output removed)"})
        except Exception as e:
            results.append({'source': display, 'status': 'ERROR', 'files': [], 'warnings': [],
                            'failed': [], 'error': str(e)})

    work_root = tempfile.mkdtemp(prefix='~split_work_', dir=out_root)
    root_is_archive = os.path.isfile(root)
    try:
        units = list(iter_batch_units(root, opts['recursive'], out_root))
        for kind, rel, path in units:
            if cancel_event.is_set():
                break
            if kind == 'file':
                process_file(rel, _safe_rel_parts(rel), path)
                continue
            status_cb(f"Extracting {rel}...")
            dest = tempfile.mkdtemp(dir=work_root)
            try:
                try:
                    members = archive_extract(path, dest)
                except Exception as e:
                    results.append({'source': rel, 'status': 'ERROR', 'files': [], 'warnings': [], 'failed': [],
                                    'error': f"cannot extract: {e}"})
                    continue
                if not members:
                    results.append({'source': rel, 'status': 'SKIPPED', 'files': [], 'warnings': [],
                                    'failed': [], 'error': "no supported file inside"})
                arch_parts = [] if root_is_archive else _safe_rel_parts(os.path.splitext(rel)[0])
                for m_rel, m_path in members:
                    if cancel_event.is_set():
                        break
                    display = m_rel if root_is_archive else f"{rel} > {m_rel}"
                    process_file(display, arch_parts + _safe_rel_parts(m_rel), m_path)
            finally:
                shutil.rmtree(dest, ignore_errors=True)
    finally:
        shutil.rmtree(work_root, ignore_errors=True)

    created = sum(len(r['files']) for r in results)
    bad = [r for r in results if r['status'] != 'OK']
    summary = [f"Processed {len(results)} source file(s), created {created} output file(s)."]
    if cancel_event.is_set():
        summary.append("Stopped by user before all files were processed.")
    if opts['verify']:
        n_fail = sum(1 for r in results if r['status'] == 'FAILED CHECK')
        summary.append("Integrity check: all output files match their source data." if not n_fail
                       else f"Integrity check FAILED for {n_fail} source file(s).")
    errs = [r for r in results if r['status'] in ('ERROR', 'SKIPPED', 'STOPPED')]
    if errs:
        summary.append("Problems:\n" + "\n".join(f"  {r['source']}: {r['error']}" for r in errs[:10])
                       + ("\n  ..." if len(errs) > 10 else ""))
    if created:
        summary.insert(1, f"Output folder: {out_root}")
    fails = [(r['source'], f) for r in results for f in r['failed']]
    if fails:
        summary.append("Failed checks:\n" + "\n".join(f"  {src}: {os.path.basename(f[1])}: {f[3]}"
                                                       for src, f in fails[:10]))
    warns = [f"{r['source']}: {w}" for r in results for w in r['warnings']]
    if warns:
        summary.append("Warnings:\n" + "\n".join("  - " + w for w in warns[:8]) +
                       (f"\n  ... and {len(warns) - 8} more" if len(warns) > 8 else ""))
    return "\n\n".join(summary), bool(bad)

# ===========================================================================
# 9c. Merge several .db files (same structure, any table name) into ONE table
# ===========================================================================

def sqlite_affinity(decl):
    """SQLite's column-affinity rules (https://sqlite.org/datatype3.html)."""
    d = (decl or '').upper()
    if 'INT' in d:
        return 'INTEGER'
    if 'CHAR' in d or 'CLOB' in d or 'TEXT' in d:
        return 'TEXT'
    if 'BLOB' in d or not d:
        return 'BLOB'
    if 'REAL' in d or 'FLOA' in d or 'DOUB' in d:
        return 'REAL'
    return 'NUMERIC'


def inspect_db_tables(path):
    """[(table_name, [(column, declared_type), ...]), ...] of a SQLite file."""
    con = _open_sqlite_source(path)
    try:
        names = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY rowid")]
        return [(t, [(r[1], r[2] or '') for r in con.execute(f"PRAGMA table_info({sql_str(t)})")]) for t in names]
    finally:
        con.close()


def plan_merge(paths):
    """Check every file against the first one. Returns (entries, ref_columns, ref_table):
    entries = [{'path', 'table', 'columns', 'error', 'note'}]. A file matches when it has
    a table with exactly the same column names (order and table name may differ)."""
    entries, ref_cols, ref_table, seen = [], None, None, set()
    for path in paths:
        e = {'path': path, 'table': None, 'columns': None, 'error': None, 'note': None}
        entries.append(e)
        key = os.path.normcase(os.path.abspath(path))
        if key in seen:
            e['error'] = "same file listed twice"
            continue
        seen.add(key)
        try:
            tables = inspect_db_tables(path)
        except Exception as ex:
            e['error'] = f"cannot open: {ex}"
            continue
        if not tables:
            e['error'] = "no table in this file"
            continue
        if ref_cols is None:
            e['table'], e['columns'] = tables[0]
            if len(tables) > 1:
                e['note'] = f"{len(tables)} tables, using '{tables[0][0]}'"
            ref_table, ref_cols = tables[0][0], tables[0][1]
            continue
        ref_set = {c.lower() for c, _ in ref_cols}
        matches = [t for t in tables if {c.lower() for c, _ in t[1]} == ref_set]
        if not matches:
            cols = {c.lower(): c for c, _ in tables[0][1]}
            missing = [c for c, _ in ref_cols if c.lower() not in cols]
            extra = [c for k, c in cols.items() if k not in ref_set]
            parts = []
            if missing:
                parts.append("missing " + ", ".join(missing[:5]) + (" ..." if len(missing) > 5 else ""))
            if extra:
                parts.append("extra " + ", ".join(extra[:5]) + (" ..." if len(extra) > 5 else ""))
            e['error'] = "columns differ from the first file: " + "; ".join(parts)
            continue
        pick = next((t for t in matches if t[0] == ref_table), matches[0])
        e['table'], e['columns'] = pick
        if len(matches) > 1:
            e['note'] = f"several matching tables, using '{pick[0]}'"
    return entries, ref_cols, ref_table


def run_db_merge(job, status_cb):
    """Append the matching table of every file into ONE table of a new .db.
    Rows are copied by SQLite itself (exact values, file order then row order);
    each source is read once. Returns (summary_text, has_problems)."""
    files, out_path, out_table = job['files'], job['out_path'], job['out_table']
    verify, cancel, add_src = job['verify'], job.get('cancel'), job.get('add_source_col')
    entries, ref_cols, _ref_table = plan_merge(files)
    bad = [e for e in entries if e['error']]
    if bad:
        raise ValueError("Some files cannot be merged:\n" + "\n".join(
            f"  {os.path.basename(e['path'])}: {e['error']}" for e in bad[:10]))
    if not entries:
        raise ValueError("No file to merge.")
    out_key = os.path.normcase(os.path.abspath(out_path))
    if any(os.path.normcase(os.path.abspath(e['path'])) == out_key for e in entries):
        raise ValueError("The output file is also one of the input files. Choose another output file.")

    warnings = []
    # A column whose type affinity differs between files is created WITHOUT a declared
    # type, so SQLite stores every value exactly as it is (no conversion at all).
    col_names = [c for c, _ in ref_cols]
    decl = {}
    for c, d in ref_cols:
        affs = set()
        for e in entries:
            dd = next(t for n, t in e['columns'] if n.lower() == c.lower())
            affs.add(sqlite_affinity(dd))
        if len(affs) > 1:
            decl[c] = ''
            warnings.append(f"Column {c} has different types across files ({' / '.join(sorted(affs))}); "
                            f"created without a declared type so every value is kept exactly.")
        else:
            decl[c] = d
    src_col = None
    if add_src:
        src_col = 'SOURCE_FILE'
        while src_col.lower() in {c.lower() for c in col_names}:
            src_col = '_' + src_col

    out_dir = os.path.dirname(os.path.abspath(out_path)) or '.'
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    total_bytes = sum(os.path.getsize(e['path']) for e in entries)
    free = shutil.disk_usage(out_dir).free
    if free < total_bytes * 1.05:
        raise ValueError(f"Not enough free disk space in the output folder: need about "
                         f"{total_bytes * 1.05 / 2**30:.1f} GB, only {free / 2**30:.1f} GB free.")

    tmp_path = os.path.join(out_dir, f"~merging_{os.getpid()}_{threading.get_ident()}.db")
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    T = qident(out_table)
    sel_sql = ', '.join(qident(c) for c in col_names)
    rowsum = _row_checksum_sql(col_names) if verify else None
    con = sqlite3.connect(tmp_path, isolation_level=None, uri=True)
    if cancel is not None:
        con.set_progress_handler(lambda: 1 if cancel.is_set() else 0, 200_000)
    results = []         # (path, table, rows, check_message)
    ok = False
    try:
        for pragma in ('journal_mode=OFF', 'synchronous=OFF', 'main.locking_mode=EXCLUSIVE',
                       'temp_store=MEMORY', 'cache_size=-262144'):
            con.execute(f'PRAGMA {pragma}')
        defs = [f"{qident(c)} {decl[c]}".rstrip() for c in col_names]
        if src_col:
            defs.append(f"{qident(src_col)} TEXT")
        con.execute(f"CREATE TABLE {T} ({', '.join(defs)})")
        ins_cols = sel_sql + (f", {qident(src_col)}" if src_col else "")

        done_bytes, started, out_rows = 0, dt.datetime.now(), 0
        for idx, e in enumerate(entries, start=1):
            name = os.path.basename(e['path'])
            size = os.path.getsize(e['path'])
            con.execute("ATTACH DATABASE ? AS src", (Path(e['path']).resolve().as_uri() + '?mode=ro',))
            try:
                S = f"src.{qident(e['table'])}"
                src_lower = {c.lower() for c, _ in e['columns']}
                create_src = (con.execute("SELECT sql FROM src.sqlite_master WHERE type='table' AND name=?",
                                          (e['table'],)).fetchone() or [''])[0] or ''
                rid = next((n for n in ('rowid', '_rowid_', 'oid') if n not in src_lower), None)
                without_rowid = rid is None or bool(re.search(r'\bWITHOUT\s+ROWID\b', create_src, re.I))
                extra = ", ?" if src_col else ""
                params_extra = (name,) if src_col else ()
                exp_n, exp_sum, got = 0, 0, 0
                first_out = out_rows + 1
                con.execute('BEGIN')

                def copy_range(where, params):
                    nonlocal exp_n, exp_sum, got
                    if verify:
                        n, sm = con.execute(f"SELECT count(*), sum({rowsum}) FROM {S} {where}", params).fetchone()
                        exp_n += n
                        exp_sum += sm or 0
                    order = f" ORDER BY {rid}" if not without_rowid else ""
                    got += con.execute(f"INSERT INTO main.{T} ({ins_cols}) SELECT {sel_sql}{extra} FROM {S} "
                                       f"{where}{order}", params_extra + params if src_col else params).rowcount

                if without_rowid:
                    status_cb(f"Merging file {idx}/{len(entries)} '{name}'...")
                    copy_range("", ())
                else:
                    lo = con.execute(f"SELECT min({rid}) FROM {S}").fetchone()[0]   # separate queries:
                    hi = con.execute(f"SELECT max({rid}) FROM {S}").fetchone()[0]   # min+max together = full scan
                    if lo is not None:
                        width = hi - lo + 1
                        span = int(min(1_000_000, max(2_000, (64 << 20) / max(1.0, size / width), width / 5000)))
                        pos = lo
                        while pos <= hi:
                            if cancel is not None and cancel.is_set():
                                raise JobCancelled()
                            end = min(hi, pos + span - 1)
                            copy_range(f"WHERE {rid} BETWEEN ? AND ?", (pos, end))
                            pos = end + 1
                            frac_file = (end - lo + 1) / width
                            frac = (done_bytes + frac_file * size) / max(1, total_bytes)
                            el = (dt.datetime.now() - started).total_seconds()
                            eta = f", ETA {_fmt_eta(el / frac - el)}" if frac > 0.002 else ""
                            status_cb(f"Merging file {idx}/{len(entries)} '{name}': {frac_file * 100:.0f}% | "
                                      f"total {frac * 100:.1f}% (~{(done_bytes + frac_file * size) / 2**30:.1f} of "
                                      f"{total_bytes / 2**30:.1f} GB), {out_rows + got:,} rows, "
                                      f"elapsed {_fmt_eta(el)}{eta}")
                con.execute('COMMIT')
            finally:
                if con.in_transaction:
                    try:
                        con.execute('ROLLBACK')
                    except sqlite3.Error:
                        pass
                try:
                    con.execute("DETACH DATABASE src")
                except sqlite3.Error:
                    pass
            out_rows += got
            done_bytes += size
            msg = None
            if verify:
                status_cb(f"Verifying rows of '{name}' in the merged file...")
                n, sm = con.execute(f"SELECT count(*), sum({rowsum}) FROM main.{T} WHERE rowid BETWEEN ? AND ?",
                                    (first_out, out_rows)).fetchone()
                if n != exp_n or got != exp_n:
                    msg = f"FAILED: source has {exp_n:,} rows, merged file has {n:,}"
                elif (sm or 0) != exp_sum:
                    msg = f"FAILED: row count matches ({n:,}) but cell contents differ"
                else:
                    msg = "OK"
            results.append((e['path'], e['table'], got, msg))
        con.close()
        os.replace(tmp_path, out_path)
        ok = True
    except sqlite3.OperationalError as ex:
        if 'interrupt' in str(ex).lower() and cancel is not None and cancel.is_set():
            raise JobCancelled()
        raise
    finally:
        if not ok:
            try:
                con.close()
            except Exception:
                pass
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    total = sum(r[2] for r in results)
    failed = [r for r in results if r[3] and r[3].startswith('FAILED')]
    lines = [f"Merged {len(results)} file(s) into one table: {total:,} rows.",
             f"Output: {out_path}\nTable: {out_table}" + (f" (+ column {src_col} = source file name)" if src_col else "")]
    if verify:
        lines.append("Integrity check: every row of every source file is in the merged file, values identical."
                     if not failed else f"Integrity check FAILED for {len(failed)} file(s).")
    shown = results[:15]
    lines.append("Rows per source file:\n" + "\n".join(
        f"  {os.path.basename(p)} [{t}]: {n:,}" + (f"  -> {m}" if m and m != 'OK' else "") for p, t, n, m in shown) +
        (f"\n  ... and {len(results) - 15} more file(s)" if len(results) > 15 else ""))
    if warnings:
        lines.append("Warnings:\n" + "\n".join("  - " + w for w in warnings[:8]))
    return "\n\n".join(lines), bool(failed)


# ===========================================================================
# 10. Report
# ===========================================================================

class Report:
    def __init__(self, job):
        self.job = job
        self.files = []          # (task_label, path or None, {sheet: rows}, verify_msg)
        self.notes = []
        self.warnings = []
        self.source_rows = {}
        self.coverage = {}       # sheet -> (total, covered)

    def warn(self, msg):
        self.warnings.append(msg)

    def warn_once(self, msg):
        if msg not in self.warnings:
            self.warnings.append(msg)

    def note(self, task_label, msg):
        self.notes.append(f"[{task_label}] {msg}")

    def add_file(self, task_label, path, sheet_counts, verify_msg):
        self.files.append((task_label, path, sheet_counts, verify_msg))

    @property
    def created(self):
        return [f for f in self.files if f[1]]

    @property
    def failed_checks(self):
        return [f for f in self.files if f[3] and f[3].startswith('FAILED')]

    def coverage_lines(self):
        lines = []
        for sheet, (total, covered) in self.coverage.items():
            exported_sum = sum(f[2].get(sheet, 0) for f in self.created)
            line = f"'{sheet}': {total:,} source rows, {covered:,} in at least one file"
            if total - covered:
                line += f", {total - covered:,} NOT in any file (did not match any filter value)"
            if exported_sum > covered:
                line += f", {exported_sum - covered:,} row copies appear in more than one file"
            lines.append(line)
        return lines

    def summary(self, max_files=15):
        parts = [f"Created {len(self.created)} file(s) from {len(self.files)} filter combination(s)."]
        if self.created:
            names = [f"  {os.path.basename(f[1])}  ({', '.join(f'{v:,} rows' for v in f[2].values())})"
                     for f in self.created[:max_files]]
            more = len(self.created) - max_files
            parts.append(f"Output folder: {self.job['out_dir']}\n" + "\n".join(names) +
                         (f"\n  ... and {more} more file(s)" if more > 0 else ""))
        if self.job['verify']:
            bad = self.failed_checks
            parts.append("Integrity check: all output files match the source data." if not bad else
                         f"Integrity check FAILED for {len(bad)} file(s):\n" +
                         "\n".join(f"  {os.path.basename(f[1])}: {f[3]}" for f in bad[:10]))
        cov = self.coverage_lines()
        if cov:
            parts.append("Coverage:\n" + "\n".join("  " + c for c in cov))
        if self.warnings:
            shown = self.warnings[:8]
            parts.append("Warnings:\n" + "\n".join("  - " + w for w in shown) +
                         ("\n  ..." if len(self.warnings) > len(shown) else ""))
        if self.notes and not self.created:
            shown = self.notes[:5]
            parts.append("Notes:\n" + "\n".join("  - " + n for n in shown) +
                         ("\n  ..." if len(self.notes) > len(shown) else ""))
        return "\n\n".join(parts)


# ===========================================================================
# 11. Qt UI
# ===========================================================================

class WorkerSignals(QObject):
    status = Signal(str)
    error = Signal(str, str)
    info = Signal(str, str)
    enable_button = Signal()
    schema_loaded = Signal(str, dict)
    batch_listed = Signal(list)
    batch_finished = Signal()


class DBSplitterApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Universal Large File & DB Splitter (PySide6)")
        self.resize(1100, 850)

        self.file_type = None  # 'db', 'excel', or 'csv'
        self.sheets_info = {}
        self.sheet_widgets = {}
        self.batch_mode = False
        self.batch_widgets = {}
        self.cancel_event = threading.Event()

        self.signals = WorkerSignals()
        self.signals.status.connect(self.update_status)
        self.signals.error.connect(self.show_error)
        self.signals.info.connect(self.show_info)
        self.signals.enable_button.connect(self.enable_button)
        self.signals.schema_loaded.connect(self._apply_schema_ui)
        self.signals.batch_listed.connect(self._apply_batch_ui)
        self.signals.batch_finished.connect(lambda: self.stop_btn.setVisible(False))

        self.setup_ui()

    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Top-level modes: Split (everything below) and Merge .db
        self.mode_tabs = QTabWidget()
        split_page = QWidget()
        split_layout = QVBoxLayout(split_page)
        split_layout.setContentsMargins(4, 4, 4, 4)
        self.mode_tabs.addTab(split_page, "Split")
        self.mode_tabs.addTab(self._build_merge_page(), "Merge .db")
        self.mode_tabs.currentChanged.connect(lambda _: self._refresh_process_btn())
        self.split_ready = False

        file_frame = QGroupBox("1. Source: one file (.xlsx .xlsm .xls .csv .db), a whole folder, or a .zip / .rar archive")
        file_layout = QGridLayout(file_frame)

        file_layout.addWidget(QLabel("Source:"), 0, 0)
        self.source_file_input = QLineEdit()
        self.source_file_input.setPlaceholderText("File, folder or .zip/.rar archive")
        file_layout.addWidget(self.source_file_input, 0, 1)
        src_btns = QHBoxLayout()
        src_btns.setContentsMargins(0, 0, 0, 0)
        for text, tip, slot in (("File", "Pick one file (with preview)", self.browse_source),
                                ("Folder", "Process every supported file in a folder (no preview)",
                                 self.browse_folder),
                                ("ZIP / RAR", "Process every supported file inside an archive (no preview)",
                                 self.browse_archive)):
            b = QPushButton(text)
            b.setToolTip(tip)
            b.clicked.connect(slot)
            src_btns.addWidget(b)
        file_layout.addLayout(src_btns, 0, 2)

        file_layout.addWidget(QLabel("Output Dir:"), 1, 0)
        self.output_dir_input = QLineEdit()
        file_layout.addWidget(self.output_dir_input, 1, 1)
        btn_browse_output = QPushButton("Browse")
        btn_browse_output.clicked.connect(self.browse_output)
        file_layout.addWidget(btn_browse_output, 1, 2)

        load_row = QHBoxLayout()
        load_row.addStretch(1)
        btn_load = QPushButton("Load Data Info")
        btn_load.clicked.connect(self.load_file_info)
        load_row.addWidget(btn_load)
        self.recursive_cb = QCheckBox("Include subfolders (folder source)")
        self.recursive_cb.setChecked(True)
        self.recursive_cb.toggled.connect(lambda _: self.batch_mode and self.load_file_info())
        load_row.addWidget(self.recursive_cb)
        load_row.addStretch(1)
        file_layout.addLayout(load_row, 2, 1)

        split_layout.addWidget(file_frame)

        self.filter_frame = QGroupBox("2. Sheet & Column Selection")
        filter_layout = QVBoxLayout(self.filter_frame)

        instructions = (
            "Filter Behavior:\n"
            "  - Comma (,): SPLIT into multiple files. | Semicolon (;): COMBINE into one file using OR.\n"
            "  - \"Quotes\": CONTAINS match (case-insensitive). | No Quotes: EXACT match (surrounding spaces ignored).\n"
            "  - A filter applies only to its own sheet. Tick 'Global' to apply it to every sheet with the same column.\n"
            "  - Output file name = MA_CSKCB value + sheet name (Excel). If the file has no MA_CSKCB column, "
            "the sheet name (Excel) / file or table name (CSV, DB) is used. Duplicate names get _2, _3...\n"
            "  - Excel: the title + table-header rows (even merged ones) are kept intact. Untick a column to blank "
            "its data below the header.\n"
            "  - Folder / ZIP / RAR source: no preview; every file is processed with auto-detected headers. Use the "
            "MA_CSKCB filter box, or leave it empty to split each file by every MA_CSKCB value."
        )
        # Guide is hidden by default; the small book button toggles it.
        guide_bar = QHBoxLayout()
        guide_bar.setContentsMargins(0, 0, 0, 0)
        self.guide_btn = QToolButton()
        self.guide_btn.setText("\U0001F4D6")
        self.guide_btn.setCheckable(True)
        self.guide_btn.setAutoRaise(True)
        self.guide_btn.setToolTip("Show / hide the filter guide")
        self.guide_btn.setStyleSheet("QToolButton { font-size: 16px; padding: 1px 4px; }")
        guide_bar.addWidget(self.guide_btn)
        guide_hint = QLabel("Filter guide")
        guide_hint.setStyleSheet("color: gray;")
        guide_bar.addWidget(guide_hint)
        guide_bar.addStretch(1)
        filter_layout.addLayout(guide_bar)

        self.inst_label = QLabel(instructions)
        self.inst_label.setStyleSheet("color: gray;")
        self.inst_label.setWordWrap(True)
        self.inst_label.setVisible(False)
        filter_layout.addWidget(self.inst_label)
        self.guide_btn.toggled.connect(self.inst_label.setVisible)

        self.notebook = QTabWidget()
        filter_layout.addWidget(self.notebook)
        split_layout.addWidget(self.filter_frame, stretch=1)
        main_layout.addWidget(self.mode_tabs, stretch=1)

        action_frame = QWidget()
        action_layout = QHBoxLayout(action_frame)
        self.status_label = QLabel("Ready.")
        action_layout.addWidget(self.status_label, stretch=1)

        self.verify_cb = QCheckBox("Verify output files after export")
        self.verify_cb.setChecked(True)
        self.verify_cb.setToolTip("Re-reads every output file and compares its rows cell-by-cell with the source.")
        action_layout.addWidget(self.verify_cb)

        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setToolTip("Stop processing (a .db file stops within seconds; partial output is removed)")
        self.stop_btn.setVisible(False)
        self.stop_btn.clicked.connect(self.stop_batch)
        action_layout.addWidget(self.stop_btn)

        self.process_btn = QPushButton("Start Processing")
        self.process_btn.setEnabled(False)
        self.process_btn.clicked.connect(self.process_data)
        action_layout.addWidget(self.process_btn)

        main_layout.addWidget(action_frame)

    @Slot(str)
    def update_status(self, message):
        self.status_label.setText(message)

    def _show_box(self, icon, title, message):
        """Message box always centered on the app window."""
        box = QMessageBox(icon, title, message, QMessageBox.Ok, self)

        def center():
            box.adjustSize()
            geo = box.frameGeometry()
            geo.moveCenter(self.frameGeometry().center())
            box.move(geo.topLeft())
        QTimer.singleShot(0, center)
        box.exec()

    @Slot(str, str)
    def show_error(self, title, message):
        self._show_box(QMessageBox.Critical, title, message)

    @Slot(str, str)
    def show_info(self, title, message):
        self._show_box(QMessageBox.Information, title, message)

    @Slot()
    def enable_button(self):
        self.busy = False
        self._refresh_process_btn()

    def _refresh_process_btn(self):
        if getattr(self, 'busy', False):
            self.process_btn.setEnabled(False)
            return
        if self.mode_tabs.currentIndex() == 1:
            self.process_btn.setText("Start Merge")
            ok = bool(self.merge_entries) and not any(e['error'] for e in self.merge_entries)
            self.process_btn.setEnabled(ok)
        else:
            self.process_btn.setText("Start Processing")
            self.process_btn.setEnabled(self.split_ready)

    # ------------------------------------------------------------------ merge page
    def _build_merge_page(self):
        self.merge_files = []
        self.merge_entries = []
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setContentsMargins(4, 4, 4, 4)

        intro = QLabel("Merge several .db files that have the same columns into ONE table of a new .db file. "
                       "Table names and column order may differ between files; columns are matched by name. "
                       "SQLite copies the rows itself, so every value is kept exactly.")
        intro.setWordWrap(True)
        intro.setStyleSheet("color: gray;")
        lay.addWidget(intro)

        box = QGroupBox("1. Files to merge (merged in this order)")
        bl = QVBoxLayout(box)
        btns = QHBoxLayout()
        for text, slot in (("Add files...", self._merge_add_files), ("Add folder...", self._merge_add_folder),
                           ("Remove selected", self._merge_remove), ("Move up", lambda: self._merge_move(-1)),
                           ("Move down", lambda: self._merge_move(1)), ("Clear", self._merge_clear)):
            b = QPushButton(text)
            b.clicked.connect(slot)
            btns.addWidget(b)
        btns.addStretch(1)
        bl.addLayout(btns)
        self.merge_table = QTableWidget(0, 4)
        self.merge_table.setHorizontalHeaderLabels(["File", "Table", "Size", "Status"])
        self.merge_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.merge_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.merge_table.verticalHeader().setVisible(False)
        hh = self.merge_table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.Stretch)
        for c in (1, 2, 3):
            hh.setSectionResizeMode(c, QHeaderView.ResizeToContents)
        bl.addWidget(self.merge_table)
        self.merge_info = QLabel("No file added yet.")
        bl.addWidget(self.merge_info)
        lay.addWidget(box, stretch=1)

        out = QGroupBox("2. Output")
        og = QGridLayout(out)
        og.addWidget(QLabel("Output file:"), 0, 0)
        self.merge_out_input = QLineEdit()
        self.merge_out_input.setPlaceholderText("e.g. D:/data/merged.db")
        og.addWidget(self.merge_out_input, 0, 1)
        b = QPushButton("Browse")
        b.clicked.connect(self._merge_browse_out)
        og.addWidget(b, 0, 2)
        og.addWidget(QLabel("Table name:"), 1, 0)
        self.merge_table_input = QLineEdit()
        og.addWidget(self.merge_table_input, 1, 1)
        self.merge_src_cb = QCheckBox("Add a column SOURCE_FILE with the name of the file each row came from")
        og.addWidget(self.merge_src_cb, 2, 1)
        lay.addWidget(out)
        return page

    def _merge_set_files(self, files):
        self.merge_files = files
        self.merge_entries, _ref, ref_table = plan_merge(files) if files else ([], None, None)
        t = self.merge_table
        t.setRowCount(len(self.merge_entries))
        red, green = QBrush(QColor(200, 60, 60)), QBrush(QColor(40, 140, 60))
        total = 0
        for i, e in enumerate(self.merge_entries):
            size = os.path.getsize(e['path']) if os.path.exists(e['path']) else 0
            total += size
            status = e['error'] or ("OK" + (f" ({e['note']})" if e['note'] else "") + (" - reference" if i == 0 else ""))
            items = [QTableWidgetItem(e['path']), QTableWidgetItem(e['table'] or ""),
                     QTableWidgetItem(self._fmt_size(size)), QTableWidgetItem(status)]
            items[3].setForeground(red if e['error'] else green)
            for c, it in enumerate(items):
                t.setItem(i, c, it)
        n_bad = sum(1 for e in self.merge_entries if e['error'])
        if not self.merge_entries:
            self.merge_info.setText("No file added yet.")
        else:
            self.merge_info.setText(f"{len(self.merge_entries)} file(s), {self._fmt_size(total) or '0 B'} in total"
                                    + (f" - {n_bad} file(s) cannot be merged (see Status); remove them to continue."
                                       if n_bad else " - all files have the same columns as the first one."))
        if files:
            if not self.merge_out_input.text().strip():
                self.merge_out_input.setText(os.path.join(os.path.dirname(files[0]), "merged.db").replace('\\', '/'))
            if ref_table and not self.merge_table_input.text().strip():
                self.merge_table_input.setText(ref_table)
        self._refresh_process_btn()

    def _merge_add_files(self):
        names, _ = QFileDialog.getOpenFileNames(self, "Add .db files", "",
                                                "SQLite databases (*.db *.sqlite *.sqlite3);;All Files (*.*)")
        if names:
            self._merge_set_files(self.merge_files + [n for n in names if n not in self.merge_files])

    def _merge_add_folder(self):
        d = QFileDialog.getExistingDirectory(self, "Add all .db files of a folder (subfolders included)")
        if not d:
            return
        found = []
        out_now = self.merge_out_input.text().strip()
        out_key = os.path.normcase(os.path.abspath(out_now)) if out_now else None
        for dirpath, dirnames, filenames in os.walk(d):
            dirnames[:] = sorted(x for x in dirnames if not x.startswith(_SKIP_DIR_PREFIXES))
            for f in sorted(filenames):
                full = os.path.join(dirpath, f)
                if out_key and os.path.normcase(os.path.abspath(full)) == out_key:
                    continue        # never feed the output file back in as an input
                if f.lower().endswith(DB_EXTS) and not f.startswith(('~', '.')):
                    found.append(full.replace('\\', '/'))
        if not found:
            self.show_info("Merge", "No .db / .sqlite file found in that folder.")
            return
        self._merge_set_files(self.merge_files + [f for f in found if f not in self.merge_files])

    def _merge_remove(self):
        rows = {i.row() for i in self.merge_table.selectedIndexes()}
        self._merge_set_files([f for i, f in enumerate(self.merge_files) if i not in rows])

    def _merge_move(self, step):
        rows = sorted({i.row() for i in self.merge_table.selectedIndexes()})
        if len(rows) != 1:
            return
        i, j = rows[0], rows[0] + step
        if 0 <= j < len(self.merge_files):
            files = list(self.merge_files)
            files[i], files[j] = files[j], files[i]
            self._merge_set_files(files)
            self.merge_table.selectRow(j)

    def _merge_clear(self):
        self.merge_table_input.clear()
        self.merge_out_input.clear()
        self._merge_set_files([])

    def _merge_browse_out(self):
        name, _ = QFileDialog.getSaveFileName(self, "Merged output file", self.merge_out_input.text(),
                                              "SQLite database (*.db)")
        if name:
            if not name.lower().endswith(DB_EXTS):
                name += '.db'
            self.merge_out_input.setText(name)
            self.merge_out_confirmed = name

    def _start_merge(self):
        self._merge_set_files(list(self.merge_files))      # re-check files right before starting
        if not self.merge_entries or any(e['error'] for e in self.merge_entries):
            return
        out = self.merge_out_input.text().strip()
        table = self.merge_table_input.text().strip()
        if not out or not table:
            self.show_error("Merge", "Please set the output file and the table name.")
            return
        if not out.lower().endswith(DB_EXTS):
            out += '.db'
            self.merge_out_input.setText(out)
        if os.path.exists(out) and getattr(self, 'merge_out_confirmed', None) != out:
            box = QMessageBox(QMessageBox.Question, "Merge", f"{out}\nalready exists. Replace it?",
                              QMessageBox.Yes | QMessageBox.No, self)
            QTimer.singleShot(0, lambda: (box.adjustSize(), box.move(
                self.frameGeometry().center() - box.rect().center())))
            if box.exec() != QMessageBox.Yes:
                return
        job = {'files': list(self.merge_files), 'out_path': out, 'out_table': table,
               'verify': self.verify_cb.isChecked(), 'add_source_col': self.merge_src_cb.isChecked(),
               'cancel': self.cancel_event}
        self.cancel_event.clear()
        self.busy = True
        self.process_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.stop_btn.setVisible(True)
        threading.Thread(target=self._run_merge_thread, args=(job, self.signals), daemon=True).start()

    @staticmethod
    def _run_merge_thread(job, signals):
        started = dt.datetime.now()
        try:
            summary, has_problems = run_db_merge(job, signals.status.emit)
            elapsed = (dt.datetime.now() - started).total_seconds()
            signals.status.emit(f"Merge done in {_fmt_eta(elapsed)}.")
            signals.info.emit("Merge finished with integrity problems" if has_problems else "Merge finished",
                              summary)
        except JobCancelled:
            signals.status.emit("Merge stopped by user. The unfinished output file was removed.")
        except Exception as e:
            signals.error.emit("Merge Error", str(e))
            signals.status.emit("Error during merge.")
        finally:
            signals.batch_finished.emit()
            signals.enable_button.emit()

    def browse_source(self):
        filename, _ = QFileDialog.getOpenFileName(
            self, "Select Source File", "",
            "All Supported (*.db *.sqlite *.sqlite3 *.xlsx *.xlsm *.xls *.csv);;"
            "Database Files (*.db *.sqlite *.sqlite3);;Excel Files (*.xlsx *.xlsm *.xls);;"
            "CSV Files (*.csv);;All Files (*.*)"
        )
        if filename:
            self.source_file_input.setText(filename)
            self.output_dir_input.setText(os.path.dirname(filename))

    def browse_folder(self):
        dirname = QFileDialog.getExistingDirectory(self, "Select Source Folder")
        if dirname:
            self.source_file_input.setText(dirname)
            self.output_dir_input.setText(os.path.join(dirname, "_split_output"))
            self.load_file_info()

    def browse_archive(self):
        filename, _ = QFileDialog.getOpenFileName(self, "Select Archive", "",
                                                  "Archives (*.zip *.rar);;All Files (*.*)")
        if filename:
            self.source_file_input.setText(filename)
            stem = os.path.splitext(os.path.basename(filename))[0]
            self.output_dir_input.setText(os.path.join(os.path.dirname(filename), stem + "_split"))
            self.load_file_info()

    def stop_batch(self):
        self.cancel_event.set()
        self.stop_btn.setEnabled(False)
        self.update_status("Stopping after the current file...")

    def browse_output(self):
        dirname = QFileDialog.getExistingDirectory(self, "Select Output Directory")
        if dirname:
            self.output_dir_input.setText(dirname)

    def load_file_info(self):
        file_path = self.source_file_input.text()
        if not file_path or not os.path.exists(file_path):
            self.show_error("Error", "Please select a valid source file, folder or archive.")
            return
        self.split_ready = False
        self.process_btn.setEnabled(False)
        self.notebook.clear()
        self.sheet_widgets.clear()
        self.sheets_info.clear()
        self.batch_widgets.clear()
        self.file_type = None
        self.batch_mode = is_batch_source(file_path)
        if self.batch_mode:
            self.update_status("Scanning files...")
            args = (file_path, self.recursive_cb.isChecked(), self.output_dir_input.text() or None, self.signals)
            threading.Thread(target=self._batch_list_worker, args=args, daemon=True).start()
            return
        self.update_status("Loading schema...")
        threading.Thread(target=self._schema_thread_worker, args=(file_path, self.signals), daemon=True).start()

    @staticmethod
    def _batch_list_worker(path, recursive, out_dir, signals):
        try:
            signals.batch_listed.emit(list_batch_entries(path, recursive, out_dir))
        except Exception as e:
            signals.error.emit("Load Error", f"Failed to scan the source:\n{e}")
            signals.status.emit("Error scanning source.")

    @staticmethod
    def _fmt_size(n):
        if not n:
            return ""
        for unit in ('B', 'KB', 'MB', 'GB'):
            if n < 1024 or unit == 'GB':
                return f"{n:,.0f} {unit}" if unit == 'B' else f"{n:,.1f} {unit}"
            n /= 1024

    @Slot(list)
    def _apply_batch_ui(self, rows):
        ok_rows = [r for r in rows if not r[3]]
        tab = QWidget()
        lay = QVBoxLayout(tab)

        kinds = {}
        for r in ok_rows:
            k = 'Excel' if r[1] in EXCEL_EXTS else ('CSV' if r[1] == '.csv' else 'DB')
            kinds[k] = kinds.get(k, 0) + 1
        found = ", ".join(f"{v} {k}" for k, v in kinds.items()) or "none"
        head = QLabel(f"Batch mode: {len(ok_rows)} file(s) found ({found}). Preview is skipped: every sheet/table "
                      f"is exported, header rows are auto-detected for each sheet, empty results are dropped.")
        head.setWordWrap(True)
        lay.addWidget(head)

        opts = QGroupBox("Batch options")
        og = QGridLayout(opts)
        og.addWidget(QLabel("MA_CSKCB filter (optional):"), 0, 0)
        ma_edit = QLineEdit()
        ma_edit.setPlaceholderText('e.g. 79001, 79002   (same syntax: , split  ; combine  "..." contains)')
        og.addWidget(ma_edit, 0, 1)
        auto_cb = QCheckBox("When the filter is empty: split each file by every MA_CSKCB value found")
        auto_cb.setChecked(True)
        og.addWidget(auto_cb, 1, 0, 1, 2)
        sep_cb = QCheckBox("Put the results of each source file in its own subfolder")
        sep_cb.setChecked(True)
        og.addWidget(sep_cb, 2, 0, 1, 2)
        ma_edit.textChanged.connect(lambda t: auto_cb.setEnabled(not t.strip()))
        lay.addWidget(opts)

        table = QTableWidget(len(rows), 3)
        table.setHorizontalHeaderLabels(["File", "Type", "Size / status"])
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        err_brush = QBrush(QColor(200, 60, 60))
        for i, (name, kind, size, err) in enumerate(rows):
            items = [QTableWidgetItem(name), QTableWidgetItem(kind),
                     QTableWidgetItem(err if err else self._fmt_size(size))]
            for c, it in enumerate(items):
                if err:
                    it.setForeground(err_brush)
                table.setItem(i, c, it)
        table.resizeColumnsToContents()
        table.horizontalHeader().setStretchLastSection(True)
        lay.addWidget(table, stretch=1)

        self.notebook.addTab(tab, "Batch")
        self.batch_widgets = {'ma': ma_edit, 'auto': auto_cb, 'sep': sep_cb}
        self.split_ready = bool(ok_rows)
        self._refresh_process_btn()
        bad = len(rows) - len(ok_rows)
        self.update_status(f"Batch: {len(ok_rows)} file(s) ready" + (f", {bad} problem(s) in the list" if bad else "")
                           + ".")

    def _schema_thread_worker(self, file_path, signals):
        ext = os.path.splitext(file_path)[1].lower()
        extracted_info = {}
        try:
            if ext in DB_EXTS:
                file_type = 'db'
                conn = sqlite3.connect(file_path)
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
                    tables = cursor.fetchall()
                    if not tables:
                        raise ValueError("No tables found in the database.")
                    table_name = tables[0][0]
                    cursor.execute(f"PRAGMA table_info({qident(table_name)})")
                    extracted_info[table_name] = [col[1] for col in cursor.fetchall()]
                finally:
                    conn.close()

            elif ext in EXCEL_EXTS:
                if not OPENPYXL_AVAILABLE:
                    raise ImportError("openpyxl is required for Excel files. Please 'pip install openpyxl'")
                file_type = 'excel'
                wb = load_head_workbook(file_path)
                try:
                    for ws in wb.worksheets:
                        extracted_info[ws.title] = scan_sheet_layout(ws)
                finally:
                    wb.close()

            elif ext == '.csv':
                file_type = 'csv'
                extracted_info[os.path.basename(file_path)] = read_csv_columns(file_path)

            else:
                signals.error.emit("Unsupported File", "The selected file type is not supported.")
                signals.status.emit("Ready.")
                return

            signals.schema_loaded.emit(file_type, extracted_info)
            if ext in EXCEL_EXTS and not CALAMINE_AVAILABLE:
                signals.info.emit("Tip", "Install 'python-calamine' (pip install python-calamine) to read "
                                         "large Excel files many times faster.")
        except Exception as e:
            signals.error.emit("Load Error", f"Failed to read file schema:\n{str(e)}")
            signals.status.emit("Error loading file.")

    @Slot(str, dict)
    def _apply_schema_ui(self, file_type, extracted_info):
        self.file_type = file_type
        self.sheets_info = extracted_info
        for sheet_name, info in extracted_info.items():
            if file_type == 'excel':
                self._create_tab_excel(sheet_name, info)
            else:
                self._create_tab(sheet_name, info, is_db=(file_type == 'db'))
        self.split_ready = True
        self._refresh_process_btn()
        self.update_status(f"Loaded {len(self.sheets_info)} data sheet(s)/table(s).")

    @staticmethod
    def _ma_text(ma_label):
        if ma_label:
            return f"File-name key: MA_CSKCB found in column '{ma_label}'."
        return "File-name key: no MA_CSKCB column found -> output files are named after the sheet/table/file."

    def _create_tab(self, sheet_name, columns, is_db=False):
        tab_widget = QWidget()
        tab_layout = QVBoxLayout(tab_widget)

        cb_export = QCheckBox(f"Export this {'Table' if is_db else 'Sheet'} by Default")
        cb_export.setChecked(True)
        tab_layout.addWidget(cb_export)

        cb_drop = QCheckBox(f"Drop {'Table' if is_db else 'Sheet'} if empty after filtering")
        cb_drop.setChecked(True)
        tab_layout.addWidget(cb_drop)

        ma_lbl = QLabel(self._ma_text(detect_ma_in_names(columns)))
        ma_lbl.setStyleSheet("color: #2a7a2a;")
        tab_layout.addWidget(ma_lbl)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll_layout = QGridLayout(scroll_content)

        header_font = scroll_content.font()
        header_font.setBold(True)
        for col, text in enumerate(["Include", "Column Name", "Filter Values"]):
            lbl = QLabel(text)
            lbl.setFont(header_font)
            scroll_layout.addWidget(lbl, 0, col)

        col_widgets = []
        for i, col_name in enumerate(columns):
            row = i + 1
            inc_var = QCheckBox()
            inc_var.setChecked(True)
            scroll_layout.addWidget(inc_var, row, 0)
            scroll_layout.addWidget(QLabel(str(col_name)), row, 1)
            filter_var = QLineEdit()
            filter_var.setMinimumWidth(250)
            scroll_layout.addWidget(filter_var, row, 2)
            global_var = QCheckBox()   # single sheet/table: global is meaningless
            col_widgets.append({'name': col_name, 'label': str(col_name), 'inc_var': inc_var,
                                'filter_var': filter_var, 'global_var': global_var})

        scroll_layout.setRowStretch(len(columns) + 1, 1)
        scroll.setWidget(scroll_content)
        tab_layout.addWidget(scroll)
        self.notebook.addTab(tab_widget, sheet_name)

        self.sheet_widgets[sheet_name] = {
            'export_var': cb_export, 'drop_empty_var': cb_drop,
            'columns': col_widgets, 'is_excel_complex': False,
        }

    def _create_tab_excel(self, sheet_name, info):
        tab_widget = QWidget()
        tab_layout = QVBoxLayout(tab_widget)

        cb_export = QCheckBox("Export this Sheet by Default")
        cb_export.setChecked(True)
        tab_layout.addWidget(cb_export)

        cb_drop = QCheckBox("Drop Sheet if empty after filtering")
        cb_drop.setChecked(True)
        tab_layout.addWidget(cb_drop)

        hdr_row_layout = QHBoxLayout()
        hdr_row_layout.addWidget(QLabel("Header rows to keep intact (title + table header):"))
        spin_header = QSpinBox()
        spin_header.setRange(0, 500)
        spin_header.setValue(info['header_rows'])
        hdr_row_layout.addWidget(spin_header)
        btn_refresh = QPushButton("Refresh Preview")
        hdr_row_layout.addWidget(btn_refresh)
        hdr_row_layout.addStretch(1)
        tab_layout.addLayout(hdr_row_layout)

        ma_lbl = QLabel()
        ma_lbl.setStyleSheet("color: #2a7a2a;")
        tab_layout.addWidget(ma_lbl)

        note = QLabel(
            "Preview below shows the detected header rows (grey, with merges applied) plus a few sample data rows. "
            "The header block is copied AS-IS into every exported file. Untick a column to blank its data in every "
            "output file while keeping the header layout unchanged."
        )
        note.setStyleSheet("color: gray;")
        note.setWordWrap(True)
        tab_layout.addWidget(note)

        preview_table = QTableWidget()
        preview_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        preview_table.setMaximumHeight(240)
        tab_layout.addWidget(preview_table)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        col_widgets = []

        def build_scroll_content(labels):
            content = QWidget()
            grid = QGridLayout(content)
            header_font = content.font()
            header_font.setBold(True)
            for col, text in enumerate(["Include", "Column (auto-detected label)", "Filter Values",
                                        "Global (Apply to All Sheets)"]):
                lbl = QLabel(text)
                lbl.setFont(header_font)
                grid.addWidget(lbl, 0, col)

            col_widgets.clear()
            for i, label in enumerate(labels):
                row = i + 1
                inc_var = QCheckBox()
                inc_var.setChecked(True)
                grid.addWidget(inc_var, row, 0)
                lbl = QLabel(label)
                lbl.setWordWrap(True)
                grid.addWidget(lbl, row, 1)
                filter_var = QLineEdit()
                filter_var.setMinimumWidth(200)
                grid.addWidget(filter_var, row, 2)
                global_var = QCheckBox()
                grid.addWidget(global_var, row, 3)
                col_widgets.append({'name': f"col_{i}", 'label': label, 'inc_var': inc_var,
                                    'filter_var': filter_var, 'global_var': global_var})
            grid.setRowStretch(len(labels) + 1, 1)
            return content

        def rebuild_column_list(labels):
            scroll.setWidget(build_scroll_content(labels))

        def update_ma_label(layout_info):
            idx = layout_info.get('ma_index')
            ma_lbl.setText(self._ma_text(layout_info['labels'][idx] if idx is not None else None))

        def render_preview(header_rows, grid, merges, sample_rows, max_col):
            preview_table.clearSpans()
            preview_table.setRowCount(header_rows + len(sample_rows))
            preview_table.setColumnCount(max_col)
            preview_table.setHorizontalHeaderLabels([f"Col {c + 1}" for c in range(max_col)])
            header_bg = QBrush(QColor(225, 225, 225))
            header_fg = QBrush(QColor(20, 20, 20))
            data_bg = QBrush(QColor(255, 255, 255))
            data_fg = QBrush(QColor(20, 20, 20))
            header_font_bold = preview_table.font()
            header_font_bold.setBold(True)
            for r in range(header_rows):
                for c in range(max_col):
                    val = grid[r][c] if r < len(grid) and c < len(grid[r]) else None
                    item = QTableWidgetItem("" if val is None else str(val))
                    item.setBackground(header_bg)
                    item.setForeground(header_fg)
                    item.setFont(header_font_bold)
                    preview_table.setItem(r, c, item)
            for (min_r, max_r, min_c, max_c) in merges:
                max_c = min(max_c, max_col)
                if max_r >= min_r and max_c >= min_c and (max_r > min_r or max_c > min_c):
                    preview_table.setSpan(min_r - 1, min_c - 1, max_r - min_r + 1, max_c - min_c + 1)
            for si, row_vals in enumerate(sample_rows):
                r = header_rows + si
                for c in range(max_col):
                    val = row_vals[c] if c < len(row_vals) else None
                    item = QTableWidgetItem("" if val is None else str(val))
                    item.setBackground(data_bg)
                    item.setForeground(data_fg)
                    preview_table.setItem(r, c, item)
            preview_table.resizeColumnsToContents()

        render_preview(info['header_rows'], info['grid'], info['merges'], info['sample_rows'], info['max_col'])
        rebuild_column_list(info['labels'])
        update_ma_label(info)

        def on_refresh():
            file_path = self.source_file_input.text()
            if not file_path or not os.path.exists(file_path):
                self.show_error("Error", "Please select a valid source file first.")
                return
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                need = max(HEAD_LOAD_ROWS, spin_header.value() + SAMPLE_DATA_ROWS)
                wb = load_head_workbook(file_path, {sheet_name: need}, default_rows=0)
                try:
                    new_info = scan_sheet_layout(wb[sheet_name], header_rows=spin_header.value())
                finally:
                    wb.close()
                self.sheets_info[sheet_name] = new_info
                render_preview(new_info['header_rows'], new_info['grid'], new_info['merges'],
                               new_info['sample_rows'], new_info['max_col'])
                rebuild_column_list(new_info['labels'])
                update_ma_label(new_info)
            except Exception as e:
                self.show_error("Preview Error", str(e))
            finally:
                QApplication.restoreOverrideCursor()

        btn_refresh.clicked.connect(on_refresh)
        tab_layout.addWidget(scroll, stretch=1)
        self.notebook.addTab(tab_widget, sheet_name)

        self.sheet_widgets[sheet_name] = {
            'export_var': cb_export, 'drop_empty_var': cb_drop,
            'columns': col_widgets, 'header_rows_var': spin_header,
            'is_excel_complex': True,
        }

    # ------------------------------------------------------------------
    def parse_filters(self):
        """Build the job description: exported sheets + list of output tasks."""
        sheets = []
        condition_sets = []
        label_of = {}   # (sheet, col) -> readable label

        for sheet_name, widgets in self.sheet_widgets.items():
            if not widgets['export_var'].isChecked():
                continue
            excluded_idx, excluded_names = set(), set()
            for i, cw in enumerate(widgets['columns']):
                label_of[(sheet_name, cw['name'])] = cw['label']
                if not cw['inc_var'].isChecked():
                    excluded_idx.add(i)
                    excluded_names.add(cw['name'])

                filter_text = cw['filter_var'].text().strip()
                if not filter_text:
                    continue
                op = 'EXACT'
                if len(filter_text) >= 2 and filter_text[0] == filter_text[-1] and filter_text[0] in ('"', "'"):
                    op = 'CLOSEST'
                    filter_text = filter_text[1:-1].strip()
                scope = None if cw['global_var'].isChecked() else sheet_name
                key = column_key(cw['label'])

                def mk(vals, _scope=scope, _col=cw['name'], _op=op, _key=key, _origin=sheet_name):
                    return Cond(_scope, _col, _op, tuple(vals), _key, _origin)
                if ',' in filter_text:
                    values = [v.strip() for v in filter_text.split(',') if v.strip()]
                    parsed = [mk((v,)) for v in dict.fromkeys(values)]
                elif ';' in filter_text:
                    values = [v.strip() for v in filter_text.split(';') if v.strip()]
                    parsed = [mk(dict.fromkeys(values))] if values else []
                else:
                    parsed = [mk((filter_text,))] if filter_text else []
                if parsed:
                    condition_sets.append(parsed)

            cfg = {'name': sheet_name, 'drop_empty': widgets['drop_empty_var'].isChecked(),
                   'excluded': excluded_idx, 'excluded_names': excluded_names}
            if widgets.get('is_excel_complex'):
                info = self.sheets_info.get(sheet_name, {})
                cfg.update({'header_rows': widgets['header_rows_var'].value(),
                            'max_col': info.get('max_col', 0),
                            'grid': info.get('grid', []), 'merges': info.get('merges', [])})
            sheets.append(cfg)

        def describe(cond):
            label = label_of.get((cond.origin, cond.col), cond.col)
            where = "[all sheets] " if cond.scope is None else \
                (f"[{cond.scope}] " if len(self.sheet_widgets) > 1 else "")
            sign = '=' if cond.op == 'EXACT' else 'contains'
            return f"{where}{label} {sign} {' OR '.join(cond.values)}"

        if not condition_sets:
            tasks = [("full export (no filter)", [])]
        else:
            tasks = []
            for combo in itertools.product(*condition_sets):
                tasks.append(("; ".join(describe(c) for c in combo), list(combo)))
        return sheets, tasks

    def process_data(self):
        if self.mode_tabs.currentIndex() == 1:
            self._start_merge()
            return
        self.busy = True
        try:
            self._process_split()
        finally:
            if not self.stop_btn.isVisible() and self.process_btn.isEnabled():
                self.busy = False

    def _process_split(self):
        source_path = self.source_file_input.text()
        out_dir = self.output_dir_input.text()
        if not source_path or not out_dir:
            self.show_error("Error", "Source and Output directories must be set.")
            return
        if self.batch_mode:
            if not self.batch_widgets:
                self.show_error("Error", "Please load the data info first.")
                return
            opts = {'source': source_path, 'out_dir': out_dir, 'recursive': self.recursive_cb.isChecked(),
                    'separate_dirs': self.batch_widgets['sep'].isChecked(),
                    'tasks': batch_ma_tasks(self.batch_widgets['ma'].text()),
                    'auto_split': self.batch_widgets['auto'].isChecked(),
                    'verify': self.verify_cb.isChecked()}
            self.cancel_event.clear()
            self.process_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
            self.stop_btn.setVisible(True)
            threading.Thread(target=self._run_batch_thread, args=(opts, self.signals, self.cancel_event),
                             daemon=True).start()
            return
        sheets, tasks = self.parse_filters()
        if not sheets:
            self.show_error("Error", "No sheet/table is selected for export.")
            return
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        job = {'source_path': source_path, 'out_dir': out_dir, 'sheets': sheets,
               'tasks': tasks, 'verify': self.verify_cb.isChecked()}
        runner = {'db': run_db_job, 'excel': run_excel_job, 'csv': run_csv_job}.get(self.file_type)
        if runner is None:
            self.show_error("Error", "Please load the file info first.")
            return
        self.process_btn.setEnabled(False)
        self.cancel_event.clear()
        job['cancel'] = self.cancel_event
        if self.file_type == 'db':
            self.stop_btn.setEnabled(True)
            self.stop_btn.setVisible(True)
        threading.Thread(target=self._run_job_thread, args=(runner, job, self.signals), daemon=True).start()

    @staticmethod
    def _run_batch_thread(opts, signals, cancel_event):
        started = dt.datetime.now()
        try:
            summary, has_problems = run_batch(opts, signals.status.emit, cancel_event)
            elapsed = (dt.datetime.now() - started).total_seconds()
            signals.status.emit(f"Batch done in {elapsed:,.1f}s.")
            signals.info.emit("Batch finished with problems" if has_problems else "Batch finished", summary)
        except Exception as e:
            signals.error.emit("Batch Error", f"An error occurred during batch processing:\n{e}")
            signals.status.emit("Error during batch processing.")
        finally:
            signals.batch_finished.emit()
            signals.enable_button.emit()

    @staticmethod
    def _run_job_thread(runner, job, signals):
        started = dt.datetime.now()
        try:
            report = runner(job, signals.status.emit)
            elapsed = (dt.datetime.now() - started).total_seconds()
            signals.status.emit(f"Done in {elapsed:,.1f}s. {len(report.created)} file(s) created.")
            title = "Success" if not report.failed_checks else "Finished with integrity problems"
            signals.info.emit(title, report.summary())
        except JobCancelled:
            signals.status.emit("Stopped by user. Partial output files were removed.")
        except Exception as e:
            signals.error.emit("Processing Error", f"An error occurred during processing:\n{e}")
            signals.status.emit("Error during processing.")
        finally:
            signals.batch_finished.emit()
            signals.enable_button.emit()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = DBSplitterApp()
    window.show()
    sys.exit(app.exec())
