"""Bản lưu danh sách nhân sự THEO CƠ SỞ lấy từ cổng tra cứu Sở Y tế — dùng chung cho mục E (đối chiếu lịch làm việc) và mục F (xác thực CCHN).

Mỗi bản lưu = 1 file JSON trong thư mục con `syt_staff` của thư mục `data` (thư mục chứa file .duckdb, xem medinet_lookup.set_cache_dir /
data.gdp_workdir); chưa mở nguồn thì dùng `data/syt_staff` cạnh chương trình. Tên file = GPHĐ + ngày giờ quét/nhập (vd 01367_HCM_GPH_20261103-001500.json) nên
mỗi bản có giá trị tại thời điểm của nó: quét mới / nhập Excel KHÔNG ghi đè bản nào; chỉ xoá khi người dùng bấm Xoá (không giới hạn).
- Chọn 1 bản lưu (đã quét hoặc nhập từ Excel) => chạy hoàn toàn offline (xem medinet_lookup.configure_from_rule).
- Không chọn bản nào => tự dùng bản quét trong vòng AUTO_DAYS ngày của GPHĐ đó; chưa có thì tự quét 1 bản mới (ensure_recent).
- Xuất ra Excel theo bố cục file mẫu của cổng (sheet "Thông tin chung" + sheet "Nhân sự") và nhập lại được (import_xlsx).
Không phụ thuộc Qt. medinet_lookup được import trễ (trong hàm) để tránh import vòng.
"""
import dataclasses
import json
import os
import re
import threading
from datetime import datetime
from pathlib import Path

SUBDIR = 'syt_staff'
FORMAT_VERSION = 2
AUTO_DAYS = 30

_BASE = None   # thư mục `data` (chứa .duckdb) — set_base_dir đặt khi mở nguồn


def set_base_dir(workdir):
    global _BASE
    try:
        _BASE = Path(workdir)
    except Exception:
        _BASE = None


def norm_gphd(g):
    return re.sub(r'\s+', '', str(g or '')).upper()


def _candidate_dirs():
    out = []
    if _BASE is not None:
        out.append(_BASE / SUBDIR)
    out.append(Path(__file__).resolve().parent / 'data' / SUBDIR)
    out.append(Path.home() / 'GeneralDataProcessor' / 'data' / SUBDIR)
    return out


def store_dir():
    """Thư mục GHI bản lưu mới: <data>/syt_staff (data = thư mục chứa .duckdb); chưa có nguồn thì cạnh chương trình; không ghi được thì thư mục người dùng."""
    for d in _candidate_dirs():
        try:
            d.mkdir(parents=True, exist_ok=True)
            probe = d / '.write_test'
            probe.write_text('x', encoding='utf-8')
            probe.unlink()
            return d
        except Exception:
            continue
    raise OSError('Không tạo/ghi được thư mục lưu danh sách nhân sự (data/syt_staff).')


def _read_dirs():
    """Các thư mục ĐỌC bản lưu: thư mục hiện hành + thư mục cũ cạnh chương trình (để vẫn thấy các bản đã quét trước đây)."""
    seen, out = set(), []
    for d in _candidate_dirs():
        try:
            k = str(d.resolve())
        except Exception:
            k = str(d)
        if k not in seen and d.is_dir():
            seen.add(k)
            out.append(d)
    return out


def _safe(s):
    return re.sub(r'[^0-9A-Za-z]+', '_', str(s or '')).strip('_') or 'GPHD'


_META_CACHE = {}   # path -> (mtime, meta)


def _read_meta(path):
    p = Path(path)
    try:
        mt = p.stat().st_mtime
    except OSError:
        return None
    hit = _META_CACHE.get(str(p))
    if hit and hit[0] == mt:
        return hit[1]
    try:
        d = json.loads(p.read_text(encoding='utf-8'))
        meta = dict(d.get('meta') or {})
        meta['path'] = str(p)
        meta['count'] = len(d.get('records') or [])
    except Exception:
        return None
    _META_CACHE[str(p)] = (mt, meta)
    return meta


def read_meta(path):
    return _read_meta(path)


def list_snapshots(gphd=None):
    """Các bản lưu (mới nhất trước). gphd=None: tất cả; có gphd: chỉ của cơ sở đó."""
    key = norm_gphd(gphd) if gphd else ''
    out = []
    for d in _read_dirs():
        try:
            files = sorted(d.glob('*.json'))
        except OSError:
            continue
        for f in files:
            m = _read_meta(f)
            if not m:
                continue
            if key and norm_gphd(m.get('gphd')) != key:
                continue
            out.append(m)
    out.sort(key=lambda m: m.get('fetched_at', ''), reverse=True)
    return out


def label(meta):
    t = str(meta.get('fetched_at', ''))
    try:
        t = datetime.fromisoformat(t).strftime('%d/%m/%Y %H:%M')
    except Exception:
        pass
    name = meta.get('facility_name') or ''
    src = ' — nhập Excel' if meta.get('origin') == 'import' else ''
    return f"{t} — {meta.get('count', 0):,} nhân sự" + (f' — {name}' if name else '') + src


def save_snapshot(gphd, facility_name, records, hours=None, origin='scan', extra=None):
    """records = {mã chuẩn hoá: PractitionerRecord}. hours = chuỗi "Thời gian hoạt động" của cơ sở ('' = cơ sở không ghi; None = chưa biết). Trả đường dẫn file đã ghi."""
    d = store_dir()
    now = datetime.now()
    meta = dict(gphd=str(gphd).strip(), facility_name=facility_name or '', fetched_at=now.isoformat(timespec='seconds'), version=FORMAT_VERSION, origin=origin)
    if hours is not None:
        meta['hours'] = str(hours)
    if extra:
        meta.update({k: v for k, v in extra.items() if k not in meta})
    recs = [dataclasses.asdict(r) for r in records.values()]
    path = d / f"{_safe(gphd)}_{now.strftime('%Y%m%d-%H%M%S')}.json"
    n = 1
    while path.exists():
        n += 1
        path = d / f"{_safe(gphd)}_{now.strftime('%Y%m%d-%H%M%S')}_{n}.json"
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(dict(meta=meta, records=recs), ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    os.replace(tmp, path)
    _META_CACHE.pop(str(path), None)
    return str(path)


def load_snapshot(path):
    """Trả (meta, {mã chuẩn hoá: PractitionerRecord}). Ném lỗi nếu file hỏng."""
    import medinet_lookup as ML
    d = json.loads(Path(path).read_text(encoding='utf-8'))
    meta = dict(d.get('meta') or {})
    pf = {f.name for f in dataclasses.fields(ML.PractitionerRecord)}
    wf = {f.name for f in dataclasses.fields(ML.Workplace)}
    out = {}
    for r in d.get('records') or []:
        wps = [ML.Workplace(**{k: v for k, v in w.items() if k in wf}) for w in (r.get('workplaces') or [])]
        rec = ML.PractitionerRecord(**{k: v for k, v in r.items() if k in pf and k != 'workplaces'}, workplaces=wps)
        code = ML._normalize_code(rec.cchn)
        if code:
            out[code] = rec
    return meta, out


def delete_snapshot(path):
    try:
        Path(path).unlink()
    finally:
        _META_CACHE.pop(str(path), None)


def _facility_name_of(recs):
    for r in recs.values():
        for w in r.workplaces:
            if w.facility_name:
                return w.facility_name
    return ''


def scan(gphd, log=None, cancelled=None):
    """Quét danh sách nhân sự của cơ sở từ cổng (bỏ qua bản lưu/bộ nhớ tạm) rồi lưu thành bản mới. Trả (đường dẫn, lỗi)."""
    import medinet_lookup as ML
    recs, err = ML.fetch_facility_staff(gphd, log=log, cancelled=cancelled, use_cache=False, ignore_snapshot=True)
    if err or not recs:
        return '', err or 'Cổng trả về danh sách nhân sự rỗng.'
    info = ML.facility_info(gphd) or {}
    name = info.get('name') or _facility_name_of(recs)
    try:
        return save_snapshot(gphd, name, recs, hours=info.get('hours', ''), extra={k: v for k, v in info.items() if k not in ('name', 'hours')}), ''
    except Exception as e:
        return '', f'Không ghi được bản lưu: {e}'


def latest_recent(gphd, max_days=AUTO_DAYS):
    """Bản lưu mới nhất của GPHĐ còn trong max_days ngày (đường dẫn) hoặc ''."""
    now = datetime.now()
    for m in list_snapshots(gphd):
        try:
            t = datetime.fromisoformat(str(m.get('fetched_at', '')))
        except Exception:
            continue
        if (now - t).total_seconds() <= max_days * 86400:
            return m['path']
    return ''


_AUTO_LOCK = threading.Lock()


def ensure_recent(gphd, max_days=AUTO_DAYS, log=None, cancelled=None):
    """Chế độ TỰ ĐỘNG (không chọn bản lưu): dùng bản của GPHĐ trong max_days ngày; chưa có thì quét bản mới và lưu lại.
    Trả (đường dẫn, lỗi, vừa_quét_mới). Quét lỗi => đường dẫn '' (bên gọi tự quay về lấy từ mạng)."""
    with _AUTO_LOCK:
        p = latest_recent(gphd, max_days)
        if p:
            return p, '', False
        if log:
            log(f'Chưa có bản lưu danh sách nhân sự của {gphd} trong {max_days} ngày — tự quét 1 bản mới từ cổng...')
        path, err = scan(gphd, log=log, cancelled=cancelled)
        return path, err, bool(path)


# ---------------------------------------------------------------- Excel (bố cục như file mẫu của cổng)
INFO_SHEET = 'Thông tin chung'
STAFF_SHEET = 'Nhân sự'
STAFF_HEADERS = ['STT', 'Họ tên', 'Vị trí chuyên môn', 'Quốc tịch', 'Số chứng chỉ', 'Phạm vi hành nghề', 'Ngày làm việc', 'Thời gian làm việc', 'Chứng chỉ Id',
                 'Ngày nghỉ việc', 'Chuyên khoa / Khoa']


def default_export_name(meta):
    """Tên file gợi ý: <tên cơ sở>_<GPHĐ>_<dd-mm-yyyy>.xlsx"""
    try:
        d = datetime.fromisoformat(str(meta.get('fetched_at', ''))).strftime('%d-%m-%Y')
    except Exception:
        d = datetime.now().strftime('%d-%m-%Y')
    base = _safe(meta.get('facility_name')) if meta.get('facility_name') else ''
    return '_'.join(x for x in (base, _safe(meta.get('gphd')), d) if x) + '.xlsx'


def export_xlsx(path, out_path):
    """Xuất 1 bản lưu ra Excel (bố cục file mẫu: 'Thông tin chung' + 'Nhân sự'; mỗi giai đoạn làm việc của 1 người = 1 dòng; thêm cột 'Ngày nghỉ việc' ở cuối)."""
    import openpyxl
    from openpyxl.styles import Font
    meta, recs = load_snapshot(path)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = INFO_SHEET
    try:
        fa = datetime.fromisoformat(str(meta.get('fetched_at', ''))).strftime('%d/%m/%Y %H:%M')
    except Exception:
        fa = str(meta.get('fetched_at', ''))
    rows = [('THÔNG TIN CHUNG', None), ('Chỉ tiêu', 'Giá trị'),
            ('Tên cơ sở', meta.get('facility_name', '')), ('Số giấy phép', meta.get('gphd', '')),
            ('Thời gian hoạt động', meta.get('hours') or ''),
            ('Địa chỉ', meta.get('address', '')), ('Tình trạng', meta.get('status', '')),
            ('Hình thức tổ chức', meta.get('form', '')), ('Loại hình', meta.get('kind', '')),
            ('Ngày quét/nhập bản lưu', fa), ('Nguồn dữ liệu', 'tracuu.medinet.org.vn' if meta.get('origin') != 'import' else 'nhập từ Excel')]
    for r in rows:
        ws.append(list(r))
    ws['A1'].font = Font(bold=True, size=13)
    for c in ws[2]:
        c.font = Font(bold=True)
    ws.column_dimensions['A'].width = 28
    ws.column_dimensions['B'].width = 90
    ws2 = wb.create_sheet(STAFF_SHEET)
    ws2.append(STAFF_HEADERS)
    for c in ws2[1]:
        c.font = Font(bold=True)
    i = 0
    for rec in recs.values():
        nat = getattr(rec, 'nationality', '')
        wps = rec.workplaces or []
        if not wps:
            i += 1
            ws2.append([i, rec.name, rec.position, nat, rec.cchn, rec.scope, '', '', None, '', ''])
        for w in wps:
            i += 1
            ws2.append([i, rec.name, w.position or rec.position, nat, rec.cchn, rec.scope, w.ngay_lam, w.thoi_gian, None, w.ngay_nghi, w.khoa])
    for col, wd in zip('ABCDEFGHIJK', (6, 28, 32, 12, 20, 50, 14, 60, 12, 14, 22)):
        ws2.column_dimensions[col].width = wd
    wb.save(out_path)
    return out_path


def _norm_head(s):
    import unicodedata
    s = unicodedata.normalize('NFD', str(s or '')).replace('đ', 'd').replace('Đ', 'D')
    s = ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_')


def _cell(v):
    if v is None:
        return ''
    if isinstance(v, datetime):
        return v.strftime('%d/%m/%Y')
    s = str(v).strip()
    return '' if s.lower() in ('none', 'nan') else s


def import_xlsx(xlsx_path, gphd_default='', log=None):
    """Nhập 1 file Excel (bố cục như file mẫu / file do export_xlsx xuất) thành BẢN LƯU mới. Trả (đường dẫn, lỗi).
    Sheet 'Thông tin chung' cần có dòng 'Số giấy phép' (không có thì dùng gphd_default); sheet 'Nhân sự' cần cột 'Số chứng chỉ'.
    Nhiều dòng cùng số chứng chỉ = nhiều giai đoạn làm việc của người đó."""
    import openpyxl
    import medinet_lookup as ML
    try:
        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    except Exception as e:
        return '', f'Không mở được file Excel: {e}'
    info = {}
    ws_info = next((wb[n] for n in wb.sheetnames if _norm_head(n) in ('thong_tin_chung', 'thong_tin')), None)
    if ws_info is not None:
        for r in ws_info.iter_rows(values_only=True):
            if r and len(r) >= 2 and r[0] is not None:
                info[_norm_head(r[0])] = _cell(r[1])
    ws_st = next((wb[n] for n in wb.sheetnames if _norm_head(n) in ('nhan_su', 'nhan_su_co_so', 'danh_sach_nhan_su')), None)
    if ws_st is None:
        ws_st = wb[wb.sheetnames[-1]]
    it = ws_st.iter_rows(values_only=True)
    head = None
    for row in it:
        hn = [_norm_head(x) for x in row]
        if 'so_chung_chi' in hn or 'so_cchn' in hn:
            head = hn
            break
    if head is None:
        return '', 'Không tìm thấy dòng tiêu đề có cột "Số chứng chỉ" trong sheet nhân sự.'

    def col(*names):
        for n in names:
            if n in head:
                return head.index(n)
        return None
    c_name, c_pos, c_nat = col('ho_ten', 'ho_va_ten'), col('vi_tri_chuyen_mon', 'vi_tri_hanh_nghe', 'vi_tri'), col('quoc_tich')
    c_code, c_scope = col('so_chung_chi', 'so_cchn'), col('pham_vi_hanh_nghe', 'pham_vi')
    c_from, c_to, c_sched = col('ngay_lam_viec', 'ngay_lam'), col('ngay_nghi_viec', 'ngay_nghi'), col('thoi_gian_lam_viec', 'lich_lam_viec')
    c_khoa = col('chuyen_khoa_khoa', 'chuyen_khoa', 'khoa')
    gphd = info.get('so_giay_phep') or str(gphd_default or '').strip()
    if not gphd:
        return '', 'File không có "Số giấy phép" ở sheet Thông tin chung và chưa nhập GPHĐ — không biết bản lưu thuộc cơ sở nào.'
    fname = info.get('ten_co_so', '')
    recs = {}
    for row in it:
        def g(i):
            return _cell(row[i]) if i is not None and i < len(row) else ''
        raw = g(c_code)
        code = ML._normalize_code(raw)
        if not code:
            continue
        wp = ML.Workplace(gphd=gphd, facility_name=fname, position=g(c_pos), khoa=g(c_khoa), ngay_lam=g(c_from), ngay_nghi=g(c_to), thoi_gian=g(c_sched))
        rec = recs.get(code)
        if rec is None:
            recs[code] = ML.PractitionerRecord(found=True, source='tracuu_cs', name=g(c_name), cchn=raw, position=g(c_pos), scope=g(c_scope),
                                               nationality=g(c_nat), workplaces=[wp])
        else:
            rec.workplaces.append(wp)
    if not recs:
        return '', 'Sheet nhân sự không có dòng nào có số chứng chỉ.'
    hours = info.get('thoi_gian_hoat_dong', '')
    extra = {k: v for k, v in (('address', info.get('dia_chi', '')), ('status', info.get('tinh_trang', '')),
                               ('form', info.get('hinh_thuc_to_chuc', '')), ('kind', info.get('loai_hinh', ''))) if v}
    try:
        return save_snapshot(gphd, fname, recs, hours=hours, origin='import', extra=extra), ''
    except Exception as e:
        return '', f'Không ghi được bản lưu: {e}'
