"""cchn_verify.py — Mục F "Xác thực CCHN" chạy ĐỘC LẬP (không giao diện, không PySide6).

Tách từ General Data Processor: giữ NGUYÊN chức năng và logic của mục F
  * tra cứu danh sách nhân sự theo cơ sở / từng mã CCHN (web 1 → web 2) — dùng lại nguyên medinet_lookup.py
  * đối chiếu từng (mã bác sĩ, thời điểm) trong .db với danh mục nhân viên + cổng Sở Y tế
      → 4 loại lỗi: Không đăng ký với BHYT / Sai giờ làm việc / Sai thứ làm việc / Không đăng ký với SYT
  * 3 kiểu xuất: summary (1 sheet KET_QUA) · multi_sheet (1 file, 5 sheet) · multi_file (5 file)
  * cache tra cứu ra đĩa (~/.general_data_processor_medinet_cache.json) dùng chung với app cũ

Khác app cũ: bỏ phần xem trước (Preview) và các bộ lọc mục A–E, G; thay bằng LOG (console + file) và dòng lệnh.
Cần để CÙNG THƯ MỤC: medinet_lookup.py (giữ nguyên, không sửa). KHÔNG cần medinet_scope.py / kho_default.py / file kho.

Ví dụ:
  python cchn_verify.py --db 79988.db --staff-file FileDM_NHANVIENYTE.xlsx --gphd "01366/HCM-GPHĐ"
  python cchn_verify.py --config cchn_config.json
Chạy `python cchn_verify.py -h` để xem toàn bộ tuỳ chọn. Ctrl+C = dừng (không ghi file dở dang).
"""
from __future__ import annotations
import argparse,copy,datetime,json,logging,logging.handlers,math,os,re,shutil,signal,sys,tempfile,threading,time,traceback,uuid
from pathlib import Path
import pandas as pd

try:
    sys.stdout.reconfigure(encoding='utf-8',errors='replace');sys.stderr.reconfigure(encoding='utf-8',errors='replace')
except Exception:pass

# ============================================================================================
# CẤU HÌNH MẶC ĐỊNH — sửa trực tiếp ở đây nếu muốn chạy bằng nút Run của IDE (không cần dòng lệnh).
# Thứ tự ưu tiên: mặc định < CONFIG này < file --config (.json) < tham số dòng lệnh.
# ============================================================================================
CONFIG={
    'db':'',                    # file nguồn: .db/.sqlite/.sqlite3/.duckdb/.parquet/.csv/.xlsx/.xls
    'table':'',                 # bảng/sheet; để trống = bảng đầu tiên (giống app cũ)
    'staff_file':'',            # file danh mục nhân viên (FileDM_NHANVIENYTE .xlsx/.xls/.csv)
    'staff_key_col':'',         # cột mã CCHN trong file danh mục; để trống = 'MACCHN' nếu có
    'gphd':'',                  # số giấy phép hoạt động của bệnh viện, vd "01366/HCM-GPHĐ"
    'out':'',                   # file kết quả .xlsx; để trống = <thư mục .db>/<MA_CSKCB>_<bảng>_xac_thuc_cchn.xlsx
    'mode':'summary',           # summary | multi_sheet | multi_file
    'pair1_on':True,'pair1_doctor':'MA_BAC_SI','pair1_time':'NGAY_YL',
    'pair2_on':None,'pair2_doctor':'MA_BSI_THUC_HIEN','pair2_time':'NGAY_TH_YL',   # None = tự bật nếu .db có đủ 2 cột
    'loai_kcb':[],              # lọc theo MA_LOAI_KCB, vd ['01','03']; để trống = lấy tất cả loại hình KCB
    'snapshot':'',              # bản lưu danh sách nhân sự cơ sở (đường dẫn .json hoặc tên file) để DÙNG LẠI, khỏi tải web; để trống = dùng bản quét ≤30 ngày của GPHĐ, chưa có thì tự quét bản mới
    'snapshot_dir':'',          # thư mục chứa các bản lưu; để trống = <thư mục chứa cchn_verify.py>/ban_luu_cchn
    'offline':False,            # True = CHỈ dùng bản lưu (--snapshot), KHÔNG kết nối mạng; không cần GPHĐ
    'web1_safe':False,          # True = tra web 1 bổ sung ở tốc độ an toàn (~10s/mã, chậm)
    'log':'',                   # file log; để trống = <file kết quả>_log.txt
    'ssd':False,                # True = dùng nhiều luồng DuckDB hơn (SSD/NVMe)
    'cache':False,              # True = tạo cache Parquet TẠM cho .db ≥ 50MB (nằm trong thư mục làm việc tạm, xoá khi xong; thường không cần)
    'overwrite':False,          # True = ghi đè file kết quả nếu đã có; False = đặt tên _001, _002...
}
LOAI_KCB={'01':'Khám bệnh','02':'Điều trị ngoại trú','03':'Điều trị nội trú','04':'Điều trị nội trú ban ngày',
    '05':'Điều trị ngoại trú các bệnh mạn tính dài ngày liên tục trong năm (theo TT 46/2016/TT-BYT), có khám bệnh và lĩnh thuốc',
    '06':'Điều trị lưu tại Trạm Y tế tuyến xã, Phòng khám đa khoa khu vực','07':'Nhận thuốc theo hẹn (không khám bệnh)',
    '08':'Điều trị ngoại trú các bệnh mạn tính dài ngày liên tục trong năm (theo TT 46/2016/TT-BYT), có khám bệnh, có thực hiện các dịch vụ kỹ thuật và/hoặc được sử dụng thuốc',
    '09':'Điều trị nội trú dưới 04 (bốn) giờ','10':'Các trường hợp khác'}
LOAI_KCB_COL='MA_LOAI_KCB'
EXPORT_MODES=('summary','multi_sheet','multi_file')
SUPPORTED={'.db','.sqlite','.sqlite3','.csv','.xlsx','.xls','.duckdb','.parquet'}
SQLITE_EXT={'.db','.sqlite','.sqlite3'}

# --------------------------------------------------------------------------------------------
# LOG
# --------------------------------------------------------------------------------------------
LOGGER=logging.getLogger('cchn')
_MEM=None
def setup_logging():
    """Console ngay từ đầu; file log gắn sau khi biết đường dẫn (các dòng sớm được giữ lại trong bộ nhớ rồi ghi vào file)."""
    global _MEM
    LOGGER.setLevel(logging.INFO);LOGGER.propagate=False
    fmt=logging.Formatter('%(asctime)s  %(message)s','%H:%M:%S')
    ch=logging.StreamHandler(sys.stdout);ch.setFormatter(fmt);LOGGER.addHandler(ch)
    _MEM=logging.handlers.MemoryHandler(capacity=5000,flushLevel=logging.CRITICAL+10,target=None);_MEM.setFormatter(fmt)
    LOGGER.addHandler(_MEM)
def attach_log_file(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    fh=logging.FileHandler(path,mode='a',encoding='utf-8');fh.setFormatter(logging.Formatter('%(asctime)s  %(message)s','%Y-%m-%d %H:%M:%S'))
    if _MEM is not None:
        _MEM.setTarget(fh);_MEM.flush();LOGGER.removeHandler(_MEM);_MEM.close()
    LOGGER.addHandler(fh)
    return path
def log(msg):
    LOGGER.info(str(msg))
def make_progress(label,step=10):
    """progress(v) 0-100 → ghi log mỗi `step`%."""
    last=[-step]
    def p(v):
        try:v=int(v)
        except Exception:return
        if v>=last[0]+step or v>=100:
            last[0]=v;log(f'{label}: {v}%')
    return p

# --------------------------------------------------------------------------------------------
# DỪNG (Ctrl+C)
# --------------------------------------------------------------------------------------------
_CANCEL=threading.Event()
_ACTIVE=[None]   # kết nối DuckDB đang chạy (để ngắt truy vấn dài)
def cancelled():return _CANCEL.is_set()
def _on_sigint(signum,frame):
    if _CANCEL.is_set():
        log('Nhấn Ctrl+C lần 2 — thoát ngay.');os._exit(130)
    _CANCEL.set();log('⏹ Đã nhận Ctrl+C — đang dừng (nhấn Ctrl+C lần nữa để thoát ngay)...')
    con=_ACTIVE[0]
    if con is not None:
        try:con.interrupt()
        except Exception:pass

# --------------------------------------------------------------------------------------------
# TIỆN ÍCH (sao từ data.py)
# --------------------------------------------------------------------------------------------
INVALID_FILENAME=re.compile('[<>:"/\\\\|?*\x00-\x1f]')
RESERVED_WINDOWS={'CON','PRN','AUX','NUL'}|{f'COM{i}' for i in range(1,10)}|{f'LPT{i}' for i in range(1,10)}
def qi(name):return '"'+str(name).replace('"','""')+'"'
def ql(value):return "'"+str(value).replace("'","''")+"'"
def safe_name(value,fallback='output',max_len=120):
    s='' if value is None else str(value)
    s=INVALID_FILENAME.sub('_',s)
    s=re.sub(r'\s+',' ',s).strip().rstrip('. ')
    if not s:s=fallback
    if s.upper() in RESERVED_WINDOWS:s='_'+s
    return s[:max_len].rstrip('. ') or fallback
def unique_path(path):
    path=Path(path)
    if not path.exists():return path
    for i in range(1,100000):
        p=path.with_name(f'{path.stem}_{i:03d}{path.suffix}')
        if not p.exists():return p
    raise RuntimeError('Không tạo được tên file duy nhất.')
def _default_memory_limit():
    override=os.environ.get('GDP_MEMORY_LIMIT')
    if override:return override
    try:
        import psutil
        total_gb=psutil.virtual_memory().total/(1024**3)
        return f'{max(2,min(16,round(total_gb*0.55)))}GB'
    except Exception:return '4GB'
def _profile_threads(ssd):
    override=os.environ.get('GDP_THREADS')
    if override:return int(override)
    cpu=os.cpu_count() or 2
    return max(2,min(16,cpu)) if ssd else max(2,min(4,cpu))

# --------------------------------------------------------------------------------------------
# NGUỒN DỮ LIỆU (.db và các định dạng khác) — tương đương data.Source, bản gọn
# --------------------------------------------------------------------------------------------
_WORKDIR=[None]   # thư mục làm việc tạm của lượt chạy hiện tại (DuckDB spill + cache Parquet + file tạm Excel)
_WRITTEN=[]       # các file kết quả đã mở ghi trong lượt chạy này (để xoá nếu ghi dở dang)
class DataSource:
    CACHE_MIN_BYTES=50*1024*1024
    def __init__(self,path,ssd=False):
        import duckdb
        self.path=Path(path);self.ext=self.path.suffix.lower()
        self._own_tmp=None
        if _WORKDIR[0]:self.workdir=Path(_WORKDIR[0])   # thư mục làm việc TẠM của lượt chạy (cạnh file .db, xoá khi xong — xem run())
        else:self._own_tmp=Path(tempfile.mkdtemp(prefix='cchn_'));self.workdir=self._own_tmp   # chỉ đọc cấu trúc file: thư mục tạm riêng, xoá ngay khi close()
        self.con=duckdb.connect(':memory:')
        self.con.execute(f"SET memory_limit='{_default_memory_limit()}'")
        self.con.execute(f'SET threads={_profile_threads(ssd)}')
        self.con.execute('SET preserve_insertion_order=false')
        self.temp_dir=self.workdir/'temp';self.temp_dir.mkdir(parents=True,exist_ok=True)
        self.con.execute(f'SET temp_directory={ql(str(self.temp_dir))}')
        self.table='source_data';self._cached_table=None;self._cache_path=None;self._sqlite_fallback=False
        _ACTIVE[0]=self.con
    def _ext(self,name):
        try:self.con.execute(f'LOAD {name}');return True
        except Exception:pass
        try:self.con.execute(f'INSTALL {name}');self.con.execute(f'LOAD {name}');return True
        except Exception:return False
    def prepare(self):
        import sqlite3
        if self.ext in SQLITE_EXT:
            with sqlite3.connect(f'file:{self.path}?mode=ro',uri=True) as c:
                tables=[r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()]
            if not tables:raise RuntimeError('Không tìm thấy bảng dữ liệu.')
            if not self._ext('sqlite'):
                # DuckDB SQLite extension chưa cài và máy không có internet → đọc bằng sqlite3 (chậm + tốn RAM hơn, kết quả như nhau: mọi cột là text).
                self._sqlite_fallback=True
                log('⚠ Không nạp được DuckDB SQLite extension — đọc .db bằng sqlite3 (chậm và tốn RAM hơn với file lớn).')
                return tables
            try:self.con.execute('SET GLOBAL sqlite_all_varchar=true')
            except Exception:self.con.execute('SET sqlite_all_varchar=true')
            self.con.execute(f'ATTACH {ql(str(self.path))} AS src (TYPE sqlite, READ_ONLY)')
            return tables
        if self.ext=='.duckdb':
            self.con.execute(f'ATTACH {ql(str(self.path))} AS src (TYPE duckdb, READ_ONLY)')
            tables=[r[0] for r in self.con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name='src' ORDER BY table_name").fetchall()]
            if not tables:raise RuntimeError('Không tìm thấy bảng dữ liệu.')
            return tables
        if self.ext=='.csv':
            self.con.execute(f"CREATE OR REPLACE VIEW source_data AS SELECT * FROM read_csv({ql(str(self.path))},header=true,auto_detect=true,all_varchar=true,sample_size=20480)")
            return ['source_data']
        if self.ext=='.parquet':
            cols=[r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM read_parquet({ql(str(self.path))})").fetchall()]
            if not cols:raise RuntimeError('File Parquet không có cột nào.')
            proj=','.join(f'CAST({qi(c)} AS VARCHAR) AS {qi(c)}' for c in cols)
            self.con.execute(f"CREATE OR REPLACE VIEW source_data AS SELECT {proj} FROM read_parquet({ql(str(self.path))})")
            return ['source_data']
        if self.ext in ('.xlsx','.xls'):
            engine='openpyxl' if self.ext=='.xlsx' else 'xlrd'
            x=pd.ExcelFile(self.path,engine=engine);sheets=x.sheet_names;x.close()
            if not sheets:raise RuntimeError('Không có worksheet.')
            return sheets
        raise RuntimeError(f'Không hỗ trợ định dạng: {self.ext}')
    def set_table(self,table):
        self.table=table
        if self._sqlite_fallback:
            import sqlite3
            with sqlite3.connect(f'file:{self.path}?mode=ro',uri=True) as c:
                cols=[r[1] for r in c.execute(f'PRAGMA table_info({qi(table)})').fetchall()]
                df=pd.read_sql_query(f'SELECT {",".join(f"CAST({qi(x)} AS TEXT) AS {qi(x)}" for x in cols)} FROM {qi(table)}',c)
            try:self.con.unregister('source_df')
            except Exception:pass
            self.con.register('source_df',df)
            self.con.execute('CREATE OR REPLACE TABLE source_data AS SELECT * FROM source_df')
            self.con.unregister('source_df')
            return
        if self.ext in ('.xlsx','.xls'):
            engine='openpyxl' if self.ext=='.xlsx' else 'xlrd'
            df=pd.read_excel(self.path,sheet_name=table,dtype=str,keep_default_na=False,na_filter=False,engine=engine)
            try:self.con.unregister('source_df')
            except Exception:pass
            self.con.register('source_df',df)
            self.con.execute('CREATE OR REPLACE VIEW source_data AS SELECT * FROM source_df')
    def _raw_relation(self):
        if self._sqlite_fallback:return 'source_data'
        return f'src.{qi(self.table)}' if (self.ext in SQLITE_EXT or self.ext=='.duckdb') else 'source_data'
    def relation(self):
        ident=safe_name(self.table)
        if self._cached_table==ident and self._cache_path and self._cache_path.exists():
            return f'read_parquet({ql(str(self._cache_path))})'
        return self._raw_relation()
    def columns(self):
        return [r[0] for r in self.con.execute(f'DESCRIBE SELECT * FROM {self.relation()}').fetchall()]
    def cache_eligible(self):
        if self.ext in ('.duckdb','.parquet'):return False
        try:return self.path.stat().st_size>=self.CACHE_MIN_BYTES
        except Exception:return False
    def _cache_file_path(self):
        st=self.path.stat();fp=f'{st.st_size}_{int(st.st_mtime)}'
        d=self.workdir/'cache';d.mkdir(parents=True,exist_ok=True)
        ident=safe_name(self.table)
        return d/f'{safe_name(self.path.stem)}_{fp}_{ident}.parquet',d,ident
    def ensure_cached(self):
        """Giống Source.ensure_cached: chép bảng ra Parquet (ZSTD) 1 lần cho .db lớn; lỗi thì bỏ qua, vẫn đọc trực tiếp .db."""
        if not self.cache_eligible():return False
        ident=safe_name(self.table)
        try:
            cache_path,cache_dir,ident=self._cache_file_path()
            if cache_path.exists():
                self._cached_table=ident;self._cache_path=cache_path;log(f'Dùng cache Parquet có sẵn: {cache_path.name}');return True
            try:
                if shutil.disk_usage(str(cache_dir)).free<self.path.stat().st_size*0.6:
                    log('Không đủ chỗ trống để tạo cache Parquet — đọc trực tiếp .db.');return False
            except Exception:pass
            if cancelled():return False
            log('Đang tạo cache Parquet cho file nguồn (chỉ chạy một lần cho mỗi file, có thể mất vài phút)...')
            t=time.time()
            tmp=cache_path.with_suffix(f'{cache_path.suffix}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp')
            self.con.execute(f"COPY (SELECT * FROM {self._raw_relation()}) TO {ql(str(tmp))} (FORMAT PARQUET, COMPRESSION ZSTD)")
            if cancelled():
                try:tmp.unlink()
                except Exception:pass
                return False
            if cache_path.exists():
                try:tmp.unlink()
                except Exception:pass
            else:tmp.replace(cache_path)
            self._cached_table=ident;self._cache_path=cache_path
            log(f'Đã tạo cache Parquet ({time.time()-t:.0f}s): {cache_path.name}')
            return True
        except Exception as e:
            log(f'⚠ Không tạo được cache Parquet ({e}) — đọc trực tiếp .db.');return False
    def close(self):
        try:self.con.close()
        except Exception:pass
        if self._own_tmp is not None:shutil.rmtree(self._own_tmp,ignore_errors=True);self._own_tmp=None
        if _ACTIVE[0] is self.con:_ACTIVE[0]=None

def open_source(path,table,ssd=False,cache=False):
    src=DataSource(path,ssd)
    try:
        tables=src.prepare()
        if table:
            if table not in tables:raise RuntimeError(f'Không tìm thấy bảng/worksheet "{table}". Có: {", ".join(tables)}')
        else:table=tables[0]
        src.set_table(table)
        if cache and src.cache_eligible():src.ensure_cached()
        return src,tables
    except Exception:
        src.close();raise

# --------------------------------------------------------------------------------------------
# DỰNG QUY TẮC (tương đương RuleEditor.validate_medinet_rule)
# --------------------------------------------------------------------------------------------
def norm_loai_kcb(v):
    """Chuẩn hoá danh sách MA_LOAI_KCB (chuỗi 'a,b' hoặc list) → list mã 2 chữ số hợp lệ, giữ thứ tự, bỏ trùng."""
    if isinstance(v,str):v=re.split(r'[,;\s]+',v)
    out=[]
    for x in (v or []):
        x=str(x).strip()
        if not x:continue
        x=x.zfill(2)
        if x not in LOAI_KCB:raise ValueError(f'Mã loại hình KCB "{x}" không hợp lệ. Hợp lệ: {", ".join(LOAI_KCB)}')
        if x not in out:out.append(x)
    return out
def build_rule(cfg,columns):
    import medinet_lookup as ML
    colset=set(columns)
    p1_on=bool(cfg['pair1_on'])
    p2_on=cfg['pair2_on']
    if p2_on is None:p2_on=(cfg['pair2_doctor'] in colset and cfg['pair2_time'] in colset)   # như UI: chỉ tự bật cặp 2 khi .db có đủ 2 cột
    p2_on=bool(p2_on)
    if not p1_on and not p2_on:raise ValueError('Xác thực CCHN cần bật ít nhất 1 trong 2 cặp cột.')
    p1d,p1t,p2d,p2t=cfg['pair1_doctor'],cfg['pair1_time'],cfg['pair2_doctor'],cfg['pair2_time']
    if p1_on and (not p1d or not p1t):raise ValueError('Cặp 1 cần chọn đủ Cột mã bác sĩ và Cột thời gian.')
    if p2_on and (not p2d or not p2t):raise ValueError('Cặp 2 cần chọn đủ Cột mã bác sĩ và Cột thời gian.')
    for on,d,t,lab in ((p1_on,p1d,p1t,'Cặp 1'),(p2_on,p2d,p2t,'Cặp 2')):
        if on:
            for c in (d,t):
                if c not in colset:raise ValueError(f'{lab}: cột "{c}" không có trong dữ liệu. Các cột hiện có: {", ".join(columns)}')
    lk=norm_loai_kcb(cfg.get('loai_kcb'))
    if lk and LOAI_KCB_COL not in colset:raise ValueError(f'Đã chọn lọc theo loại hình KCB nhưng dữ liệu không có cột {LOAI_KCB_COL}. Các cột hiện có: {", ".join(columns)}')
    sf=cfg['staff_file']
    if not sf:raise ValueError('Chưa chọn file danh mục nhân viên (--staff-file).')
    sf_path=Path(sf)
    if not sf_path.exists():raise ValueError(f'Không tìm thấy file danh mục nhân viên: {sf_path}')
    try:staff_cols=[str(c) for c in ML._read_staff_dir_df(sf_path).columns]
    except Exception as e:raise ValueError(f'Không đọc được file danh mục nhân viên: {e}')
    key=cfg['staff_key_col'] or ('MACCHN' if 'MACCHN' in staff_cols else '')
    if not key:raise ValueError(f'Chưa chọn Cột mã CCHN trong file danh mục nhân viên (--staff-key-col). Các cột: {", ".join(staff_cols)}')
    if key not in staff_cols:raise ValueError(f'File danh mục nhân viên không có cột "{key}". Các cột: {", ".join(staff_cols)}')
    gphd=str(cfg['gphd'] or '').strip()
    if not gphd and not str(cfg.get('snapshot') or '').strip():raise ValueError('Chưa nhập số giấy phép bệnh viện (GPHĐ) (--gphd) hoặc chọn bản lưu.')
    mode=cfg['mode'] if cfg['mode'] in EXPORT_MODES else 'summary'
    return ML.MedinetVerifyRule(enabled=True,pair1_enabled=p1_on,pair1_doctor_column=p1d,pair1_time_column=p1t,
        pair2_enabled=p2_on,pair2_doctor_column=p2d,pair2_time_column=p2t,staff_dir_file=str(sf_path),staff_dir_key_column=key,
        facility_gphd=gphd,export_mode=mode,web1_safe_pacing=bool(cfg['web1_safe']),scope_sheet=False)

# --------------------------------------------------------------------------------------------
# TRUY VẤN .db (tương đương data.medinet_lite_query / Engine.query không có bộ lọc A–E)
# --------------------------------------------------------------------------------------------
def _where_clause(cfg):
    lk=norm_loai_kcb(cfg.get('loai_kcb'))
    if not lk:return ''
    expr=f"LPAD(REGEXP_REPLACE(TRIM(CAST({qi(LOAI_KCB_COL)} AS VARCHAR)),'\\.0+$',''),2,'0')"
    return f" WHERE {expr} IN ({','.join(ql(x) for x in lk)})"
def lite_query(src,rule,cfg,limit=None):
    """CHỈ lấy các cột cần cho đối chiếu CCHN (+ THANH_TIEN/T_BHTT_DTL nếu có, + cột cho sheet phạm vi nếu bật)."""
    import medinet_lookup as ML
    cols=ML.medinet_lookup_columns(rule)
    if not cols:raise ValueError('Chưa bật cặp cột nào để đối chiếu.')
    available=set(src.columns())
    extras=['THANH_TIEN','T_BHTT_DTL']
    for e in extras:
        if e in available and e not in cols:cols.append(e)
    sql=f'SELECT {",".join(qi(c) for c in cols)} FROM {src.relation()}{_where_clause(cfg)}'
    if limit:sql+=f' LIMIT {int(limit)}'
    return sql
def full_query(src,cfg):
    return f'SELECT * FROM {src.relation()}{_where_clause(cfg)}'
def default_base_name(src,db_path):
    """Giống MainWindow._medinet_default_base_name: <MA_CSKCB>_<bảng>, không có thì tên file."""
    try:
        row=src.con.execute(f'SELECT MA_CSKCB FROM {src.relation()} WHERE MA_CSKCB IS NOT NULL LIMIT 1').fetchone()
        if row and str(row[0]).strip():return safe_name(f'{row[0]}_{safe_name(src.table)}')
    except Exception:pass
    return safe_name(Path(db_path).stem) or 'xac_thuc_cchn'

# --------------------------------------------------------------------------------------------
# GHI EXCEL (sao từ data.py: _write_excel_sheet và các hàm phụ)
# --------------------------------------------------------------------------------------------
NUMERIC_DECIMAL_COLUMNS=frozenset({
    'MUC_HUONG_XML1','T_TONGCHI','T_BNTT','T_BHTT','T_BNCCT','T_XN','T_CDHA','T_THUOC','T_MAU',
    'T_TTPT','T_VTYT','T_DVKT_TYLE','T_THUOC_TYLE','T_VTYT_TYLE','T_KHAM','T_GIUONG','T_VCHUYEN',
    'T_NGOAIDS','T_NGUONKHAC','SO_LUONG','SO_LUONG_BV','DON_GIA','DON_GIA_BV','THANH_TIEN',
    'TYLE_TT','T_NGUONKHAC_DTL','T_BNTT_DTL','T_BHTT_DTL','T_BNCCT_DTL','T_NGOAIDS_DTL',
    'MUC_HUONG_DTL','THANH_TIEN_BV','THANH_TIEN_BH','T_NGUONKHAC_NSNN','T_NGUONKHAC_VTNN',
    'T_NGUONKHAC_VTTN','T_NGUONKHAC_CL'})
_INVALID_SHEET_CHARS=re.compile(r'[:\\/?*\[\]]')
def _safe_sheet_name(name):return _INVALID_SHEET_CHARS.sub('_',str(name))[:31] or 'Sheet'
_HEADER_FILL_RGB='FF0072BC';_HEADER_FONT_RGB='FFFFFFFF'
def _style_header_row(ws,n_cols):
    from openpyxl.styles import Font,PatternFill,Alignment
    fill=PatternFill(start_color=_HEADER_FILL_RGB,end_color=_HEADER_FILL_RGB,fill_type='solid')
    font=Font(bold=True,color=_HEADER_FONT_RGB);align=Alignment(horizontal='center',vertical='center',wrap_text=True)
    for col in range(1,n_cols+1):
        c=ws.cell(row=1,column=col);c.fill=fill;c.font=font;c.alignment=align
    ws.freeze_panes='A2'
def _autosize_columns(ws,frame,sample_rows=200,min_width=10,max_width=50):
    from openpyxl.utils import get_column_letter
    sample=frame.head(sample_rows)
    for i,name in enumerate(frame.columns,start=1):
        hl=len(str(name))
        try:bl=int(sample[name].astype(str).map(len).max()) if len(sample) else 0
        except Exception:bl=0
        ws.column_dimensions[get_column_letter(i)].width=max(min_width,min(max_width,max(hl,bl)+2))
EXCEL_MAX_ROWS=1_000_000   # số dòng dữ liệu tối đa mỗi sheet (giới hạn thật của Excel là 1.048.575, chừa dư cho an toàn)
def _split_name(base,k):
    suf=f'_{k}';return _safe_sheet_name(base)[:31-len(suf)]+suf
def _write_excel_sheet(writer,sheet_name,frame,bold_mask=None,max_rows=None,red_mask=None):
    """Ghi 1 DataFrame ra 1+ sheet. Quá giới hạn dòng Excel → tách thành nhiều sheet "<tên>_1".."<tên>_n", số dòng CHIA ĐỀU. Trả {tên sheet: số dòng}."""
    from openpyxl.styles import Font,PatternFill
    max_rows=max_rows or EXCEL_MAX_ROWS
    bold_font=Font(bold=True)
    red_fill=PatternFill(start_color='FFFFC7CE',end_color='FFFFC7CE',fill_type='solid');red_font=Font(color='FF9C0006')
    out={};name=_safe_sheet_name(sheet_name)
    numeric_cols=[c for c in frame.columns if c in NUMERIC_DECIMAL_COLUMNS]
    if numeric_cols:
        frame=frame.copy()
        for c in numeric_cols:frame[c]=pd.to_numeric(frame[c],errors='coerce')
    if len(frame)<=max_rows:parts=[(name,frame,bold_mask,red_mask)]
    else:
        n_parts=math.ceil(len(frame)/max_rows);q,rem=divmod(len(frame),n_parts);size=q+(1 if rem else 0)   # chia ĐỀU: các sheet chênh nhau tối đa 1 dòng
        log(f'⚠ Sheet "{name}" có {len(frame):,} dòng, vượt giới hạn {max_rows:,} dòng/sheet — tách thành {n_parts} sheet "{_split_name(name,1)}".."{_split_name(name,n_parts)}" (mỗi sheet khoảng {size:,} dòng).')
        parts=[]
        for k in range(n_parts):
            a=k*q+min(k,rem);b=a+q+(1 if k<rem else 0)
            parts.append((_split_name(name,k+1),frame.iloc[a:b],
                          bold_mask[a:b] if bold_mask is not None else None,red_mask[a:b] if red_mask is not None else None))
    for pname,pdf,pmask,pred in parts:
        pdf.to_excel(writer,sheet_name=pname,index=False);out[pname]=len(pdf)
        ws=writer.sheets[pname];n_cols=pdf.shape[1]
        if n_cols:_style_header_row(ws,n_cols);_autosize_columns(ws,pdf)
        if pmask is not None and any(pmask):
            for i,b in enumerate(pmask):
                if b:
                    for col in range(1,n_cols+1):ws.cell(row=i+2,column=col).font=bold_font
        if pred is not None and any(pred):
            for i,r in enumerate(pred):
                if r:
                    for col in range(1,n_cols+1):
                        c=ws.cell(row=i+2,column=col);c.fill=red_fill;c.font=red_font
    return out

def write_results(rule,output_path,summary_df,detail_sheets):
    """Ghi kết quả theo rule.export_mode (summary / multi_sheet / multi_file) — y hệt data.export_medinet_verification."""
    export_mode=rule.export_mode if rule.export_mode in EXPORT_MODES else 'summary'
    output_path=Path(output_path);output_path.parent.mkdir(parents=True,exist_ok=True)
    bold_mask=summary_df['_BOLD'].tolist() if '_BOLD' in summary_df.columns else None
    summary_out=summary_df.drop(columns=['_BOLD']) if '_BOLD' in summary_df.columns else summary_df
    if summary_out.empty:
        summary_out=pd.DataFrame(columns=['(không có dòng nào)']);bold_mask=None
    counts={};files=[]
    if export_mode=='multi_file':
        stem=output_path.stem
        def part_path(sn):return output_path.with_name(f'{stem}_{_safe_sheet_name(sn)}{output_path.suffix or ".xlsx"}')
        kq=part_path('KET_QUA');_WRITTEN.append(kq)
        with pd.ExcelWriter(kq,engine='openpyxl') as w:counts.update(_write_excel_sheet(w,'KET_QUA',summary_out,bold_mask=bold_mask))
        files.append(kq);log(f'Đã ghi "{kq.name}" — {len(summary_df)} dòng.')
        for sn,ddf in detail_sheets.items():
            pp=part_path(sn);_WRITTEN.append(pp)
            with pd.ExcelWriter(pp,engine='openpyxl') as w:counts.update(_write_excel_sheet(w,sn,ddf))
            files.append(pp);log(f'Đã ghi "{pp.name}" — {len(ddf)} dòng.')
    else:
        _WRITTEN.append(output_path)
        with pd.ExcelWriter(output_path,engine='openpyxl') as w:
            counts.update(_write_excel_sheet(w,'KET_QUA',summary_out,bold_mask=bold_mask))
            for sn,ddf in detail_sheets.items():counts.update(_write_excel_sheet(w,sn,ddf))
        files.append(output_path)
    return counts,files

# --------------------------------------------------------------------------------------------
# CHẠY TOÀN BỘ (tương đương: Áp dụng bộ lọc → tra cứu → Xuất; bỏ Preview)
# --------------------------------------------------------------------------------------------
def _run_inner(cfg):
    import medinet_lookup as ML
    t0=time.time()
    db=Path(cfg['db'])
    if not str(cfg['db']).strip():raise ValueError('Chưa chọn file nguồn (--db).')
    if not db.exists():raise ValueError(f'Không tìm thấy file nguồn: {db}')
    if db.suffix.lower() not in SUPPORTED:raise ValueError(f'Không hỗ trợ định dạng {db.suffix}. Hỗ trợ: {", ".join(sorted(SUPPORTED))}')
    # bản lưu danh sách nhân sự (syt_store): có bản lưu thì KHÔNG bắt buộc nhập GPHĐ (lấy từ bản lưu); offline = không kết nối mạng
    import syt_store
    snapshot_dir(cfg)
    snap=resolve_snapshot(cfg);offline=bool(cfg.get('offline'))
    if offline and snap is None:raise ValueError('Chế độ offline cần chọn 1 bản lưu danh sách nhân sự (--snapshot).')
    if snap is not None and not str(cfg.get('gphd') or '').strip():
        g0=(syt_store.read_meta(snap) or {}).get('gphd','')
        if not g0:raise ValueError(f'Bản lưu "{snap.name}" không ghi số GPHĐ — hãy nhập GPHĐ.')
        cfg=dict(cfg,gphd=g0);log(f'GPHĐ lấy từ bản lưu: {g0}')
    if not offline and (ML.requests is None or ML.BeautifulSoup is None):
        raise RuntimeError('Thiếu thư viện tra cứu. Cài bằng: python -m pip install requests beautifulsoup4')
    if ML.pd is None:raise RuntimeError('Thiếu pandas.')

    # --- 1. mở nguồn lấy bảng/cột/tên mặc định, rồi ĐÓNG (không để DuckDB mở trong lúc tra mạng — như app cũ)
    log(f'Nguồn: {db} ({db.stat().st_size/1024/1024:,.1f} MB)')
    src,tables=open_source(db,cfg['table'],cfg['ssd'],cache=False)
    try:
        table=src.table;columns=src.columns()
        log(f'Bảng "{table}" ({len(tables)} bảng: {", ".join(tables[:10])}{"..." if len(tables)>10 else ""}) — {len(columns)} cột.')
        if len(tables)>1 and not cfg['table']:log('⚠ File có nhiều bảng và chưa chỉ định --table → dùng bảng đầu tiên.')
        base=default_base_name(src,db)
    finally:src.close()
    rule=build_rule(cfg,columns)
    # --- đường dẫn kết quả + file log
    out=Path(cfg['out']) if cfg['out'] else db.parent/f'{base}_xac_thuc_cchn.xlsx'
    if out.suffix.lower()!='.xlsx':out=out.with_suffix('.xlsx')
    if not cfg['overwrite']:out=unique_path(out)
    lp=attach_log_file(cfg['log'] or out.with_name(out.stem+'_log.txt'))
    log('='*70)
    log(f'BẮT ĐẦU xác thực CCHN · nguồn={db.name} · bảng={table}')
    log(f'File log: {lp}')
    log(f'File kết quả: {out}  (kiểu xuất: {rule.export_mode})')
    pairs=ML._build_pairs(rule)
    for label,_,d,t in pairs:log(f'{label}: mã = {d} · thời gian = {t}')
    _lk=norm_loai_kcb(cfg.get('loai_kcb'))
    log('Loại hình KCB (MA_LOAI_KCB): '+(', '.join(f'{x} ({LOAI_KCB[x].split(" (")[0]})' for x in _lk) if _lk else 'tất cả (không lọc)'))
    log(f'Danh mục nhân viên: {rule.staff_dir_file} (cột mã: {rule.staff_dir_key_column}) · GPHĐ: {rule.facility_gphd}')
    log(f'Tốc độ web 1: {"AN TOÀN (~10s/mã)" if rule.web1_safe_pacing else "nhanh (mặc định)"}')
    needs_detail=rule.export_mode!='summary'

    # --- 2. nguồn danh sách nhân sự cơ sở (logic của app chính, ML.configure_from_rule):
    #   CÓ chọn bản lưu -> chạy hoàn toàn OFFLINE trên bản đó; KHÔNG chọn -> dùng bản quét ≤30 ngày của GPHĐ, chưa có thì tự quét 1 bản mới và lưu.
    rule.facility_snapshot=str(snap) if snap is not None else ''
    rule.syt_offline=bool(offline)
    try:ML.set_cache_dir(snapshot_dir(cfg))
    except Exception:pass
    snap_used=ML.configure_from_rule(rule,log=log)
    if offline:log('⚡ CHẾ ĐỘ OFFLINE: chỉ dùng bản lưu, KHÔNG kết nối mạng (không tra lẻ từng mã).')
    if snap_used:
        _m=syt_store.read_meta(snap_used) or {}
        log(f'Dùng BẢN LƯU danh sách nhân sự: {Path(snap_used).name} · {_m.get("count","?")} người · quét lúc {_m.get("fetched_at","?")}'+(f' · giờ hoạt động cơ sở: {_m.get("hours") or "cơ sở không ghi"}' if "hours" in _m else ''))
    try:
        log('Đang tra cứu danh mục nhân viên trên cổng tra cứu...')
        t=time.time()
        staff_keys,records1=ML.prefetch_staff_records(rule.staff_dir_file,rule.staff_dir_key_column,progress=make_progress('Tra cứu danh mục'),
                                                       cancelled=cancelled,log=log,facility_gphd=rule.facility_gphd)
        log(f'Đã tra xong {len(records1)} mã trong danh mục nhân viên ({time.time()-t:.0f}s).')
        if cancelled():return None

        # --- 3. mở .db, lấy dữ liệu cần đối chiếu rồi ĐÓNG
        src,_=open_source(db,table,cfg['ssd'],cache=cfg['cache'])
        try:
            sql=full_query(src,cfg) if needs_detail else lite_query(src,rule,cfg)
            log('Đang đọc dữ liệu từ .db'+(' (đủ cột để dựng sheet chi tiết)...' if needs_detail else ' (chỉ các cột cần đối chiếu)...'))
            t=time.time()
            df=src.con.execute(sql).df()
        except Exception:
            if cancelled():return None
            raise
        finally:src.close()
        log(f'Đã lọc được {len(df):,} dòng ({time.time()-t:.0f}s) — bắt đầu đối chiếu với cổng tra cứu.')
        if cancelled():return None

        # --- 4. PHA 2: đối chiếu
        t=time.time()
        summary_df,detail_sheets,records1,records2=ML.combine_with_dataframe(df,rule,staff_keys,records1,records2=None,log=log,cancelled=cancelled,track_details=needs_detail)
        log(f'Đối chiếu xong ({time.time()-t:.0f}s).')
        if cancelled():return None
    finally:
        ML.set_offline(False);ML.set_facility_snapshot('')

    # --- 5. ghi file
    log('Đang ghi file Excel...')
    counts,files=write_results(rule,out,summary_df,detail_sheets)
    log(f'Hoàn tất — {len(summary_df)} dòng cần rà soát trên {len(counts)} sheet.')
    if len(summary_df):
        for lab,n in summary_df['Lý do'].value_counts().items():log(f'   • {lab}: {n} dòng')
        nb=int(summary_df['_BOLD'].sum()) if '_BOLD' in summary_df.columns else 0
        if nb:log(f'   • Dòng tô đậm (mã vướng cả BHYT lẫn SYT): {nb}')
    for f in files:log(f'File: {f}')
    log(f'XONG · tổng thời gian {time.time()-t0:.0f}s')
    return dict(files=[str(f) for f in files],rows=len(summary_df),sheets=counts,log=str(lp),snapshot=str(snap_used or ''))


# --------------------------------------------------------------------------------------------
# BẢN LƯU DANH SÁCH NHÂN SỰ — dùng syt_store.py (cùng định dạng với app chính: JSON, nhập/xuất Excel theo mẫu cổng)
# Tải 1 lần, dùng lại nhiều lần, chạy offline được → hạn chế truy cập mạng (dễ bị chặn)
# --------------------------------------------------------------------------------------------
def snapshot_dir(cfg=None):
    """Thư mục gốc chứa các bản lưu (syt_store lưu vào <thư mục này>/syt_staff): cfg['snapshot_dir'] hoặc <thư mục script>/ban_luu_cchn (không ghi được thì ~/ban_luu_cchn)."""
    import syt_store
    cands=[]
    if cfg and str(cfg.get('snapshot_dir') or '').strip():cands.append(Path(cfg['snapshot_dir']))
    if getattr(sys,'frozen',False):cands.append(Path.home()/'Documents'/'CCHN'/'ban_luu_cchn')   # bản đóng gói (.app/.exe): không ghi vào bên trong ứng dụng
    cands+=[Path(__file__).resolve().parent/'ban_luu_cchn',Path.home()/'ban_luu_cchn']
    for d in cands:
        try:
            d.mkdir(parents=True,exist_ok=True);(d/'.w').write_text('x');(d/'.w').unlink()
            syt_store.set_base_dir(d);return d
        except Exception:continue
    raise RuntimeError('Không tạo được thư mục chứa bản lưu.')
def list_snapshots(directory=None):
    """Bản lưu, mới nhất trước: [{path,name,gphd,facility,saved_at,n,note,hours}]."""
    import syt_store
    if directory:syt_store.set_base_dir(directory)
    out=[]
    for m in syt_store.list_snapshots():
        out.append(dict(path=m['path'],name=Path(m['path']).name,gphd=m.get('gphd',''),facility=m.get('facility_name',''),
                        saved_at=str(m.get('fetched_at','')),n=m.get('count',0),note='nhập Excel' if m.get('origin')=='import' else 'quét từ cổng',hours=m.get('hours','')))
    return out
def delete_snapshot(path):
    import syt_store;syt_store.delete_snapshot(path)
def resolve_snapshot(cfg):
    """cfg['snapshot'] (đường dẫn hoặc tên file trong thư mục bản lưu) → Path hoặc None."""
    import syt_store
    v=str(cfg.get('snapshot') or '').strip()
    if not v:return None
    snapshot_dir(cfg)
    p=Path(v)
    if not p.exists():
        for m in syt_store.list_snapshots():
            if Path(m['path']).name==v or Path(m['path']).stem==v:p=Path(m['path']);break
    if not p.exists():raise ValueError(f'Không tìm thấy bản lưu danh sách nhân sự: {v}')
    return p
def import_snapshot(xlsx,gphd='',directory=None):
    """Thêm bản lưu từ file Excel theo bố cục mẫu của cổng (sheet "Thông tin chung" + "Nhân sự"). Trả đường dẫn bản lưu."""
    import syt_store
    if directory:syt_store.set_base_dir(directory)
    p,err=syt_store.import_xlsx(xlsx,gphd,log=log)
    if err:raise ValueError(err)
    log(f'Đã nhập bản lưu: {p}');return p
def export_snapshot(path,out_xlsx):
    import syt_store;return syt_store.export_xlsx(path,out_xlsx)
def download_snapshot(gphd,directory=None,cancelled=None):
    """Quét danh sách nhân sự cơ sở từ cổng và lưu thành bản lưu mới (giữ cả "Thời gian hoạt động" của cơ sở). Trả đường dẫn."""
    import syt_store
    if not str(gphd or '').strip():raise ValueError('Chưa nhập số giấy phép hoạt động (GPHĐ) của cơ sở.')
    if directory:syt_store.set_base_dir(directory)
    p,err=syt_store.scan(gphd,log=log,cancelled=cancelled or cancelled_flag)
    if err or not p:raise RuntimeError(err or 'Không lấy được danh sách nhân sự.')
    log(f'Đã lưu bản lưu: {p}');return p
cancelled_flag=cancelled

# --------------------------------------------------------------------------------------------
# THƯ MỤC LÀM VIỆC TẠM — tạo ngay cạnh file .db (ổ thường còn nhiều chỗ), xoá khi xong
# --------------------------------------------------------------------------------------------
WORK_PREFIX='_cchn_tmp_'
def _gb(n):return f'{n/1024**3:,.1f} GB'
def _cleanup_stale(parent,max_age_h=6):
    """Xoá thư mục tạm còn sót lại từ lượt chạy trước bị tắt đột ngột (chỉ thư mục tên _cchn_tmp_* cũ hơn max_age_h giờ)."""
    try:
        for d in parent.iterdir():
            if d.is_dir() and d.name.startswith(WORK_PREFIX) and time.time()-d.stat().st_mtime>max_age_h*3600:
                shutil.rmtree(d,ignore_errors=True);log(f'Đã dọn thư mục tạm cũ còn sót: {d.name}')
    except Exception:pass
def make_workdir(db,out_dir):
    for parent in (db.parent,out_dir):
        try:
            _cleanup_stale(parent)
            d=parent/f'{WORK_PREFIX}{uuid.uuid4().hex[:8]}';d.mkdir(parents=True,exist_ok=False);return d
        except Exception:continue
    return None
def _is_disk_full(e):
    seen=set()
    while e is not None and id(e) not in seen:   # đi theo chuỗi lỗi (lỗi dọn dẹp của pandas/openpyxl có thể che lỗi gốc)
        seen.add(id(e))
        if isinstance(e,OSError) and (getattr(e,'errno',None)==28 or getattr(e,'winerror',None) in (112,39)):return True
        e=e.__cause__ or e.__context__
    return False
def run(cfg):
    """Bọc _run_inner: tạo thư mục tạm cạnh .db → mọi file tạm (DuckDB spill, cache Parquet, file tạm của Excel) nằm trong đó
    → xoá sạch khi xong/lỗi/huỷ; ghi file kết quả dở dang thì xoá; hết dung lượng thì báo rõ ràng."""
    db=Path(str(cfg.get('db') or ''))
    out_dir=Path(cfg['out']).parent if cfg.get('out') else db.parent
    work=None;_WRITTEN.clear()
    old_env={k:os.environ.get(k) for k in ('TMP','TEMP','TMPDIR')};old_tmp=tempfile.tempdir
    try:
        if str(cfg.get('db') or '').strip() and db.exists():
            work=make_workdir(db,out_dir)
            if work is not None:
                _WORKDIR[0]=str(work);tempfile.tempdir=str(work)
                for k in old_env:os.environ[k]=str(work)
                try:
                    f_w=shutil.disk_usage(str(work)).free;f_o=shutil.disk_usage(str(out_dir if out_dir.exists() else db.parent)).free
                    log(f'Thư mục làm việc tạm: {work} (xoá khi xong) · trống {_gb(f_w)} · nơi lưu kết quả trống {_gb(f_o)}')
                    if min(f_w,f_o)<200*1024*1024:raise ValueError(f'Ổ đĩa còn quá ít chỗ trống ({_gb(min(f_w,f_o))}) — hãy giải phóng dung lượng hoặc chọn nơi lưu khác rồi chạy lại.')
                except ValueError:raise
                except Exception:pass
            else:log('⚠ Không tạo được thư mục làm việc tạm cạnh .db — dùng thư mục tạm mặc định của hệ thống.')
        return _run_inner(cfg)
    except BaseException as e:
        for f in _WRITTEN:   # file kết quả ghi dở dang
            try:Path(f).unlink()
            except Exception:pass
        if _is_disk_full(e):
            raise RuntimeError('HẾT DUNG LƯỢNG Ổ ĐĨA khi ghi dữ liệu. Hãy giải phóng ổ chứa file .db/nơi lưu kết quả, hoặc chọn nơi lưu ở ổ còn nhiều chỗ, hoặc dùng kiểu xuất "1 sheet" rồi chạy lại. (File kết quả dở dang đã được xoá.)') from e
        raise
    finally:
        _WORKDIR[0]=None;tempfile.tempdir=old_tmp
        for k,v in old_env.items():
            if v is None:os.environ.pop(k,None)
            else:os.environ[k]=v
        if work is not None:
            for _ in range(5):
                shutil.rmtree(work,ignore_errors=True)
                if not work.exists():break
                time.sleep(0.5)
            log(f'Đã xoá thư mục làm việc tạm.' if not work.exists() else f'⚠ Chưa xoá hết thư mục tạm, bạn có thể xoá tay: {work}')

# --------------------------------------------------------------------------------------------
# DÒNG LỆNH
# --------------------------------------------------------------------------------------------
def parse_args(argv=None):
    ap=argparse.ArgumentParser(description='Mục F — Xác thực CCHN (độc lập, không giao diện).',formatter_class=argparse.RawDescriptionHelpFormatter,epilog=__doc__)
    a=ap.add_argument
    a('--config',help='file .json cấu hình (cùng tên khoá như CONFIG ở đầu file)')
    a('--db',help='file nguồn (.db/.sqlite/.duckdb/.parquet/.csv/.xlsx)');a('--table',help='tên bảng/sheet (mặc định: bảng đầu tiên)')
    a('--staff-file',dest='staff_file',help='file danh mục nhân viên (.xlsx/.xls/.csv)');a('--staff-key-col',dest='staff_key_col',help='cột mã CCHN trong file danh mục (mặc định MACCHN)')
    a('--gphd',help='số giấy phép hoạt động của bệnh viện, vd "01366/HCM-GPHĐ"')
    a('--out',help='file kết quả .xlsx');a('--mode',choices=EXPORT_MODES,help='kiểu xuất (mặc định summary)')
    a('--pair1',nargs=2,metavar=('COT_MA','COT_TG'),help='cặp 1: cột mã bác sĩ + cột thời gian (mặc định MA_BAC_SI NGAY_YL)')
    a('--pair2',nargs=2,metavar=('COT_MA','COT_TG'),help='cặp 2 (mặc định MA_BSI_THUC_HIEN NGAY_TH_YL, tự bật nếu .db có đủ 2 cột)')
    a('--no-pair1',action='store_true',help='tắt cặp 1');a('--no-pair2',action='store_true',help='tắt cặp 2')
    a('--loai-kcb',dest='loai_kcb',help='chỉ xét các loại hình KCB (MA_LOAI_KCB), vd "01,03"; mặc định tất cả')
    a('--snapshot',help='dùng lại bản lưu danh sách nhân sự (đường dẫn .json hoặc tên file trong thư mục bản lưu) — không tải lại từ web')
    a('--snapshot-dir',dest='snapshot_dir',help='thư mục chứa các bản lưu (mặc định <thư mục script>/ban_luu_cchn)')
    a('--offline',action='store_true',help='CHỈ dùng bản lưu (--snapshot), KHÔNG kết nối mạng, không cần --gphd')
    a('--list-snapshots',action='store_true',help='liệt kê các bản lưu rồi thoát')
    a('--download-snapshot',action='store_true',help='chỉ tải danh sách nhân sự của cơ sở (--gphd) từ web và lưu thành bản lưu mới, rồi thoát')
    a('--delete-snapshot',metavar='TEN',help='xoá 1 bản lưu rồi thoát')
    a('--web1-safe',dest='web1_safe',action='store_true',help='giới hạn tốc độ web 1 (bổ sung) ở mức an toàn ~10s/mã (chậm)')
    a('--log',help='file log (mặc định <file kết quả>_log.txt)');a('--ssd',action='store_true',help='dùng nhiều luồng DuckDB hơn (SSD/NVMe)')
    a('--no-cache',action='store_true',help='không tạo cache Parquet cho .db lớn');a('--overwrite',action='store_true',help='ghi đè file kết quả nếu đã có')
    return ap.parse_args(argv)
def build_cfg(args):
    cfg=dict(CONFIG)
    if args.config:
        p=Path(args.config)
        if not p.exists():raise ValueError(f'Không tìm thấy file cấu hình: {p}')
        try:j=json.loads(p.read_text(encoding='utf-8-sig'))
        except Exception as e:raise ValueError(f'File cấu hình không đọc được: {e}')
        bad=[k for k in j if k not in CONFIG]
        if bad:raise ValueError(f'Khoá không hợp lệ trong file cấu hình: {", ".join(bad)}. Hợp lệ: {", ".join(CONFIG)}')
        cfg.update(j)
    for k in ('db','table','staff_file','staff_key_col','gphd','out','mode','log','snapshot','snapshot_dir'):
        v=getattr(args,k,None)
        if v:cfg[k]=v
    if args.pair1:cfg['pair1_doctor'],cfg['pair1_time']=args.pair1
    if args.pair2:cfg['pair2_doctor'],cfg['pair2_time']=args.pair2;cfg['pair2_on']=True
    if args.no_pair1:cfg['pair1_on']=False
    if args.no_pair2:cfg['pair2_on']=False
    if args.loai_kcb:cfg['loai_kcb']=args.loai_kcb
    if args.offline:cfg['offline']=True
    if args.web1_safe:cfg['web1_safe']=True
    if args.ssd:cfg['ssd']=True
    if args.no_cache:cfg['cache']=False
    if args.overwrite:cfg['overwrite']=True
    cfg['loai_kcb']=norm_loai_kcb(cfg.get('loai_kcb'))
    if cfg['mode'] not in EXPORT_MODES:raise ValueError(f'--mode phải là một trong: {", ".join(EXPORT_MODES)}')
    return cfg
def _ask(label,cur):
    if cur:return cur
    try:v=input(f'{label}: ').strip().strip('"').strip("'")
    except EOFError:v=''
    return v
def _pause():
    try:input('\nNhấn Enter để đóng...')
    except Exception:pass
def main(argv=None):
    # Chạy không tham số (bấm đúp file / chạy từ IDE): hỏi các mục bắt buộc còn thiếu và giữ cửa sổ mở
    interactive=argv is None and len(sys.argv)==1
    setup_logging()
    try:signal.signal(signal.SIGINT,_on_sigint)
    except Exception:pass
    try:
        if interactive:
            if not CONFIG['db']:print('Kéo-thả file hoặc dán đường dẫn rồi nhấn Enter.')
            CONFIG['db']=_ask('File dữ liệu (.db/.duckdb/.csv/.xlsx...)',CONFIG['db'])
            CONFIG['staff_file']=_ask('File danh mục nhân viên y tế',CONFIG['staff_file'])
            CONFIG['gphd']=_ask('Số GPHĐ bệnh viện (vd 01366/HCM-GPHĐ)',CONFIG['gphd'])
        args=parse_args(argv)
        if args.list_snapshots or args.download_snapshot or args.delete_snapshot:
            cfg=build_cfg(args);d=snapshot_dir(cfg)
            if args.delete_snapshot:
                p=resolve_snapshot(dict(cfg,snapshot=args.delete_snapshot));delete_snapshot(p);log(f'Đã xoá bản lưu: {p.name}')
            if args.download_snapshot:download_snapshot(cfg['gphd'],d)
            if args.list_snapshots or args.download_snapshot or args.delete_snapshot:
                for x in list_snapshots(d):log(f'{x["name"]} · GPHĐ {x["gphd"]} · {x["facility"]} · {x["saved_at"]} · {x["n"]} người')
                if not list_snapshots(d):log(f'(chưa có bản lưu nào trong {d})')
            if interactive:_pause()
            return 0
        cfg=build_cfg(args)
        res=run(cfg)
    except SystemExit as e:
        if interactive:_pause()
        raise
    except Exception as e:
        if cancelled():log('ĐÃ HỦY.');rc=130
        else:
            if isinstance(e,ValueError):log(f'LỖI cấu hình: {e}')
            else:log('LỖI:\n'+traceback.format_exc())
            rc=1
        if interactive:_pause()
        return rc
    rc=0
    if res is None:
        log('ĐÃ HỦY — không ghi file kết quả.');rc=130
    if interactive:_pause()
    return rc
if __name__=='__main__':
    sys.exit(main())
