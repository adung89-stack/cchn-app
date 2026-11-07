"""cchn_gui.py — Giao diện cho mục F "Xác thực CCHN" (tách riêng, không có Preview).

Chọn file .db (hoặc .duckdb/.parquet/.csv/.xlsx) → chọn file danh mục nhân viên, nhập GPHĐ,
chọn cặp cột, kiểu xuất, nơi lưu → bấm ▶ Xác thực CCHN. Có khung LOG trực tiếp, đồng thời ghi file <kết quả>_log.txt.
Toàn bộ logic dùng lại nguyên cchn_verify.py + medinet_lookup.py (để CÙNG THƯ MỤC).
Cần: pip install PySide6 duckdb pandas openpyxl requests beautifulsoup4
Chạy: python cchn_gui.py
"""
from __future__ import annotations
import logging,logging.handlers,os,sys,threading,time,traceback
from pathlib import Path
if sys.stdout is None:sys.stdout=open(os.devnull,'w')
if sys.stderr is None:sys.stderr=open(os.devnull,'w')
sys.path.insert(0,str(Path(__file__).resolve().parent))
from PySide6.QtCore import Qt,QObject,QThread,Signal,QSettings,QUrl
from PySide6.QtGui import QDesktopServices,QFont,QTextCursor
from PySide6.QtWidgets import (QApplication,QCheckBox,QComboBox,QFileDialog,QGridLayout,QGroupBox,QHBoxLayout,QLabel,QLineEdit,
    QMainWindow,QMessageBox,QPlainTextEdit,QProgressBar,QPushButton,QRadioButton,QVBoxLayout,QWidget,QButtonGroup,QDialog,QTableWidget,
    QTableWidgetItem,QAbstractItemView,QHeaderView,QScrollArea,QSpinBox,QFrame)
from PySide6.QtGui import QShortcut,QKeySequence,QGuiApplication
import cchn_verify as CV
import medinet_lookup as ML
if not hasattr(CV,'LOGGER') or not hasattr(CV,'run'):
    _m=(f'File "{getattr(CV,"__file__","cchn_verify.py")}" KHÔNG phải bản logic của cchn_verify.py '
        '(có thể đã lưu nhầm nội dung cchn_gui.py đè lên tên cchn_verify.py).\n'
        'Hãy tải lại ĐÚNG 2 file riêng biệt: cchn_verify.py (logic, không giao diện) và cchn_gui.py (giao diện), để cùng thư mục.')
    print(_m)
    try:
        from PySide6.QtWidgets import QApplication as _A,QMessageBox as _M
        _a=_A.instance() or _A(sys.argv);_M.critical(None,'Sai file',_m)
    except Exception:pass
    sys.exit(1)

MODES=[('summary','1 sheet kết quả (mặc định)'),('multi_sheet','1 file, 5 sheet (kết quả + 4 chi tiết)'),('multi_file','5 file riêng (kết quả + 4 chi tiết)')]
SRC_FILTER='Dữ liệu (*.db *.sqlite *.sqlite3 *.duckdb *.parquet *.csv *.xlsx *.xls);;Tất cả (*.*)'

class QtLogHandler(logging.Handler,QObject):
    line=Signal(str)
    def __init__(self):
        logging.Handler.__init__(self);QObject.__init__(self)
        self.setFormatter(logging.Formatter('%(asctime)s  %(message)s','%H:%M:%S'))
    def emit(self,record):
        try:self.line.emit(self.format(record))
        except Exception:pass

def reset_file_logging():
    """Mỗi lượt chạy có file log riêng: bỏ file handler của lượt trước, tạo lại bộ đệm cho các dòng log đến lúc biết đường dẫn file."""
    L=CV.LOGGER;L.setLevel(logging.INFO);L.propagate=False
    for h in list(L.handlers):
        if isinstance(h,(logging.FileHandler,logging.handlers.MemoryHandler)):
            L.removeHandler(h)
            try:h.close()
            except Exception:pass
    m=logging.handlers.MemoryHandler(capacity=5000,flushLevel=logging.CRITICAL+10,target=None)
    m.setFormatter(logging.Formatter('%(asctime)s  %(message)s','%H:%M:%S'));L.addHandler(m);CV._MEM=m

class Task(QThread):
    done=Signal(object,object)   # (kết quả, lỗi)
    def __init__(self,fn):super().__init__();self.fn=fn
    def run(self):
        try:self.done.emit(self.fn(),None)
        except BaseException as e:
            e._tb=traceback.format_exc();self.done.emit(None,e)

def _avail():
    """Vùng làm việc của màn hình đang dùng (đã trừ thanh tác vụ/Dock)."""
    sc=QGuiApplication.primaryScreen();return sc.availableGeometry() if sc else None
def fit_size(w,h,frac=0.94):
    """Kích thước cửa sổ không vượt quá vùng làm việc của màn hình (laptop nhỏ / MacBook Air)."""
    a=_avail()
    if a is None:return w,h
    return min(w,int(a.width()*frac)),min(h,int(a.height()*frac))
BASE_PT=[0.0]
def apply_zoom(pct):
    """Thu/phóng toàn bộ giao diện bằng cỡ chữ (mọi nút, ô nhập, bảng, log co giãn theo)."""
    pct=max(60,min(160,int(pct)))
    app=QApplication.instance();f=app.font()
    if not BASE_PT[0]:BASE_PT[0]=f.pointSizeF() if f.pointSizeF()>0 else 10.0
    f.setPointSizeF(BASE_PT[0]*pct/100.0);app.setFont(f);return pct
def auto_zoom():
    """Mặc định theo màn hình: màn hình thấp thì tự thu nhỏ để thấy đủ."""
    a=_avail()
    if a is None:return 100
    h=a.height()
    return 100 if h>=900 else 90 if h>=800 else 80 if h>=720 else 70

def wrap_text(t,width=52):
    """Ngắt dòng tại khoảng trắng để nhãn dài (checkbox/radio) không làm cửa sổ rộng ra."""
    if '\n' in t or len(t)<=width:return t
    out=[];cur=''
    for w in t.split(' '):
        if cur and len(cur)+1+len(w)>width:out.append(cur);cur=w
        else:cur=(cur+' '+w) if cur else w
    out.append(cur);return '\n'.join(out)
def wrap_labels(root,width=52):
    for T in (QCheckBox,QRadioButton):
        for c in root.findChildren(T):
            t=c.text()
            if len(t)>width and '\n' not in t:
                if not c.toolTip():c.setToolTip(t)
                c.setText(wrap_text(t,width))

class SnapDialog(QDialog):
    """Quản lý các bản lưu danh sách nhân sự cơ sở (dùng chung định dạng với app chính): xem, quét mới từ cổng, nhập/xuất Excel, xoá, chọn dùng."""
    HEAD=['Tên file','GPHĐ','Cơ sở','Lưu lúc','Số nhân sự','Ghi chú']
    def __init__(self,parent,gphd,directory_cfg):
        super().__init__(parent);self.setWindowTitle('Bản lưu danh sách nhân sự cơ sở');self.resize(*fit_size(900,460))
        self.cfg=directory_cfg;self.chosen=None;self.busy=False;self.rows=[]
        v=QVBoxLayout(self)
        v.addWidget(QLabel('Mỗi bản lưu là danh sách nhân sự + "Thời gian hoạt động" của cơ sở tại thời điểm quét. Chọn bản lưu khi chạy để KHÔNG phải tải lại từ web (hạn chế bị chặn). '
                           'Quét mới / nhập Excel không ghi đè bản nào; chỉ xoá khi bạn bấm Xoá. Excel nhập/xuất theo bố cục file mẫu của cổng (sheet "Thông tin chung" + "Nhân sự").'))
        self.tbl=QTableWidget(0,len(self.HEAD));self.tbl.setHorizontalHeaderLabels(self.HEAD);self.tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbl.setSelectionMode(QAbstractItemView.SingleSelection);self.tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive);self.tbl.horizontalHeader().setStretchLastSection(True)
        self.tbl.setColumnWidth(0,300);self.tbl.setColumnWidth(1,120);self.tbl.setColumnWidth(2,150);self.tbl.setColumnWidth(3,140);self.tbl.setColumnWidth(4,80)
        self.tbl.doubleClicked.connect(self.use);v.addWidget(self.tbl,1)
        h=QHBoxLayout();h.addWidget(QLabel('GPHĐ cơ sở:'));self.gphd=QLineEdit(gphd);self.gphd.setPlaceholderText('vd: 01366/HCM-GPHĐ');h.addWidget(self.gphd,1)
        self.b_dl=QPushButton('⬇ Quét mới từ cổng và lưu');self.b_dl.clicked.connect(self.download);h.addWidget(self.b_dl)
        self.b_stop=QPushButton('⏹ Dừng tải');self.b_stop.setEnabled(False);self.b_stop.clicked.connect(lambda:CV._CANCEL.set());h.addWidget(self.b_stop);v.addLayout(h)
        h=QHBoxLayout()
        self.b_imp=QPushButton('📥 Nhập từ Excel...');self.b_imp.clicked.connect(self.import_file)
        self.b_exp=QPushButton('📤 Xuất ra Excel...');self.b_exp.clicked.connect(self.export_file)
        self.b_del=QPushButton('🗑 Xoá bản đã chọn');self.b_del.clicked.connect(self.delete)
        self.b_dir=QPushButton('📁 Mở thư mục');self.b_dir.clicked.connect(self.open_dir)
        self.b_use=QPushButton('✔ Dùng bản đã chọn');self.b_use.clicked.connect(self.use);self.b_use.setStyleSheet('font-weight:bold')
        b_close=QPushButton('Đóng');b_close.clicked.connect(self.reject)
        for b in (self.b_imp,self.b_exp,self.b_del,self.b_dir):h.addWidget(b)
        h.addStretch(1);h.addWidget(self.b_use);h.addWidget(b_close);v.addLayout(h)
        self.status=QLabel('');v.addWidget(self.status);self.refresh()
    def refresh(self,select=None):
        try:self.rows=CV.list_snapshots(CV.snapshot_dir(self.cfg))
        except Exception as e:self.rows=[];self.status.setText(f'Lỗi đọc thư mục bản lưu: {e}')
        self.tbl.setRowCount(len(self.rows))
        for i,r in enumerate(self.rows):
            for j,k in enumerate(('name','gphd','facility','saved_at','n','note')):self.tbl.setItem(i,j,QTableWidgetItem(str(r[k])))
            if select and r['path']==select:self.tbl.selectRow(i)
        if self.rows and select is None and not self.tbl.selectedItems():self.tbl.selectRow(0)
        self.status.setText(f'{len(self.rows)} bản lưu trong: {CV.snapshot_dir(self.cfg)}')
    def current(self):
        i=self.tbl.currentRow();return self.rows[i] if 0<=i<len(self.rows) else None
    def set_busy(self,b):
        self.busy=b
        for w in (self.b_dl,self.b_imp,self.b_exp,self.b_del,self.b_dir,self.b_use,self.tbl,self.gphd):w.setEnabled(not b)
        self.b_stop.setEnabled(b and self._can_stop)
    _can_stop=False
    def _run(self,fn,done,can_stop=False):
        self._can_stop=can_stop;self.set_busy(True);CV._CANCEL.clear();reset_file_logging()
        self.t=Task(fn);self.t.done.connect(lambda r,e:(self.set_busy(False),done(r,e)));self.t.start()
    def download(self):
        g=self.gphd.text().strip()
        if not g:QMessageBox.information(self,'Bản lưu','Hãy nhập số GPHĐ của cơ sở.');return
        self.status.setText('Đang quét danh sách nhân sự từ cổng... (xem tiến trình ở khung log chính)')
        def done(r,e):
            if e is not None:
                if CV.cancelled():self.status.setText('Đã dừng tải.');return
                QMessageBox.warning(self,'Bản lưu',f'Không tải được: {e}');self.refresh();return
            self.refresh(select=str(r));self.status.setText('Đã tải và lưu bản mới.')
        self._run(lambda:CV.download_snapshot(g,CV.snapshot_dir(self.cfg)),done,can_stop=True)
    def import_file(self):
        p,_=QFileDialog.getOpenFileName(self,'Chọn file Excel danh sách nhân sự (bố cục mẫu của cổng)','','Excel (*.xlsx *.xlsm)')
        if not p:return
        g=self.gphd.text().strip()
        def done(r,e):
            if e is not None:QMessageBox.warning(self,'Bản lưu',f'Không thêm được: {e}');return
            self.refresh(select=str(r));self.status.setText('Đã thêm bản lưu từ file Excel.')
        self._run(lambda:CV.import_snapshot(p,g,CV.snapshot_dir(self.cfg)),done)
    def export_file(self):
        r=self.current()
        if not r:return
        import syt_store
        fn,_=QFileDialog.getSaveFileName(self,'Xuất bản lưu ra Excel',syt_store.default_export_name(syt_store.read_meta(r['path']) or {}),'Excel (*.xlsx)')
        if not fn:return
        try:CV.export_snapshot(r['path'],fn);self.status.setText(f'Đã xuất: {fn}')
        except Exception as e:QMessageBox.warning(self,'Bản lưu',f'Không xuất được: {e}')
    def delete(self):
        r=self.current()
        if not r:return
        if QMessageBox.question(self,'Xoá bản lưu',f'Xoá bản lưu "{r["name"]}"?')!=QMessageBox.Yes:return
        try:CV.delete_snapshot(r['path'])
        except Exception as e:QMessageBox.warning(self,'Bản lưu',f'Không xoá được: {e}')
        self.refresh()
    def open_dir(self):QDesktopServices.openUrl(QUrl.fromLocalFile(str(CV.snapshot_dir(self.cfg))))
    def use(self,*_):
        r=self.current()
        if r:self.chosen=r;self.accept()

class Win(QMainWindow):
    def __init__(self):
        super().__init__();self.setWindowTitle('Xác thực CCHN (mục F) — độc lập');self.resize(*fit_size(980,860))
        self.st=QSettings('GeneralDataProcessor','cchn_gui');self.columns=[];self.task=None;self.run_busy=False;self.last_dir=''
        self._build();wrap_labels(self);self._wire_log();self._restore()
    # ---------------- giao diện ----------------
    def _row_file(self,label,edit,btn_text,slot,ph=''):
        b=QPushButton(btn_text);b.clicked.connect(slot);edit.setPlaceholderText(ph);return b
    def _combo(self):
        c=QComboBox();c.setEditable(False);c.setMinimumWidth(170);return c
    def _build(self):
        root=QWidget();v=QVBoxLayout(root)
        # cuộn được khi màn hình nhỏ: không bao giờ mất phần dưới/bên phải; thu/phóng ở góc trên (hoặc Ctrl +/-/0)
        sa=QScrollArea();sa.setWidgetResizable(True);sa.setFrameShape(QFrame.NoFrame);sa.setWidget(root);self.setCentralWidget(sa);self.scroll=sa
        self.zoom_sb=QSpinBox();self.zoom_sb.setRange(60,160);self.zoom_sb.setSingleStep(10);self.zoom_sb.setSuffix(' %');self.zoom_sb.setToolTip('Thu/phóng giao diện (Ctrl + / Ctrl - / Ctrl 0)')
        self.zoom_sb.valueChanged.connect(self.set_zoom)
        zl=QLabel('🔍 Thu/phóng:');zb=QPushButton('Tự động');zb.clicked.connect(lambda:self.zoom_sb.setValue(auto_zoom()))
        zh=QHBoxLayout();zh.addStretch(1);zh.addWidget(zl);zh.addWidget(self.zoom_sb);zh.addWidget(zb);v.addLayout(zh)
        for k,fn in (('Ctrl+=',lambda:self.zoom_sb.setValue(self.zoom_sb.value()+10)),('Ctrl++',lambda:self.zoom_sb.setValue(self.zoom_sb.value()+10)),
                     ('Ctrl+-',lambda:self.zoom_sb.setValue(self.zoom_sb.value()-10)),('Ctrl+0',lambda:self.zoom_sb.setValue(100))):
            QShortcut(QKeySequence(k),self,activated=fn)
        # 1. Nguồn
        g=QGroupBox('1. Nguồn dữ liệu (.db)');f=QGridLayout(g)
        self.db_edit=QLineEdit();self.db_edit.setReadOnly(True)
        f.addWidget(QLabel('File dữ liệu'),0,0);f.addWidget(self.db_edit,0,1)
        f.addWidget(self._row_file('',self.db_edit,'📂 Chọn file .db...',self.pick_db,'(chưa chọn file)'),0,2)
        self.table_cb=self._combo();self.table_cb.currentTextChanged.connect(self.on_table_changed)
        f.addWidget(QLabel('Bảng / sheet'),1,0);f.addWidget(self.table_cb,1,1)
        self.info_lbl=QLabel('');self.info_lbl.setWordWrap(True);f.addWidget(self.info_lbl,2,0,1,3)
        f.setColumnStretch(1,1);v.addWidget(g)
        # 2. Cặp cột
        g=QGroupBox('2. Cặp cột đối chiếu (mã bác sĩ · thời gian)');f=QGridLayout(g)
        self.p1_on=QCheckBox('Cặp 1');self.p1_on.setChecked(True);self.p1_doc=self._combo();self.p1_time=self._combo()
        self.p2_on=QCheckBox('Cặp 2');self.p2_on.setChecked(True);self.p2_doc=self._combo();self.p2_time=self._combo()
        for r,(cb,d,t) in enumerate(((self.p1_on,self.p1_doc,self.p1_time),(self.p2_on,self.p2_doc,self.p2_time))):
            f.addWidget(cb,r,0);f.addWidget(QLabel('Cột mã bác sĩ'),r,1);f.addWidget(d,r,2);f.addWidget(QLabel('Cột thời gian'),r,3);f.addWidget(t,r,4)
        f.setColumnStretch(2,1);f.setColumnStretch(4,1);v.addWidget(g)
        # 3. Danh mục & GPHĐ
        g=QGroupBox('3. File liên quan');f=QGridLayout(g)
        self.staff_edit=QLineEdit();self.staff_edit.setReadOnly(True)
        f.addWidget(QLabel('Danh mục nhân viên'),0,0);f.addWidget(self.staff_edit,0,1)
        f.addWidget(self._row_file('',self.staff_edit,'📂 Chọn file danh mục...',self.pick_staff,'(chưa chọn file)'),0,2)
        self.key_cb=self._combo();self.key_cb.addItem('(chọn file trước)','')
        f.addWidget(QLabel('Cột mã CCHN'),1,0);f.addWidget(self.key_cb,1,1)
        self.gphd_edit=QLineEdit();self.gphd_edit.setPlaceholderText('vd: 01366/HCM-GPHĐ')
        f.addWidget(QLabel('Số GPHĐ bệnh viện'),2,0);f.addWidget(self.gphd_edit,2,1)
        f.setColumnStretch(1,1);v.addWidget(g)
        # 3a. Bản lưu danh sách nhân sự
        g=QGroupBox('Bản lưu danh sách nhân sự cơ sở (dùng lại, khỏi tải web)');f=QGridLayout(g)
        self.snap_cb=QComboBox();self.snap_cb.setMinimumWidth(420);self.snap_cb.currentIndexChanged.connect(self.on_snap_changed)
        f.addWidget(QLabel('Nguồn danh sách'),0,0);f.addWidget(self.snap_cb,0,1)
        mb=QPushButton('⚙ Quản lý bản lưu (tải mới / thêm / xoá)...');mb.clicked.connect(self.manage_snaps);f.addWidget(mb,0,2)
        self.offline_cb=QCheckBox('Chỉ dùng bản lưu — KHÔNG kết nối mạng (offline), không cần nhập GPHĐ')
        self.offline_cb.setToolTip('Tra cứu hoàn toàn từ bản lưu. Mã bác sĩ không có trong bản lưu sẽ không được tra web (xem như chưa có dữ liệu).')
        self.offline_cb.toggled.connect(self.on_snap_changed);f.addWidget(self.offline_cb,1,0,1,3)
        hint=QLabel('Chọn "Tự động": dùng bản quét ≤30 ngày của GPHĐ, chưa có thì tự quét 1 bản mới và lưu. Chọn 1 bản lưu cụ thể thì chạy theo bản đó (GPHĐ để trống cũng được); '
                    'tích offline thì tuyệt đối không kết nối mạng (mã chưa có trong bản lưu coi như chưa có dữ liệu).')
        hint.setWordWrap(True);f.addWidget(hint,3,0,1,3);f.setColumnStretch(1,1);v.addWidget(g)
        # 3b. Loại hình KCB
        g=QGroupBox('Loại hình KCB (MA_LOAI_KCB) — tích nhiều loại; bỏ trống = xuất tất cả');f=QGridLayout(g)
        self.kcb_cbs={}
        for i,(k,lab) in enumerate(CV.LOAI_KCB.items()):
            short=lab.replace(' (theo TT 46/2016/TT-BYT)','').replace(' liên tục trong năm','')
            cb=QCheckBox(f'{k} — {short}');cb.setToolTip(f'{k}: {lab}');self.kcb_cbs[k]=cb;f.addWidget(cb,i%5,i//5)
        clr=QPushButton('Bỏ chọn hết');clr.clicked.connect(lambda:[c.setChecked(False) for c in self.kcb_cbs.values()]);f.addWidget(clr,5,0)
        f.setColumnStretch(0,1);f.setColumnStretch(1,1);v.addWidget(g)
        # 4. Tuỳ chọn + xuất
        g=QGroupBox('4. Tuỳ chọn & xuất kết quả');f=QVBoxLayout(g)
        self.web1_safe=QCheckBox('Giới hạn tốc độ web 1 (bổ sung) xuống mức an toàn đã đo (~10s/mã, CHẬM hơn nhiều — mặc định tắt để ưu tiên tốc độ)')
        f.addWidget(self.web1_safe)
        h=QHBoxLayout();h.addWidget(QLabel('Kiểu xuất:'));self.mode_grp=QButtonGroup(self);self.mode_btns={}
        for i,(k,lab) in enumerate(MODES):
            rb=QRadioButton(lab);self.mode_grp.addButton(rb,i);self.mode_btns[k]=rb;h.addWidget(rb)
        self.mode_btns['summary'].setChecked(True);h.addStretch(1);f.addLayout(h)
        h=QHBoxLayout();h.addWidget(QLabel('Lưu vào:'));self.out_edit=QLineEdit();self.out_edit.setPlaceholderText('(mặc định: cạnh file .db, tên <mã CSKCB>_<bảng>_xac_thuc_cchn.xlsx)')
        h.addWidget(self.out_edit,1);ob=QPushButton('Chọn nơi lưu...');ob.clicked.connect(self.pick_out);h.addWidget(ob);f.addLayout(h)
        h=QHBoxLayout();self.overwrite=QCheckBox('Ghi đè nếu file đã có (mặc định: đặt tên _001, _002...)');self.ssd=QCheckBox('SSD/NVMe (nhiều luồng hơn)')
        self.cache=QCheckBox('Tạo cache Parquet tạm cho .db ≥ 50MB (xoá khi xong; thường không cần)');self.cache.setChecked(False)
        for w in (self.overwrite,self.ssd,self.cache):h.addWidget(w)
        h.addStretch(1);f.addLayout(h);v.addWidget(g)
        # 5. Đối chiếu file DM nhân viên do đơn vị gửi với SYT (không cần .db)
        g=QGroupBox('5. Đối chiếu file DM nhân viên đơn vị gửi với SYT (không cần .db)');f=QGridLayout(g)
        self.sc_file='';self.sc_label=QLabel('(chưa chọn file)');self.sc_label.setWordWrap(True)
        b=QPushButton('📂 Chọn file DM nhân viên của đơn vị...');b.clicked.connect(self.pick_sc_file);f.addWidget(b,0,0);f.addWidget(self.sc_label,0,1,1,2)
        self.sc_key=self._combo();self.sc_key.addItem('(chọn file trước)','')
        f.addWidget(QLabel('Cột mã CCHN (file đơn vị gửi)'),1,0);f.addWidget(self.sc_key,1,1)
        self.sc_fresh=QCheckBox('Quét lại danh sách nhân sự từ cổng trước khi đối chiếu (bỏ qua nếu đã chọn 1 bản lưu cụ thể ở mục 3 — khi đó chạy offline)');f.addWidget(self.sc_fresh,2,0,1,3)
        self.sc_btn=QPushButton('🔎 Đối chiếu với SYT → xuất file đơn vị + cột đối chiếu (ô sai tô đỏ)');self.sc_btn.clicked.connect(self.run_sc);f.addWidget(self.sc_btn,3,0,1,3)
        self.sc_status=QLabel('');self.sc_status.setWordWrap(True);f.addWidget(self.sc_status,4,0,1,3)
        f.setColumnStretch(1,1);v.addWidget(g)
        # chạy
        h=QHBoxLayout();self.run_btn=QPushButton('▶ Xác thực CCHN');self.run_btn.setMinimumHeight(36);self.run_btn.setStyleSheet('font-weight:bold')
        self.run_btn.clicked.connect(self.start)
        self.stop_btn=QPushButton('⏹ Dừng');self.stop_btn.setMinimumHeight(36);self.stop_btn.setEnabled(False);self.stop_btn.clicked.connect(self.stop)
        self.open_btn=QPushButton('📁 Mở thư mục kết quả');self.open_btn.setMinimumHeight(36);self.open_btn.setEnabled(False);self.open_btn.clicked.connect(self.open_dir)
        h.addWidget(self.run_btn,2);h.addWidget(self.stop_btn,1);h.addWidget(self.open_btn,1);v.addLayout(h)
        self.bar=QProgressBar();self.bar.setRange(0,1);self.bar.setTextVisible(False);self.bar.setMaximumHeight(8);v.addWidget(self.bar)
        self.log_view=QPlainTextEdit();self.log_view.setReadOnly(True);self.log_view.setMaximumBlockCount(20000)
        fnt=QFont('Consolas');fnt.setStyleHint(QFont.Monospace);fnt.setPointSize(9);self.log_view.setFont(fnt);self.log_view.setMinimumHeight(170)
        v.addWidget(QLabel('Log (đồng thời ghi vào file <kết quả>_log.txt):'));v.addWidget(self.log_view,1)
    def _wire_log(self):
        self.qh=QtLogHandler();self.qh.line.connect(self.append_log);CV.LOGGER.setLevel(logging.INFO);CV.LOGGER.propagate=False
        CV.LOGGER.addHandler(self.qh)
    def append_log(self,s):
        self.log_view.appendPlainText(s);self.log_view.moveCursor(QTextCursor.End)
    def say(self,s):CV.log(s)
    # ---------------- bản lưu danh sách nhân sự ----------------
    def refresh_snaps(self,select=None):
        sel=select if select is not None else (self.snap_cb.currentData() or '')
        self.snap_cb.blockSignals(True);self.snap_cb.clear();self.snap_cb.addItem('🌐 Tự động (dùng bản ≤30 ngày, chưa có thì quét mới)','')
        try:rows=CV.list_snapshots(CV.snapshot_dir())
        except Exception:rows=[]
        self._snaps={r['path']:r for r in rows}
        for r in rows:self.snap_cb.addItem(f"📄 {r['facility'] or r['gphd']} · {r['gphd']} · {r['saved_at'][:16].replace('T',' ')} · {r['n']} người",r['path'])
        i=self.snap_cb.findData(sel);self.snap_cb.setCurrentIndex(i if i>=0 else 0);self.snap_cb.blockSignals(False);self.on_snap_changed()
    def on_snap_changed(self,_=None):
        r=self._snaps.get(self.snap_cb.currentData() or '')
        has=r is not None
        self.offline_cb.setEnabled(has)
        if not has:self.offline_cb.blockSignals(True);self.offline_cb.setChecked(False);self.offline_cb.blockSignals(False)
        off=has and self.offline_cb.isChecked()
        self.gphd_edit.setEnabled(not off)
        self.gphd_edit.setPlaceholderText(f'(để trống = dùng GPHĐ của bản lưu: {r["gphd"]})' if has else 'vd: 01366/HCM-GPHĐ')
    def manage_snaps(self):
        d=SnapDialog(self,self.gphd_edit.text().strip(),{})
        if d.exec()==QDialog.Accepted and d.chosen:self.refresh_snaps(select=d.chosen['path'])
        else:self.refresh_snaps()
        wrap_labels(self);self.on_snap_changed()
    def set_zoom(self,pct):
        p=apply_zoom(pct)
        if self.zoom_sb.value()!=p:self.zoom_sb.setValue(p)
        # bố cục tự tính lại theo cỡ chữ mới; cửa sổ không vượt quá màn hình
        self.resize(*fit_size(self.width(),self.height()))
    # ---------------- lưu/khôi phục ----------------
    def _restore(self):
        g=lambda k:self.st.value(k,'') or ''
        try:z=int(g('zoom') or 0)
        except ValueError:z=0
        self.zoom_sb.setValue(z if 60<=z<=160 else auto_zoom())
        self.gphd_edit.setText(g('gphd'));self.last_dir=g('last_dir')
        self.refresh_snaps(select=g('snapshot'));self.offline_cb.setChecked(g('offline')=='1');self.on_snap_changed()
        sf=g('staff_file')
        if sf and Path(sf).exists():self.set_staff(sf,g('key_col'))
    def closeEvent(self,e):
        if self.run_busy:
            if QMessageBox.question(self,'Đang chạy','Đang xác thực — thoát sẽ huỷ lượt chạy. Thoát?')!=QMessageBox.Yes:e.ignore();return
            self.stop()
        for k,v in (('gphd',self.gphd_edit.text()),('staff_file',self.staff_edit.text()),('key_col',self.key_cb.currentData() or self.key_cb.currentText()),('last_dir',self.last_dir),('snapshot',self.snap_cb.currentData() or ''),('offline','1' if self.offline_cb.isChecked() else '0'),('zoom',str(self.zoom_sb.value()))):self.st.setValue(k,v)
        e.accept()
    # ---------------- chọn file ----------------
    def _start_dir(self):return self.last_dir if self.last_dir and Path(self.last_dir).exists() else ''
    def pick_db(self):
        p,_=QFileDialog.getOpenFileName(self,'Chọn file dữ liệu',self._start_dir(),SRC_FILTER)
        if p:self.load_source(p)
    def load_source(self,path,table=None):
        self.last_dir=str(Path(path).parent);self.info_lbl.setText('Đang đọc cấu trúc file...');self.setEnabled(False);QApplication.setOverrideCursor(Qt.WaitCursor)
        ssd=self.ssd.isChecked()
        def work():
            src,tables=CV.open_source(path,table,ssd,cache=False)
            try:return tables,src.table,src.columns()
            finally:src.close()
        self._tsk=Task(work);self._tsk.done.connect(lambda r,e:self._source_loaded(path,r,e));self._tsk.start()
    def _source_loaded(self,path,r,e):
        QApplication.restoreOverrideCursor();self.setEnabled(True)
        if e is not None:
            self.info_lbl.setText('');QMessageBox.critical(self,'Không đọc được file',str(e));return
        tables,table,cols=r;self.db_edit.setText(path);self.columns=cols
        self.table_cb.blockSignals(True);self.table_cb.clear();self.table_cb.addItems(tables);self.table_cb.setCurrentText(table);self.table_cb.blockSignals(False)
        self.fill_columns();self.info_lbl.setText(f'{Path(path).name} · {len(tables)} bảng · bảng "{table}" có {len(cols)} cột')
        self.say(f'Đã chọn nguồn: {path} · bảng "{table}" · {len(cols)} cột')
    def on_table_changed(self,t):
        p=self.db_edit.text()
        if p and t and not self.run_busy:self.load_source(p,t)
    def fill_columns(self):
        cols=self.columns
        for c in (self.p1_doc,self.p1_time,self.p2_doc,self.p2_time):
            c.clear();c.addItem('');c.addItems(cols)
        has=lambda *n:all(x in cols for x in n)
        if 'MA_BAC_SI' in cols:self.p1_doc.setCurrentText('MA_BAC_SI')
        if 'NGAY_YL' in cols:self.p1_time.setCurrentText('NGAY_YL')
        if has('MA_BSI_THUC_HIEN','NGAY_TH_YL'):
            self.p2_doc.setCurrentText('MA_BSI_THUC_HIEN');self.p2_time.setCurrentText('NGAY_TH_YL');self.p2_on.setChecked(True)
        else:self.p2_on.setChecked(False)
    def pick_staff(self):
        p,_=QFileDialog.getOpenFileName(self,'Chọn file danh mục nhân viên',self._start_dir(),'Excel/CSV (*.xlsx *.xls *.csv)')
        if p:self.last_dir=str(Path(p).parent);self.set_staff(p,'',warn=True)
    def set_staff(self,p,key,warn=False):
        try:
            import medinet_lookup as ML
            cols=[str(c) for c in ML._read_staff_dir_df(Path(p)).columns]
        except Exception as e:
            if warn:QMessageBox.warning(self,'Chọn file danh mục nhân viên',f'Không đọc được file: {e}')
            return
        self.staff_edit.setText(p);self.key_cb.clear();self.key_cb.addItems(cols)
        if key in cols:self.key_cb.setCurrentText(key)
        elif 'MACCHN' in cols:self.key_cb.setCurrentText('MACCHN')
    def pick_out(self):
        cur=self.out_edit.text() or (str(Path(self.db_edit.text()).parent/'xac_thuc_cchn.xlsx') if self.db_edit.text() else 'xac_thuc_cchn.xlsx')
        p,_=QFileDialog.getSaveFileName(self,'Lưu kết quả xác thực CCHN',cur,'Excel (*.xlsx)')
        if p:self.out_edit.setText(p if p.lower().endswith('.xlsx') else p+'.xlsx')
    # ---------------- chạy ----------------
    def cfg(self):
        mode=next(k for k,_ in MODES if self.mode_btns[k].isChecked())
        c=dict(CV.CONFIG)
        c.update(db=self.db_edit.text(),table=self.table_cb.currentText(),staff_file=self.staff_edit.text(),staff_key_col='' if self.key_cb.currentText()=='(chọn file trước)' else self.key_cb.currentText(),
            gphd=self.gphd_edit.text().strip(),out=self.out_edit.text().strip(),mode=mode,
            pair1_on=self.p1_on.isChecked(),pair1_doctor=self.p1_doc.currentText(),pair1_time=self.p1_time.currentText(),
            pair2_on=self.p2_on.isChecked(),pair2_doctor=self.p2_doc.currentText(),pair2_time=self.p2_time.currentText(),
            loai_kcb=[k for k,c in self.kcb_cbs.items() if c.isChecked()],web1_safe=self.web1_safe.isChecked(),
            snapshot=self.snap_cb.currentData() or '',offline=self.offline_cb.isChecked(),ssd=self.ssd.isChecked(),cache=self.cache.isChecked(),overwrite=self.overwrite.isChecked())
        return c
    # ---------------- đối chiếu file DM nhân viên đơn vị gửi ----------------
    def pick_sc_file(self):
        p,_=QFileDialog.getOpenFileName(self,'Chọn file DM nhân viên do đơn vị gửi',self.last_dir,'Excel (*.xlsx)')
        if not p:return
        try:cols=[str(c) for c in ML._read_staff_dir_df(Path(p)).columns]
        except Exception as e:QMessageBox.warning(self,'Đối chiếu',f'Không đọc được file: {e}');return
        need=[c for c in ('THOIGIAN_NGAY','THOIGIAN_TUAN','TU_NGAY') if c not in cols]
        if need:QMessageBox.information(self,'Đối chiếu','File thiếu cột '+', '.join(need)+' nên chỉ đối chiếu được các phần còn lại (có thể bạn chọn nhầm file FileDM_NHANVIENYTE).')
        self.sc_file=p;self.last_dir=str(Path(p).parent);self.sc_label.setText(Path(p).name)
        self.sc_key.clear();self.sc_key.addItems(cols)
        if 'MACCHN' in cols:self.sc_key.setCurrentText('MACCHN')
    def run_sc(self):
        if self.run_busy:return
        if not self.sc_file:QMessageBox.information(self,'Đối chiếu','Hãy chọn file DM nhân viên của đơn vị trước.');return
        key=self.sc_key.currentText()
        if not key or key.startswith('('):QMessageBox.information(self,'Đối chiếu','Hãy chọn cột mã CCHN.');return
        g=self.gphd_edit.text().strip();snap=self.snap_cb.currentData() or ''
        if not g and not snap:QMessageBox.information(self,'Đối chiếu','Hãy nhập GPHĐ của cơ sở hoặc chọn 1 bản lưu danh sách nhân sự.');return
        import staff_syt_check
        CV._CANCEL.clear();reset_file_logging();self.set_busy(True);self.sc_status.setText('Đang đối chiếu...')
        def work():
            CV.snapshot_dir();ML.set_cache_dir(CV.snapshot_dir())
            try:return staff_syt_check.run(self.sc_file,key,g,snapshot=snap,fresh=self.sc_fresh.isChecked(),log=CV.log,cancelled=CV.cancelled)
            finally:ML.set_offline(False);ML.set_facility_snapshot('')
        def done(r,e):
            self.set_busy(False)
            if e is not None:self.sc_status.setText('Lỗi: '+str(e));QMessageBox.warning(self,'Đối chiếu',str(e));return
            out,st,err=r
            if err:self.sc_status.setText('Lỗi: '+err);QMessageBox.warning(self,'Đối chiếu',err);return
            self.refresh_snaps(select=snap or None)
            m=(f"Đã xuất: {out}\nTổng {st.get('tong',0)} dòng — khớp {st.get('khop',0)}, có sai lệch {st.get('co_loi',0)} "
               f"(không có trên SYT/cơ sở: {st.get('khong_co_syt',0)}); SYT đang làm nhưng danh mục thiếu: {st.get('syt_thieu_trong_dm',0)}.")
            self.sc_status.setText(m);self.last_out=str(Path(out).parent);self.open_btn.setEnabled(True);QMessageBox.information(self,'Đối chiếu',m)
        self.sc_task=Task(work);self.sc_task.done.connect(done);self.sc_task.start()
    def set_busy(self,b):
        self.run_busy=b
        for w in (self.run_btn,):w.setEnabled(not b)
        self.stop_btn.setEnabled(b);self.bar.setRange(0,0 if b else 1)
        for g in self.findChildren(QGroupBox):g.setEnabled(not b)
    def start(self):
        if self.run_busy:return
        cfg=self.cfg()
        if not cfg['db']:QMessageBox.information(self,'Xác thực CCHN','Hãy chọn file dữ liệu (.db) trước.');return
        if not cfg['staff_file']:QMessageBox.information(self,'Xác thực CCHN','Hãy chọn file danh mục nhân viên.');return
        if not cfg['gphd'] and not cfg['snapshot']:QMessageBox.information(self,'Xác thực CCHN','Hãy nhập số giấy phép bệnh viện (GPHĐ), hoặc chọn 1 bản lưu danh sách nhân sự.');return
        CV._CANCEL.clear();CV._ACTIVE[0]=None;reset_file_logging();self.open_btn.setEnabled(False);self.result=None
        self.log_view.appendPlainText('-'*70);self.set_busy(True)
        self.task=Task(lambda:CV.run(cfg));self.task.done.connect(self.finished);self.task.start()
    def stop(self):
        if not self.run_busy:return
        CV._CANCEL.set();CV.log('⏹ Đang dừng — chờ bước hiện tại kết thúc (không ghi file kết quả)...')
        con=CV._ACTIVE[0]
        if con is not None:
            try:con.interrupt()
            except Exception:pass
    def finished(self,res,err):
        self.set_busy(False)
        if err is not None:
            if CV.cancelled():CV.log('ĐÃ HỦY.');return
            if isinstance(err,ValueError):
                CV.log(f'LỖI cấu hình: {err}');QMessageBox.warning(self,'Xác thực CCHN',str(err))
            else:
                CV.log('LỖI:\n'+getattr(err,'_tb',str(err)));QMessageBox.critical(self,'Xác thực CCHN',f'Có lỗi: {err}\n\nXem chi tiết trong khung log.')
            return
        if res is None:CV.log('ĐÃ HỦY — không ghi file kết quả.');return
        self.result=res;fs=res.get('files') or []
        self.refresh_snaps(select=res.get('snapshot') or None)   # bản lưu mới tự tạo (nếu có) được chọn sẵn cho lần sau
        if fs:self.last_out=str(Path(fs[0]).parent);self.open_btn.setEnabled(True)
        QMessageBox.information(self,'Xác thực CCHN',f"Hoàn tất — {res.get('rows',0):,} dòng cần rà soát.\n\n"+'\n'.join(fs[:6])+(f'\n... (+{len(fs)-6} file)' if len(fs)>6 else '')+f"\n\nLog: {res.get('log','')}")
    def open_dir(self):
        d=getattr(self,'last_out','')
        if d:QDesktopServices.openUrl(QUrl.fromLocalFile(d))

def main():
    app=QApplication(sys.argv);app.setStyle('Fusion');w=Win();w.show()
    # cho phép: python cchn_gui.py file.db
    if len(sys.argv)>1 and Path(sys.argv[1]).exists():w.load_source(sys.argv[1])
    return app.exec()
if __name__=='__main__':
    try:sys.exit(main())
    except Exception:
        tb=traceback.format_exc();print(tb)
        try:
            _d=(Path.home()/'Documents'/'CCHN') if getattr(sys,'frozen',False) else Path(__file__).resolve().parent
            _d.mkdir(parents=True,exist_ok=True);(_d/'cchn_gui_crash.txt').write_text(tb,encoding='utf-8')
        except Exception:pass
        if sys.platform.startswith('win'):input('Lỗi — nhấn Enter để đóng...')
        raise
