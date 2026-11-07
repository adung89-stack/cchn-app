"""medinet_lookup.py — Tra cứu chứng chỉ hành nghề y qua 2 cổng của Sở Y tế TP.HCM:
  1. thongtin.medinet.org.vn (NGUỒN CHÍNH kể từ 2026-10 — form ASPX/DotNetNuke cổ điển,
     có trạng thái: mỗi bước phải gửi lại đúng __VIEWSTATE/__EVENTVALIDATION lấy từ HTML
     của bước ngay trước đó, không phải giá trị cố định). Đã kiểm chứng ổn định ở 6 luồng
     song song, 0 lỗi, qua nhiều lượt test thực tế.
  2. tracuu.medinet.org.vn (CHỈ dùng BỔ SUNG khi (1) không xác nhận được — API JSON gọn,
     2 bước: tìm kiếm rồi lấy chi tiết theo UUID trả về trong kết quả tìm kiếm). Đã đo thực
     tế (xem test_web1_dong_thoi_10s.py): ngưỡng an toàn chỉ ~1 request/10 giây cho TOÀN BỘ
     hệ thống (không phải theo luồng/session — 2 request liên tiếp không chờ, dù cùng 1
     luồng, đã đủ gây lỗi/bị chặn) — gần như chắc chắn chặn theo IP. MẶC DÙ VẬY, mặc định
     module này vẫn tra web 1 SONG SONG/nhanh như trước (xem MedinetVerifyRule.
     web1_safe_pacing — mặc định False, lựa chọn CỦA NGƯỜI DÙNG ưu tiên tốc độ hơn an toàn
     tuyệt đối); bật web1_safe_pacing=True mới chuyển sang tra TUẦN TỰ, CÁCH NHAU ĐÚNG
     WEB1_FALLBACK_DELAY_SECONDS giữa 2 request, đúng ngưỡng an toàn đã đo. Dù chế độ nào,
     web 1 cũng chỉ được dùng cho SỐ ÍT mã mà web 2 không xác nhận được, và 1 lỗi kết nối là
     dừng hẳn web 1 cho cả lượt chạy (không phụ thuộc web1_safe_pacing).

(Trước 2026-10, vai trò 2 trang này ngược lại — tracuu là nguồn chính. Đã đổi sau khi đo
được ngưỡng thật của tracuu quá thấp để dùng làm nguồn chính cho khối lượng lớn; xem
GHI_CHU_DU_AN.md mục lịch sử thay đổi.)

File này tách riêng khỏi data.py/ui.py, không import gì từ 2 file đó, để dễ sửa/thêm
nguồn tra cứu khác sau này. Chỉ phụ thuộc requests + beautifulsoup4 (đã có sẵn trong môi
trường chạy ứng dụng).

SỬ DỤNG CÓ TRÁCH NHIỆM: cả 2 trang đều LÀ CỔNG TRA CỨU CÔNG KHAI (không cần đăng nhập),
nhưng thongtin.medinet.org.vn khai báo robots.txt không cho phép truy cập tự động — không
có rào cản kỹ thuật nào chặn việc gọi trực tiếp bằng HTTP request thông thường như module
này làm, nhưng đó là tín hiệu rõ ràng từ chủ trang là không muốn bị crawl hàng loạt. Việc
dùng trang này làm nguồn chính (xem trên) là lựa chọn có cân nhắc: đây là cổng tra cứu
công khai đúng mục đích xác thực CCHN từng mã (không phải crawl toàn bộ danh bạ), và mỗi
mã vẫn chỉ được tra ĐÚNG 1 LẦN mỗi lượt chạy nhờ cache. Vì vậy module này: (a) có độ trễ
giữa các lượt tra cứu khác nhau (SLEEP_BETWEEN_REQUESTS), (b) qua LookupCache chỉ tra 1 mã
CCHN đúng 1 lần trong 1 lượt chạy dù mã đó lặp lại nhiều lần trong dữ liệu, và (c) không
nhằm để quét toàn bộ danh bạ — chỉ để xác thực đúng những mã THỰC SỰ xuất hiện trong dữ
liệu đang xử lý.
"""
from __future__ import annotations
import re,time,threading,json,ssl,os
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
from dataclasses import dataclass,field
from datetime import datetime,time as dtime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    pd=None
try:
    import requests
except ImportError:
    requests=None
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup=None

USER_AGENT='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
SLEEP_BETWEEN_REQUESTS=0.6  # độ trễ sau mỗi request — dùng cho web 2 (nguồn chính, 6 luồng
                            # song song, đã kiểm chứng ổn định ở mức này — xem calibrate_workers.py).
REQUEST_TIMEOUT=15
MAX_LOOKUP_WORKERS=6  # số luồng tra song song cho web 2 (nguồn chính) — xem
                       # _pipeline_fetch_web2_then_web1. Từng thử tăng mức này và bị lỗi/crash
                       # tăng rõ rệt — xem calibrate_workers.py. KHÔNG tăng số này lên mà không
                       # dò lại bằng calibrate_workers.py trước. Cũng là số luồng web 1 (bổ
                       # sung) dùng khi MedinetVerifyRule.web1_safe_pacing=False (MẶC ĐỊNH —
                       # ưu tiên tốc độ, tốc độ cũ trước khi đo ngưỡng thật); khi
                       # web1_safe_pacing=True, web 1 dùng WEB1_FALLBACK_WORKERS=1 thay vì số
                       # này — xem _pipeline_fetch_web2_then_web1.

# web 1 (tracuu.medinet.org.vn) — CHỈ dùng bổ sung cho số ít mã web 2 không xác nhận được. Đo
# thực tế (test_web1_dong_thoi_10s.py, 2026-10) cho thấy ngưỡng an toàn chỉ ~1 request/10 giây
# cho TOÀN BỘ hệ thống (không phải theo luồng — 2 request liên tiếp không chờ, dù cùng 1
# luồng/session, đã đủ gây lỗi/bị chặn ở ngay đợt thứ 2). 2 hằng số dưới đây CHỈ được dùng khi
# MedinetVerifyRule.web1_safe_pacing=True (người dùng chủ động bật, KHÔNG phải mặc định — mặc
# định vẫn dùng MAX_LOOKUP_WORKERS/SLEEP_BETWEEN_REQUESTS như web 2 để ưu tiên tốc độ, chấp
# nhận rủi ro dễ bị dừng sớm hơn nếu có nhiều mã cần rà qua web 1):
WEB1_FALLBACK_WORKERS=1
WEB1_FALLBACK_DELAY_SECONDS=10.0

# --- Cache tra cứu ra đĩa (giữa các lần chạy khác nhau) ---------------------------------
# Cache trong bộ nhớ (LookupCache/records1/records2 của 1 lượt chạy) chỉ sống trong 1 tiến
# trình — mỗi lần mở lại app phải tra mạng lại từ đầu. Cache ra đĩa này giúp nhiều lần chạy
# GẦN NHAU (cùng 1 buổi làm việc, xử lý nhiều file .db liên tiếp, hoặc chạy lại trong 1-2 ngày
# kế tiếp) không phải tra lại các mã đã biết — giảm trực tiếp số request thật sự cần bắn lên
# cổng, bất kể ngưỡng an toàn kỹ thuật là bao nhiêu. TTL dài (xem CACHE_TTL_SECONDS) vì CCHN
# của 1 người hiếm khi đổi trong vài ngày; chỉ áp dụng cho bản ghi found=True (xem
# save_disk_cached) nên rủi ro dùng nhầm dữ liệu đã lỗi thời là rất thấp so với lợi ích giảm
# tải lên cổng tra cứu.
# Cache nằm trong thư mục con 'medinet' của thư mục data (cạnh file nguồn .db — xem
# data.gdp_workdir); app.py gọi set_cache_dir(source.workdir) mỗi khi mở nguồn. Chưa mở nguồn
# thì dùng <thư mục chương trình>/data/medinet. File cache cũ ở thư mục home (bản trước) được
# tự chuyển vào đây lần đầu dùng.
_LEGACY_CACHE_FILE=Path.home()/'.general_data_processor_medinet_cache.json'
_cache_dir=Path(__file__).resolve().parent/'data'/'medinet'
def set_cache_dir(workdir):
    global _cache_dir
    try:_cache_dir=Path(workdir)/'medinet'
    except Exception:pass
def cache_file():
    d=_cache_dir
    f=d/'medinet_cache.json'
    try:
        d.mkdir(parents=True,exist_ok=True)
        if not f.exists() and _LEGACY_CACHE_FILE.exists():
            try:_LEGACY_CACHE_FILE.replace(f)
            except Exception:
                import shutil;shutil.copy2(_LEGACY_CACHE_FILE,f)
    except Exception:pass
    return f
# TTL 30 ngày (trước đây 48 giờ): chỉ cache bản ghi found=True, CCHN/nơi công tác hiếm khi đổi
# trong vài tuần; đặt GDP_MEDINET_CACHE_DAYS để đổi.
try:CACHE_TTL_SECONDS=float(os.environ.get('GDP_MEDINET_CACHE_DAYS') or 30)*86400
except Exception:CACHE_TTL_SECONDS=30*86400

TRACUU_BASE='https://tracuu.medinet.org.vn'
THONGTIN_BASE='https://thongtin.medinet.org.vn'
THONGTIN_PAGE_PATH='/Ch%E1%BB%A9ng-ch%E1%BB%89-h%C3%A0nh-ngh%E1%BB%81'

_SSL_OP_LEGACY_SERVER_CONNECT=0x4  # tên hằng số ssl.OP_LEGACY_SERVER_CONNECT chỉ có từ Python
# 3.12 — dùng thẳng giá trị số (theo OpenSSL) để chạy được trên các bản Python cũ hơn.
def _build_legacy_ssl_context():
    """SSLContext 'nới lỏng' để bắt tay được với server cũ (IIS/ASP.NET đời cũ, như
    thongtin.medinet.org.vn) — OpenSSL bản mới (3.x, mặc định trong Python hiện đại) đặt mức
    bảo mật TLS mặc định CAO HƠN nhiều server cũ hỗ trợ (bộ cipher yếu hơn, hoặc thiếu hẳn TLS
    1.2 đầy đủ), khiến kết nối bị đứt giữa chừng ngay từ bước bắt tay dù server vẫn hoạt động
    bình thường (trình duyệt vẫn vào được vì trình duyệt tự hạ cấp, còn OpenSSL của Python thì
    không) — hiện ra dưới dạng SSLEOFError ('EOF occurred in violation of protocol'), TÁI HIỆN
    NGAY CẢ VỚI 1 REQUEST ĐƠN LẺ, không cần nhiều luồng mới gây ra. Hạ SECLEVEL xuống 1 (chấp
    nhận cipher yếu hơn mức mặc định) + cho phép tối thiểu từ TLS 1.0 + bật lại "legacy server
    connect" — đây là tổ hợp sửa phổ biến nhất cho đúng dạng lỗi này khi kết nối tới các IIS
    đời cũ."""
    ctx=ssl.create_default_context()
    try:
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
    except ssl.SSLError:
        pass  # 1 số bản OpenSSL không hỗ trợ cú pháp SECLEVEL — bỏ qua, vẫn còn 2 tuỳ chỉnh dưới
    try:
        ctx.minimum_version=ssl.TLSVersion.TLSv1
    except (AttributeError,ValueError):
        pass  # bản Python/OpenSSL quá cũ hoặc quá mới không cho hạ mốc này — bỏ qua, không chặn
    ctx.options|=_SSL_OP_LEGACY_SERVER_CONNECT
    return ctx

if requests is not None:
    from requests.adapters import HTTPAdapter as _HTTPAdapter
    class _LegacySSLAdapter(_HTTPAdapter):
        """HTTPAdapter dùng SSLContext nới lỏng (xem _build_legacy_ssl_context) để bắt tay
        TLS được với server cũ."""
        def init_poolmanager(self,*args,**kwargs):
            kwargs['ssl_context']=_build_legacy_ssl_context()
            return super().init_poolmanager(*args,**kwargs)
        def proxy_manager_for(self,*args,**kwargs):
            kwargs['ssl_context']=_build_legacy_ssl_context()
            return super().proxy_manager_for(*args,**kwargs)
else:
    _LegacySSLAdapter=None

def _new_session():
    """Tạo 1 session mới, đã gắn User-Agent + _LegacySSLAdapter (xem docstring lớp đó) —
    gắn cho MỌI session dùng chung cho cả web1 lẫn web2 vì vô hại với server hiện đại (chỉ
    nới lỏng 1 tuỳ chọn TLS phía client), trong khi lại cần thiết để tránh SSLEOFError với
    server cũ của web2 — đỡ phải phân biệt 2 loại session khác nhau."""
    if requests is None:return None
    sess=requests.Session()
    sess.headers.update({'User-Agent':USER_AGENT})
    if _LegacySSLAdapter is not None:
        sess.mount('https://',_LegacySSLAdapter())
    return sess

class _SessionPool:
    """1 session HTTP riêng cho MỖI LUỒNG (threading.local) — mồi cookie tracuu 1 LẦN khi
    luồng đó tạo request đầu tiên, không tạo session mới cho MỖI mã (đỡ tốn 1 GET mồi/mã),
    NHƯNG cũng không dồn hết traffic của cả đợt (có thể vài trăm/vài nghìn mã) vào ĐÚNG 1
    session/cookie duy nhất — tối đa số luồng của pool đang dùng (MAX_LOOKUP_WORKERS cho web2;
    đúng 1, WEB1_FALLBACK_WORKERS, cho web1) cookie khác nhau hoạt động song song, mỗi luồng
    1 cookie riêng. Quan sát thực tế (trước 2026-10, khi web1 còn là nguồn chính, tra nhiều
    luồng): dùng 1 session DUY NHẤT cho toàn bộ đợt vài trăm mã khiến web1 trả HTTP 403
    Forbidden gần như toàn bộ sau một thời gian ngắn — nhiều khả năng do hệ thống chống spam
    của server khoá theo session/cookie sau khi thấy nó gửi quá nhiều request liên tục, nên
    cách này cân bằng lại: vẫn tiết kiệm request mồi, nhưng trải traffic ra nhiều cookie như
    nhiều luồng duyệt web độc lập."""
    def __init__(self,prime=False):
        self._local=threading.local()
        self._prime=prime
    def get(self):
        sess=getattr(self._local,'sess',None)
        if sess is None:
            sess=_new_session()
            if sess and self._prime:
                _prime_tracuu_session(sess)
            self._local.sess=sess
        return sess
    def refresh(self):
        """Bỏ session của LUỒNG ĐANG GỌI, buộc tạo cookie MỚI ở lần get() kế tiếp — dùng khi
        nghi ngờ cookie hiện tại đã bị server khoá (vd sau khi gặp HTTP 403)."""
        self._local.sess=None

RETRY_403_DELAY=5.0
CIRCUIT_BREAKER_THRESHOLD=8  # mặc định (dùng cho web2 — nguồn chính — xem web2_breaker/
# web1_breaker ở _pipeline_fetch_web2_then_web1/_fetch_parallel: web1 dùng ngưỡng 1, xem
# WEB1_SWITCH_THRESHOLD).
CIRCUIT_BREAKER_COOLDOWN=90.0  # giây tạm dừng gửi request tới site đó sau khi "mở" circuit
# (mặc định, dùng cho web2 — web1 dùng cooldown gần như vô hạn, xem WEB1_SWITCH_COOLDOWN).
WEB1_SWITCH_THRESHOLD=1     # web1 (bổ sung) CHỈ CẦN 1 lỗi liên quan kết nối là dừng hẳn việc
# gọi thêm web1 cho phần còn lại của lượt chạy — vì ngưỡng thật của web1 đã đo được quá thấp
# (~1 request/10s cho toàn hệ thống, xem test_web1_dong_thoi_10s.py), nên hễ lỗi 1 lần là coi
# như đang bị chặn, khỏi phí thêm request — các mã không rà được qua web1 giữ nguyên trạng
# thái theo dữ liệu web2 đã có (không xác nhận được, không phải lỗi mất toàn bộ dữ liệu, vì
# web2 là nguồn chính đã tra trước).
WEB1_SWITCH_COOLDOWN=24*3600.0  # coi như "không gọi lại web1 nữa" trong suốt lượt chạy hiện
# tại (24 giờ dài hơn bất kỳ lượt chạy thực tế nào) — muốn thử lại web1 thì chạy 1 lượt MỚI.
def _looks_like_blocked(record):
    """True khi record.error cho thấy đây là lỗi LIÊN QUAN KẾT NỐI (mất kết nối, SSL,
    timeout, hay bị từ chối với mã trạng thái như 403) — record1.error CHỈ được set bởi các
    exception mạng/HTTP trong _tracuu_search/_tracuu_detail/_thongtin_get_state/_thongtin_post
    (xem lookup_tracuu/lookup_thongtin), không bao giờ do lỗi parse dữ liệu — nên ĐÃ CÓ error
    nghĩa là ĐÃ LÀ lỗi kết nối, không cần lọc thêm theo mã trạng thái cụ thể."""
    return bool(record and record.error)

class _CircuitBreaker:
    """Theo dõi số lần gặp lỗi kết nối LIÊN TIẾP GẦN ĐÂY trên 1 trang — DÙNG CHUNG cho mọi
    luồng trong 1 lượt chạy hàng loạt (khác với _SessionPool vốn tách riêng theo luồng). Vượt
    ngưỡng (threshold) lỗi liên tiếp thì coi như trang đang bị chặn/không dùng được, TẠM DỪNG
    (hoặc với web1 — chuyển HẲN, xem WEB1_SWITCH_COOLDOWN) việc gửi thêm request tới trang đó
    trong `cooldown` giây: các mã còn lại trong lúc đó nhận lỗi NGAY, không tốn network/thời
    gian chờ — tránh lặp lại tình trạng "chạy rất lâu mà không có kết quả gì" nếu bị chặn giữa
    chừng (không chỉ lúc mới bắt đầu — xem thêm preflight_check, vốn chỉ bắt được kiểu bị chặn
    NGAY TỪ ĐẦU, không bắt được kiểu bị chặn sau khi đã chạy được 1 lúc)."""
    def __init__(self,threshold=CIRCUIT_BREAKER_THRESHOLD,cooldown=CIRCUIT_BREAKER_COOLDOWN):
        self._threshold=threshold
        self._cooldown=cooldown
        self._lock=threading.Lock()
        self._consecutive_errors=0
        self._open_until=0.0
    def blocked(self):
        with self._lock:
            return time.time()<self._open_until
    def record_success(self):
        with self._lock:
            self._consecutive_errors=0
    def record_error(self):
        """Trả về True đúng 1 LẦN — thời điểm circuit VỪA chuyển sang trạng thái tạm dừng —
        để bên gọi biết mà log đúng 1 dòng thông báo, không log lặp lại mỗi mã.
        LƯU Ý (lỗi đã sửa): với threshold=1 (web1), nhiều luồng có thể cùng đang gọi mạng DỞ
        TRƯỚC KHI breaker kịp mở (đều đã qua được .blocked() lúc bắt đầu, vì lúc đó còn chưa
        có lỗi nào) — nên vài luồng có thể báo lỗi gần như CÙNG LÚC. Bản trước đây reset
        _consecutive_errors về 0 ngay khi mở, nên lỗi tiếp theo (từ luồng khác) lại tính từ 0,
        LẠI vượt threshold=1 ngay lập tức và trả True LẦN NỮA — gây log lặp lại nhiều dòng
        "chuyển sang dùng nguồn khác" cho CÙNG 1 lần mở breaker. Giờ kiểm tra "đã đang mở hay
        chưa" TRƯỚC khi tăng bộ đếm — nếu đã mở rồi (còn trong cooldown) thì coi như lỗi này
        là lỗi của 1 luồng đã lỡ gọi mạng từ trước, không tính là 1 lần mở MỚI, không log lại."""
        with self._lock:
            now=time.time()
            if now<self._open_until:
                return False  # đã mở từ trước (do luồng khác) — không mở lại, không log lại
            self._consecutive_errors+=1
            if self._consecutive_errors>=self._threshold:
                self._open_until=now+self._cooldown
                self._consecutive_errors=0
                return True
            return False

def _fetch_with_403_recovery(pool,fetch_fn,code,breaker=None,log=None,site_label='trang này',retry_403=True):
    """Gọi fetch_fn(session,code) bằng session của luồng hiện tại (SessionPool). Nếu breaker
    đang "mở" (đang trong thời gian tạm dừng/đã chuyển hẳn — xem _CircuitBreaker), trả lỗi
    NGAY không gọi mạng — KHÔNG có luồng nào khác còn được thử thêm nữa sau khi breaker đã mở,
    vì breaker dùng CHUNG cho mọi luồng (kiểm tra .blocked() ngay đầu hàm, trước khi làm bất
    cứ việc gì khác). retry_403=True (mặc định — dùng cho web2, nguồn chính): nếu lỗi là HTTP
    403 cụ thể (dấu hiệu cookie/session bị khoá do gửi quá nhiều request), ĐỔI SANG COOKIE MỚI
    rồi thử lại ĐÚNG 1 LẦN sau khi chờ RETRY_403_DELAY — hợp lý cho trang đã chứng minh ổn
    định, 1 lần 403 thoáng qua chưa chắc là bị chặn diện rộng. retry_403=False (dùng cho web1
    — bổ sung, xem lời gọi ở _pipeline_fetch_web2_then_web1): KHÔNG thử lại, KHÔNG chờ đợi gì
    cả — ĐÚNG 1 LẦN LỖI (bất kỳ lỗi kết nối nào) là báo breaker NGAY LẬP TỨC để dừng hẳn việc
    gọi thêm web1 cho phần còn lại của lượt chạy, theo đúng ngưỡng thật đã đo: web1 quá mong
    manh nên không đáng bỏ thời gian ra thử lại."""
    if breaker is not None and breaker.blocked():
        return PractitionerRecord(error=f'Đã dừng gọi {site_label} cho phần còn lại của lượt chạy này (vừa gặp lỗi kết nối trước đó).')
    sess=pool.get()
    rec=fetch_fn(sess,code)
    if retry_403 and rec.error and '403' in rec.error:
        pool.refresh()
        time.sleep(RETRY_403_DELAY)
        sess=pool.get()
        rec=fetch_fn(sess,code)
    if breaker is not None:
        if _looks_like_blocked(rec):
            if breaker.record_error() and log:
                log(f'⚠ {site_label}: gặp lỗi kết nối ({rec.error}) — dừng gọi thêm {site_label} cho phần còn lại của lượt chạy này.')
        else:
            breaker.record_success()
    return rec

def preflight_check():
    """Kiểm tra NHANH (1 request cho web1 + 1 cho web2, vài giây) xem 2 cổng có gọi được
    ngay lúc này không — gọi TRƯỚC KHI bắt đầu 1 đợt tra hàng loạt (có thể hàng trăm/hàng
    nghìn mã), để biết ngay là đang bị chặn/mất kết nối thay vì phải đợi chạy hết cả đợt
    (mỗi mã còn có thêm 1 lần thử lại riêng) rồi mới phát hiện ra TOÀN BỘ đều lỗi giống nhau.
    Không cần mã CCHN thật — chỉ cần request không bị NÉM EXCEPTION/lỗi HTTP là coi như trang
    còn gọi được (nội dung tìm thấy hay không không quan trọng ở bước này).
    Trả về dict {'web1':(ok,err_text),'web2':(ok,err_text)}."""
    result={'web1':(True,''),'web2':(True,'')}
    if requests is None:
        return {'web1':(False,'Thiếu thư viện requests/beautifulsoup4'),
                'web2':(False,'Thiếu thư viện requests/beautifulsoup4')}
    try:
        sess=_new_session()
        _prime_tracuu_session(sess)
        _tracuu_search(sess,'0000000/HCM-CCHN')
    except Exception as e:
        result['web1']=(False,str(e))
    try:
        sess=_new_session()
        _thongtin_get_state(sess)
    except Exception as e:
        result['web2']=(False,str(e))
    return result

RETRY_TRANSIENT_DELAY=1.0
def _retry_transient(fn,*args,**kwargs):
    """Gọi fn(*args,**kwargs), THỬ LẠI ĐÚNG 1 LẦN nếu gặp lỗi mạng THOÁNG QUA (mất kết nối
    giữa chừng, SSL bị ngắt đột ngột, timeout) — đây là các lỗi hay gặp khi chạy nhiều luồng
    dồn dập hoặc mạng chập chờn, và thường chỉ cần thử lại là qua (khác với lỗi HTTP có mã
    trạng thái rõ ràng như 403/404, hay lỗi parse dữ liệu — những lỗi đó KHÔNG thử lại vì
    thử lại cũng sẽ lỗi y hệt, không nên tốn thêm 1 request vô ích). Không dùng thử lại nhiều
    lần/backoff luỹ thừa vì đây là tra cứu đơn lẻ trong 1 pipeline đã có SLEEP_BETWEEN_REQUESTS
    và giới hạn số luồng riêng — mục tiêu là vượt qua sự cố thoáng qua, không phải chịu tải
    lỗi kéo dài của server (nếu server thực sự down/chặn, để lỗi nổi lên ở lượt thử lại)."""
    if requests is None:
        return fn(*args,**kwargs)
    try:
        return fn(*args,**kwargs)
    except (requests.exceptions.SSLError,requests.exceptions.ConnectionError,requests.exceptions.Timeout):
        time.sleep(RETRY_TRANSIENT_DELAY)
        return fn(*args,**kwargs)

# Thứ trong tuần theo cách viết của 2 trang -> datetime.weekday() (Thứ 2=0 ... Chủ nhật=6)
_WEEKDAY_MAP={'T2':0,'T3':1,'T4':2,'T5':3,'T6':4,'T7':5,'CN':6}
_WEEKDAY_LABEL={v:k for k,v in _WEEKDAY_MAP.items()}

@dataclass
class Workplace:
    """Một dòng trong bảng "Nơi công tác" — 1 người có thể có nhiều Workplace (làm nhiều nơi)."""
    gphd:str=''             # số giấy phép hoạt động của CƠ SỞ (không phải của người), vd "01366/HCM-GPHĐ"
    facility_name:str=''
    position:str=''         # Vị trí — site 2 không tách riêng field này, luôn để trống
    khoa:str=''             # Khoa — có thể rỗng (không phải lỗi, một số vị trí không thuộc khoa nào)
    ngay_lam:str=''         # Ngày làm việc, giữ nguyên dạng chữ "dd/mm/yyyy" như trang trả về
    ngay_nghi:str=''        # Ngày nghỉ việc — rỗng nghĩa là còn đang làm (không có ngày kết thúc)
    thoi_gian:str=''        # "T2: 7:00 -> 16:30; T3: 7:00 -> 16:30; ..."
    address:str=''

@dataclass
class PractitionerRecord:
    found:bool=False
    source:str=''           # 'tracuu' | 'thongtin' | '' (rỗng nếu found=False và không tra được từ trang nào)
    name:str=''
    cchn:str=''
    status:str=''           # "Đang hoạt động" / "Hoạt động" / "Đình chỉ"...
    workplaces:list=field(default_factory=list)
    error:str=''            # lỗi KỸ THUẬT (mất mạng, trang đổi cấu trúc...) — khác hẳn found=False
                             # (nghĩa là tra được, nhưng trang xác nhận không có mã này)
    scope:str=''            # "Phạm vi chuyên môn" đăng ký với SYT (web 1: danh sách theo cơ sở hoặc trang chi tiết)
    position:str=''         # Vị trí hành nghề (danh sách theo cơ sở)
    nationality:str=''      # Quốc tịch (chỉ để xuất Excel bản lưu)

@lru_cache(maxsize=None)
def _normalize_code(code):
    return re.sub(r'\s+','',str(code or '')).upper()
_BLANK_DOCTOR_CODE_TOKENS={
    # Các giá trị "trống" thường gặp khi đọc .db/Excel/CSV mà KHÔNG phải là 1 mã CCHN thật
    # (không tồn tại dạng mã này trên cổng tra cứu) — coi như ô trống, không phải chuỗi "NAN"
    # hay "#N/A" theo nghĩa đen.
    '','NAN','NONE','NULL','<NA>','#N/A','#NA','N/A','NA','#VALUE!','#REF!','#NULL!','#DIV/0!',
}
def _normalize_doctor_code(raw):
    """Chuẩn hoá mã MA_BAC_SI/MA_BSI_THUC_HIEN lấy thẳng từ .db — coi None, NaN (số thực),
    chuỗi rỗng/toàn khoảng trắng, HOẶC các dấu hiệu lỗi/thiếu dữ liệu thường gặp khi đọc
    Excel/CSV (nan, #N/A, #VALUE!, NULL, None...) là KHÔNG CÓ MÃ — trả về '' để cả pipeline tự
    bỏ qua dòng đó (không tra cứu mạng, không thống kê vào KET_QUA lẫn sheet chi tiết), vì
    không có mã CCHN dạng như vậy trên cổng tra cứu để mà đối chiếu."""
    try:
        if pd is not None and pd.isna(raw):return ''
    except (TypeError,ValueError):
        pass  # pd.isna() có thể ném lỗi với 1 số kiểu lạ (vd mảng) — bỏ qua, xử lý tiếp như chuỗi
    if raw is None:return ''
    s=re.sub(r'\s+','',str(raw)).upper()
    return '' if s in _BLANK_DOCTOR_CODE_TOKENS else s
def split_doctor_codes(raw):
    """Tách ô mã (vd '003199/HCM-GPHN;002455/HCM-GPHN') thành các mã ĐỘC LẬP (ngăn bởi ';' hoặc ','),
    mỗi mã chuẩn hoá bằng _normalize_doctor_code; bỏ mã trống/trùng, giữ thứ tự. Trả list[(mã_chuẩn_hoá, mã_thô)]."""
    try:
        if pd is not None and pd.isna(raw):return []
    except (TypeError,ValueError):
        pass
    if raw is None:return []
    out=[];seen=set()
    for piece in re.split(r'[;,]',str(raw)):
        c=_normalize_doctor_code(piece)
        if c and c not in seen:
            seen.add(c);out.append((c,piece.strip()))
    return out
def _clean_text(text):
    return re.sub(r'\s+',' ',str(text or '').replace('\xa0',' ')).strip()
@lru_cache(maxsize=None)
def parse_vn_date(text):
    text=_clean_text(text)
    if not text:return None
    try:return datetime.strptime(text,'%d/%m/%Y').date()
    except ValueError:return None
_VN_DAY_WORDS={'hai':0,'ba':1,'tư':2,'tu':2,'bốn':2,'năm':3,'sáu':4,'bảy':5,'bay':5,'chủ nhật':6,'cn':6}
@lru_cache(maxsize=None)
def parse_weekly_schedule(text):
    """'T2: 7:00 -> 16:30; T3: 7:00 -> 16:30; ...' -> {0:[(time,time)], 1:[...], ...}.
    Có thể có nhiều khung giờ cho cùng 1 thứ nếu chuỗi liệt kê thứ đó nhiều lần.
    Cũng nhận: ký hiệu 'Tcn' (web ghi 'Tcn' thay cho 'CN'), và kiểu chữ tự do mà một số cơ sở
    nhập, vd 'Từ Thứ hai đến Chủ nhật: từ 06 giờ 00 đến 21 giờ 00'."""
    schedule={}
    for part in _clean_text(text).split(';'):
        part=part.strip()
        m=re.match(r'^(T2|T3|T4|T5|T6|T7|CN|TCN)\s*:\s*(\d{1,2}):(\d{2})\s*->\s*(\d{1,2}):(\d{2})$',part,re.I)
        if not m:continue
        key=m.group(1).upper()
        day=_WEEKDAY_MAP['CN' if key=='TCN' else key]
        start=dtime(int(m.group(2))%24,int(m.group(3)))
        end=dtime(int(m.group(4))%24,int(m.group(5)))
        schedule.setdefault(day,[]).append((start,end))
    if schedule:return schedule
    t=_clean_text(text).lower()
    m=re.search(r'th(?:ứ|u)\s+(hai|ba|tư|tu|bốn|năm|sáu|bảy|bay)\s+đến\s+(?:th(?:ứ|u)\s+(hai|ba|tư|tu|bốn|năm|sáu|bảy|bay)|(chủ nhật|cn))\s*[:,]?\s*(?:từ\s*)?(\d{1,2})\s*(?:giờ|h|:)\s*(\d{0,2})\s*(?:đến|->|-)\s*(\d{1,2})\s*(?:giờ|h|:)\s*(\d{0,2})',t)
    if m:
        d1=_VN_DAY_WORDS.get(m.group(1));d2=_VN_DAY_WORDS.get(m.group(2) or m.group(3))
        if d1 is not None and d2 is not None:
            start=dtime(int(m.group(4))%24,int(m.group(5) or 0)%60)
            end=dtime(int(m.group(6))%24,int(m.group(7) or 0)%60)
            day=d1
            for _ in range(8):
                schedule.setdefault(day,[]).append((start,end))
                if day==d2:break
                day=(day+1)%7
    return schedule

# ---------------------------------------------------------------------------
# Lịch làm việc mở rộng: chỉ ghi thứ / "từ thứ này đến thứ kia" = làm NGUYÊN NGÀY đó; giờ lấy theo
# "Thời gian hoạt động" của cơ sở (xem parse_facility_hours / resolve_facility_hours).
# ---------------------------------------------------------------------------
_DAY_TOK=r'(?:t2|t3|t4|t5|t6|t7|cn|tcn)'
_FULL_DAY=(dtime(0,0),dtime(23,59))
def _day_idx(tok):
    tok=tok.lower()
    return _WEEKDAY_MAP['CN' if tok=='tcn' else tok.upper()]
def _norm_days_text(text):
    t=_clean_text(text).lower()
    t=re.sub(r'ch(?:ủ|u)\s*nh(?:ậ|a)t','cn',t)
    for w,n in (('hai','2'),('ba','3'),('tư','4'),('tu','4'),('bốn','4'),('năm','5'),('sáu','6'),('bảy','7'),('bay','7')):
        t=re.sub(r'th(?:ứ|u)\s+'+w+r'\b','t'+n,t)
    t=re.sub(r'th(?:ứ|u)\s*([2-7])',r't\1',t)
    return t
def _days_of(spec):
    """'t2 -> t6' / 't2 đến t7' / 't2' / 't2, t4' -> danh sách chỉ số thứ."""
    m=re.search(r'('+_DAY_TOK+r')\s*(?:->|-|–|đến|den|tới)\s*('+_DAY_TOK+')',spec)
    if m:
        d1,d2=_day_idx(m.group(1)),_day_idx(m.group(2))
        out=[d1]
        while out[-1]!=d2 and len(out)<8:out.append((out[-1]+1)%7)
        return out
    return list(dict.fromkeys(_day_idx(x) for x in re.findall(r'\b('+_DAY_TOK+r')\b',spec)))
_TIME_RX=re.compile(r'(?<![\w:])(\d{1,2})\s*(?:giờ|gio|h|:)\s*(\d{0,2})')
def _times_of(rest):
    out=[]
    for m in _TIME_RX.finditer(rest):
        h=int(m.group(1));mi=int(m.group(2) or 0)
        if h>24 or mi>59:continue
        out.append(dtime(h%24,mi))
    return out
def parse_schedule_ex(text):
    """Lịch làm việc -> {thứ: [(bắt đầu|None, kết thúc|None), ...]}. None = chưa có giờ => lấy theo giờ hoạt động của cơ sở:
       'T2: 7:00 -> 16:30'  -> (7:00,16:30)    'T2:' / 'T2' / 'T2 -> T6' -> (None,None) nguyên ngày
       'CN: 07:30'          -> (7:30,None)     (từ giờ đó đến hết giờ hoạt động)
    Chuỗi trống hoặc không nhận được thứ nào -> {} (giữ hành vi cũ: coi như không có lịch)."""
    t=_norm_days_text(text)
    out={}
    for part in re.split(r'[;\n]+',t):
        part=part.strip(' .,')
        if not part:continue
        m=re.match(r'^(?:từ\s+)?((?:'+_DAY_TOK+r'(?:\s*(?:->|-|–|đến|den|tới|,|và)\s*)?)+)\s*[:,]?\s*(.*)$',part)
        if not m:continue
        days=_days_of(m.group(1))
        if not days:continue
        times=_times_of(m.group(2))
        if len(times)>=2:win=(times[0],times[1])
        elif len(times)==1:win=(times[0],None)
        else:win=(None,None)
        for d in days:out.setdefault(d,[]).append(win)
    if out:return out
    old=parse_weekly_schedule(text)
    return {d:list(w) for d,w in old.items()}

def parse_facility_hours(text):
    """"Thời gian hoạt động" của cơ sở -> 'ALL' (toàn thời gian/24h) | {thứ: [(bắt đầu,kết thúc)]} | None (không ghi/không đọc được).
    Vd 'TỪ THỨ 2 ĐẾN THỨ 7: TỪ 07 GIỜ 30 ĐẾN 21 GIỜ; CHỦ NHẬT: TỪ 07 GIỜ 30 ĐẾN 18 GIỜ'."""
    raw=_clean_text(text)
    if not raw:return None
    low=raw.lower()
    if re.search(r'to(?:à|a)n\s*th(?:ờ|o)i\s*gian|24\s*/\s*24|24\s*(?:giờ|h)\s*/\s*(?:ngày|ngay)|c(?:ả|a)\s*ng(?:à|a)y\s*l(?:ẫ|a)n\s*(?:đ|d)(?:ê|e)m',low) and not _times_of(low):return 'ALL'
    t=_norm_days_text(raw)
    out={}
    for part in re.split(r'[;\n]+',t):
        part=part.strip()
        if not part:continue
        times=_times_of(part)
        if len(times)<2:continue
        _m0=_TIME_RX.search(part)
        head=part[:_m0.start()] if _m0 else part
        days=_days_of(head) or list(range(7))
        for d in days:out.setdefault(d,[]).append((times[0],times[1]))
    return out or None

_FAC_HOURS={}      # {gphd chuẩn hoá: 'ALL' | {thứ:[(s,e)]} | None}  (None = đã tra nhưng cơ sở không ghi giờ)
_FACILITY_META={}  # {gphd chuẩn hoá: {'name','hours', ...}}
def set_facility_hours(gphd,hours_text):
    _FAC_HOURS[_normalize_code(gphd)]=parse_facility_hours(hours_text)
    _FACILITY_META.setdefault(_normalize_code(gphd),{})['hours']=hours_text or ''
def facility_info(gphd):
    return dict(_FACILITY_META.get(_normalize_code(gphd)) or {})
def _fac_day_window(gphd,wd):
    """Khung giờ hoạt động của cơ sở trong thứ wd -> (bắt đầu, kết thúc) hoặc cả ngày nếu không biết."""
    h=_FAC_HOURS.get(_normalize_code(gphd))
    if h=='ALL' or not h:return _FULL_DAY
    ws=h.get(wd)
    if not ws:return _FULL_DAY
    return (min(a for a,_ in ws),max(b for _,b in ws))
def _resolve_window(win,gphd,wd):
    s,e=win
    fs,fe=_fac_day_window(gphd,wd)
    return (s if s is not None else fs, e if e is not None else fe)
def find_facility_workplace(record,gphd,check_dt=None):
    """Chọn Workplace phù hợp nhất để HIỂN THỊ (Ngày làm việc/Ngày nghỉ việc/Thời gian làm
    việc/Vị trí/Khoa) khi 1 người có thể có NHIỀU dòng "Nơi công tác" tại cùng 1 giấy phép
    qua các giai đoạn khác nhau (vd đổi ca, tái ký hợp đồng). Ưu tiên dòng có khoảng
    [ngày làm, ngày nghỉ] THỰC SỰ CHỨA check_dt; nếu không có dòng nào khớp đúng giai đoạn,
    vẫn trả về dòng ĐẦU TIÊN khớp giấy phép (để người dùng còn thấy thông tin mà so sánh,
    thay vì để trống hoàn toàn) — chỉ trả None khi không có dòng nào khớp giấy phép này."""
    if not record.found:return None
    gphd_norm=_normalize_code(gphd)
    matches=[w for w in record.workplaces if _normalize_code(w.gphd)==gphd_norm]
    if not matches:return None
    if check_dt:
        check_date=check_dt.date()
        for w in matches:
            start_d=parse_vn_date(w.ngay_lam)
            end_d=parse_vn_date(w.ngay_nghi)
            if start_d and check_date<start_d:continue
            if end_d and check_date>end_d:continue
            return w
    return matches[0]
def verify_at_facility(record,gphd,check_dt):
    """So khớp 1 PractitionerRecord với 1 giấy phép bệnh viện + 1 thời điểm cụ thể (ngày+giờ).
    Trả về:
      'x'   — có nơi công tác khớp đúng giấy phép, thời điểm nằm trong [ngày làm, ngày nghỉ]
              VÀ đúng khung giờ theo lịch trực của thứ đó.
      'xx'  — có nơi công tác khớp đúng giấy phép và trong khoảng ngày làm-nghỉ, nhưng SAI
              giờ/thứ (thời điểm nằm ngoài mọi khung giờ đã đăng ký cho thứ đó, kể cả khi
              thứ đó hoàn toàn không có ca nào).
      None  — không có nơi công tác nào khớp đúng giấy phép này trong đúng khoảng ngày —
              tức "không xác nhận được" hoàn toàn (không chỉ sai giờ mà còn sai chỗ/thời gian)."""
    status,_,_=verify_at_facility_detailed(record,gphd,check_dt)
    return {'x':'x','wrong_time':'xx','wrong_day':'xx'}.get(status)
def verify_at_facility_detailed(record,gphd,check_dt):
    """Như verify_at_facility, nhưng phân biệt rõ 2 kiểu SAI khác nhau (phục vụ 2 cột xuất
    riêng NGOAI_GIO/KHONG_DANG_KY). Trả về tuple (status, gio_str, thu_str):
      ('x','','')             — đúng hoàn toàn (đúng chỗ, đúng giai đoạn, đúng khung giờ).
      ('wrong_time', 'HH:MM','') — đúng chỗ + đúng giai đoạn ngày làm-nghỉ + THỨ này CÓ đăng
              ký ca trực, nhưng giờ cụ thể (check_dt) nằm ngoài mọi khung giờ đã đăng ký cho
              thứ đó. gio_str = giờ:phút của check_dt (không lấy ngày).
      ('wrong_day', '', 'T2'..'CN') — đúng chỗ + đúng giai đoạn, nhưng THỨ trong tuần của
              check_dt hoàn toàn KHÔNG có ca trực nào đăng ký (lịch trực không liệt kê thứ
              này). thu_str = thứ của check_dt theo đúng ký hiệu T2..T7/CN dùng trong lịch.
      ('no_match', '', '')    — không có nơi công tác nào khớp đúng giấy phép này trong đúng
              khoảng ngày làm-nghỉ, hoặc không tìm thấy người — không xác nhận được gì cả."""
    if not record.found:return ('no_match','','')
    gphd_norm=_normalize_code(gphd)
    check_date=check_dt.date()
    best=None
    for w in record.workplaces:
        if _normalize_code(w.gphd)!=gphd_norm:continue
        start_d=parse_vn_date(w.ngay_lam)
        end_d=parse_vn_date(w.ngay_nghi)
        if start_d and check_date<start_d:continue
        if end_d and check_date>end_d:continue
        schedule=parse_schedule_ex(w.thoi_gian)
        wd=check_dt.weekday()
        windows=[_resolve_window(x,gphd,wd) for x in schedule.get(wd,[])]
        t=check_dt.time()
        if any(s<=t<=e for s,e in windows):return ('x','','')
        best=('wrong_time',check_dt.strftime('%H:%M'),'') if windows else ('wrong_day','',_WEEKDAY_LABEL[wd])
    return best or ('no_match','','')

NOTE_FIXED={
    # "Ghi chú" cố định theo ĐÚNG nội dung người dùng đã chốt — không phụ thuộc web 1/web 2
    # tra được hay không (KHÔNG hiện lỗi kỹ thuật kiểu SSL/timeout/403 ở cột này nữa, đó chỉ
    # là chi tiết gỡ lỗi tạm thời trước đây, không phải nội dung người dùng muốn thấy):
    'sai_thu':'Có mã, phù hợp nơi công tác, đúng thời gian hành nghề, nhưng thứ không có trong lịch đăng ký.',
    'sai_gio':'Đúng thứ nhưng sai khung giờ.',
    'ngoai_bhyt':'Không có trong file nhân viên đăng ký với BHYT.',
    'loi_mang':'Hãy chạy lại; mã này chưa được kết luận.',
}
NOTE_KHONG_DK_SYT={
    # 4 khả năng con của "Không đăng ký với SYT" — xem _classify_no_match_reason.
    'not_found':'Không tìm thấy mã này trên cổng.',
    'no_facility':'Có mã nhưng không khớp nơi công tác.',
    'before_start':'Ngày y lệnh trước ngày bắt đầu đăng ký hành nghề.',
    'after_end':'Ngày y lệnh sau khi nghỉ việc.',
}
def _net_error(rec):
    """Bản ghi tra cứu bị lỗi KỸ THUẬT (kết nối/cổng chặn/thiếu thư viện) — khác 'không tìm thấy' và khác OFFLINE (mã không có trong bản lưu)."""
    e=str(getattr(rec,'error','') or '')
    return bool(e) and not e.startswith('OFFLINE')
def _classify_no_match_reason(disp_record,gphd,check_dt):
    """Chỉ gọi cho loại lỗi 'khong_dk_syt' (verify_at_facility_detailed đã trả 'no_match') —
    xác định rõ ĐÚNG 1 trong 4 lý do con đã thống nhất với người dùng, thay vì gộp chung
    thành 1 câu mơ hồ:
      'not_found'    — không tìm thấy mã này trên cổng tra cứu.
      'no_facility'  — có mã, nhưng KHÔNG có dòng "nơi công tác" nào khớp giấy phép bệnh viện
                       đã nhập, ở BẤT KỲ giai đoạn ngày nào.
      'before_start' — có dòng nơi công tác khớp giấy phép, nhưng ngày y lệnh (check_dt) nằm
                       TRƯỚC ngày bắt đầu làm việc đã đăng ký.
      'after_end'    — có dòng nơi công tác khớp giấy phép, nhưng ngày y lệnh nằm SAU ngày
                       nghỉ việc đã đăng ký.
    Nếu có nhiều dòng khớp giấy phép (nhiều giai đoạn hợp đồng khác nhau) mà TẤT CẢ đều lệch
    ngày, ưu tiên báo 'before_start' nếu có ít nhất 1 dòng y lệnh đến trước ngày bắt đầu —
    đây thường là trường hợp cần chú ý nhất (làm việc trước khi có đăng ký hành nghề)."""
    if not disp_record.found:return 'not_found'
    gphd_norm=_normalize_code(gphd)
    check_date=check_dt.date()
    matches=[w for w in disp_record.workplaces if _normalize_code(w.gphd)==gphd_norm]
    if not matches:return 'no_facility'
    before=after=False
    for w in matches:
        start_d=parse_vn_date(w.ngay_lam)
        end_d=parse_vn_date(w.ngay_nghi)
        if start_d and check_date<start_d:before=True
        elif end_d and check_date>end_d:after=True
    if before:return 'before_start'
    if after:return 'after_end'
    return 'no_facility'  # an toàn — logic verify_at_facility_detailed không nên rơi vào đây
def _build_note(cat,disp_record,gphd,check_dt):
    """Ghi chú cho cột "Ghi chú" ở KET_QUA — ĐÚNG nội dung đã chốt cho TỪNG loại lỗi (xem
    NOTE_FIXED/NOTE_KHONG_DK_SYT), tra theo `cat` (loại lỗi của DÒNG này — mỗi dòng chỉ
    thuộc 1 loại, xem REASON_LABELS), không còn phụ thuộc web 1/web 2 tra được hay không."""
    if cat in NOTE_FIXED:
        return NOTE_FIXED[cat]
    if cat=='khong_dk_syt':
        return NOTE_KHONG_DK_SYT.get(_classify_no_match_reason(disp_record,gphd,check_dt),'')
    return ''

# ---------------------------------------------------------------------------
# Site 1: tracuu.medinet.org.vn — API JSON, 2 bước (tìm kiếm -> chi tiết theo UUID)
# ---------------------------------------------------------------------------
def _prime_tracuu_session(session):
    """Trình duyệt thật luôn có cookie 'session=...' (JWT) từ lần tải trang chủ trước khi
    gọi API tìm kiếm — cookie này thấy rõ trong request thật của người dùng. Nếu gọi thẳng
    API mà chưa từng GET trang chủ, session/cookie có thể thiếu và server có thể trả về
    kết quả khác (rỗng/lỗi) dù cùng từ khoá. Gọi trước lượt tra cứu đầu tiên của mỗi session
    dùng chung (kiểm tra cookie đã có chưa để không GET lại mỗi lần tra)."""
    try:
        session.get(TRACUU_BASE+'/',timeout=REQUEST_TIMEOUT)
    except Exception:
        pass  # không chặn tra cứu nếu bước mồi thất bại — vẫn thử API như trước
def _tracuu_search(session,cchn):
    if 'session' not in session.cookies.get_dict():
        _prime_tracuu_session(session)
    resp=session.post(f'{TRACUU_BASE}/chungchihanhnghey',
        headers={'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest','Accept':'*/*','Accept-Language':'vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5',
                 'Origin':TRACUU_BASE,'Referer':TRACUU_BASE+'/'},
        json={'key_word':cchn,'checked':0,'page':1,'results_per_page':10},timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup=BeautifulSoup(resp.text,'html.parser')
    results=[]
    for card in soup.select('div.coso-hanhnghey[data-class]'):
        uuid=(card.get('data-class') or '').strip()
        h5=card.select_one('h5')
        name=''
        cchn_full=''
        if h5:
            name_node=h5.find(string=True,recursive=False)
            name=_clean_text(name_node)
            span=h5.select_one('span')
            if span:cchn_full=_clean_text(span.get_text())
        ps=card.select('p.card-text')
        status=''
        if ps:
            last=ps[-1]
            b=last.select_one('span')
            status=_clean_text(b.get_text() if b else last.get_text())
        results.append({'uuid':uuid,'name':name,'cchn':cchn_full,'status':status})
    return results
_SUB_DATE=r'(\d{1,2}/\d{1,2}/\d{4})'
def _parse_subrow(td,main):
    """Dòng phụ -> Workplace (kế thừa giấy phép/cơ sở/địa chỉ của dòng chính ngay trên nó)."""
    txt=_clean_text(td.get_text(' '))
    if not txt:return None
    sched=''
    for sp in td.select('span'):
        if sp.select_one('i.fa-calendar') or sp.select_one('i[class*="calendar"]'):
            sched=_clean_text(sp.get_text())
    m=re.search(r'Ng(?:à|a)y l(?:à|a)m:\s*'+_SUB_DATE,txt)
    start=m.group(1) if m else ''
    m=re.search(r'Ng(?:à|a)y ngh(?:ỉ|i):\s*'+_SUB_DATE,txt)
    end=m.group(1) if m else ''
    if not sched:
        # không có span lịch: lấy các đoạn dạng "T2: ..."/"CN: ..." trong ô
        parts=[x.strip() for x in txt.split(';') if re.match(r'^(?:T[2-7]|CN|TCN)\b',x.strip(),re.I)]
        sched='; '.join(parts)
    m=re.search(r'V(?:ị|i) tr(?:í|i):\s*([^;]*)',txt)
    position=m.group(1).strip() if m else main.position
    m=re.search(r'Chuy(?:ê|e)n khoa:\s*([^;]*)',txt)
    khoa=m.group(1).strip() if m else main.khoa
    if not (start or end or sched):return None
    return Workplace(gphd=main.gphd,facility_name=main.facility_name,position=position,khoa=khoa,
        ngay_lam=start,ngay_nghi=end,thoi_gian=sched,address=main.address)
def _dedupe_and_clip(wps):
    """Bỏ dòng trùng y hệt; dòng KHÔNG có ngày nghỉ mà phía sau (cùng giấy phép) còn giai đoạn bắt đầu muộn hơn thì chỉ kéo đến trước ngày bắt đầu đó
    (giai đoạn cũ không được coi là còn hiệu lực đến nay, tránh khớp nhầm giờ của giai đoạn cũ cho ngày thuộc giai đoạn mới)."""
    seen=set();out=[]
    for w in wps:
        k=(_normalize_code(w.gphd),w.ngay_lam,w.ngay_nghi,w.thoi_gian,w.position,w.khoa)
        if k in seen:continue
        seen.add(k);out.append(w)
    if len(out)<2:return out
    from datetime import timedelta
    for w in out:
        if w.ngay_nghi:continue
        sd=parse_vn_date(w.ngay_lam)
        if not sd:continue
        later=[parse_vn_date(o.ngay_lam) for o in out if o is not w and _normalize_code(o.gphd)==_normalize_code(w.gphd)]
        later=[d for d in later if d and d>sd]
        if later:
            w.ngay_nghi=(min(later)-timedelta(days=1)).strftime('%d/%m/%Y')
    seen=set();res=[]
    for w in out:   # cắt xong mới có thể trùng nhau
        k=(_normalize_code(w.gphd),w.ngay_lam,w.ngay_nghi,w.thoi_gian,w.position,w.khoa)
        if k not in seen:seen.add(k);res.append(w)
    return res
def _tracuu_detail(session,uuid):
    resp=session.post(f'{TRACUU_BASE}/chungchihanhngheydetail',
        headers={'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest','Accept':'*/*','Accept-Language':'vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5',
                 'Origin':TRACUU_BASE,'Referer':TRACUU_BASE+'/'},
        json={'check':uuid},timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup=BeautifulSoup(resp.text,'html.parser')
    def _field(label_for):
        lab=soup.select_one(f'label[for="{label_for}"]')
        if not lab:return ''
        fc=lab.find_next_sibling('p',class_='form-control')
        return _clean_text(fc.get_text()) if fc else ''
    name=_field('hovaten')
    cchn=_field('so_cchn')
    status=_field('active')
    scope=_field('phamvi_hanhnghe')
    workplaces=[]
    table=soup.select_one('table.table-bordered')
    if table:
        last_main=None
        for tr in table.select('tbody tr'):
            tds=tr.find_all('td')
            if len(tds)<6:
                # Dòng PHỤ (2 ô, ô thứ 2 gộp cột): các giai đoạn làm việc KHÁC của cùng 1 nơi công tác (Ngày làm/Ngày nghỉ/Vị trí/lịch ghi trong 1 ô)
                if len(tds)>=2 and last_main is not None:
                    sw=_parse_subrow(tds[-1],last_main)
                    if sw is not None:workplaces.append(sw)
                continue
            gphd=facility=position=khoa=''
            for d in tds[1].find_all('div'):
                txt=_clean_text(d.get_text())
                m=re.match(r'Số giấy phép:\s*([^\s]+)\s*-\s*(.*)',txt)
                if m:
                    gphd,facility=m.group(1).strip(),m.group(2).strip()
                    continue
                m=re.match(r'Vị trí:\s*(.*)',txt)
                if m:position=m.group(1).strip()
                m=re.match(r'Khoa:\s*(.*)',txt)
                if m:khoa=m.group(1).strip()
            last_main=Workplace(gphd=gphd,facility_name=facility,position=position,khoa=khoa,
                ngay_lam=_clean_text(tds[2].get_text()),ngay_nghi=_clean_text(tds[3].get_text()),
                thoi_gian=_clean_text(tds[4].get_text()),address=_clean_text(tds[5].get_text()))
            workplaces.append(last_main)
    workplaces=_dedupe_and_clip(workplaces)
    return name,cchn,status,workplaces,scope
def lookup_tracuu(session,cchn):
    try:
        results=_retry_transient(_tracuu_search,session,cchn)
    except Exception as e:
        return PractitionerRecord(error=f'Lỗi kết nối tracuu.medinet.org.vn: {e}')
    if not results:
        return PractitionerRecord(found=False,source='tracuu')
    target=_normalize_code(cchn)
    chosen=None
    for r in results:
        code_only=re.split(r'\s*-\s*\[',r['cchn'])[0].strip()
        if _normalize_code(code_only)==target:
            chosen=r
            break
    if not chosen and len(results)==1:
        # Trang tự làm "tìm kiếm tương đối" (fuzzy) — khi kết quả trả về CHỈ CÓ 1 dòng,
        # đó đã là lựa chọn của chính trang cho từ khoá này, không còn gì để so sánh nhầm
        # lẫn với dòng khác — chấp nhận luôn thay vì đòi khớp chuỗi tuyệt đối, vì phần Số
        # GPHN hiển thị có thể lệch định dạng nhỏ so với mã tìm (khoảng trắng, ký tự nối
        # khác nhau...) mà vẫn là ĐÚNG người cần tìm.
        chosen=results[0]
    if not chosen:
        return PractitionerRecord(found=False,source='tracuu')
    try:
        name,cchn_full,status,workplaces,scope=_retry_transient(_tracuu_detail,session,chosen['uuid'])
    except Exception as e:
        return PractitionerRecord(found=True,source='tracuu',name=chosen['name'],cchn=chosen['cchn'],
            status=chosen['status'],error=f'Lỗi lấy chi tiết tracuu.medinet.org.vn: {e}')
    return PractitionerRecord(found=True,source='tracuu',name=name or chosen['name'],
        cchn=cchn_full or chosen['cchn'],status=status or chosen['status'],workplaces=workplaces,scope=scope)


# ---------------------------------------------------------------------------
# Web 1 — danh sách nhân sự THEO CƠ SỞ (nguồn ưu tiên số 1 của mục F)
# Trang chi tiết cơ sở có nút "Tra cứu thông tin nhân sự tại cơ sở" — gọi
# POST /api/v1/public/nv_cskcb (10 người/trang) trả JSON đủ họ tên, số CCHN, vị trí, phạm vi,
# chuyên khoa, ngày làm/nghỉ, lịch làm việc. Một lần lấy cả cơ sở (vài chục trang) thay cho
# hàng trăm lượt tra lẻ từng mã. Chỉ những mã KHÔNG có trong danh sách (hoặc có nhưng chưa xác
# nhận được vì danh sách chỉ ghi giai đoạn hiện hành) mới tra lẻ — xem fill_records_chain.
# ---------------------------------------------------------------------------
WEB1_PER_CODE_DELAY=1.0     # tra lẻ web 1: đúng 1 mã mỗi giây (theo yêu cầu — web 1 không chịu được nhiều hơn)
FACILITY_PAGE_DELAY=0.3     # nghỉ giữa 2 trang danh sách nhân sự (đã thử 61 trang liên tục không bị chặn)
FACILITY_CACHE_TTL=3600.0
_FACILITY_CACHE={}          # {gphd chuẩn hoá: (thời điểm, {mã: PractitionerRecord}, tên cơ sở)}
_FACILITY_LOCK=threading.Lock()
_FACILITY_FAIL={}           # {gphd chuẩn hoá: (thời điểm, thông báo lỗi)} — không thử lại liên tục trong 5 phút
def _tracuu_json_headers():
    return {'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest','Accept':'*/*',
            'Accept-Language':'vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5','Origin':TRACUU_BASE,'Referer':TRACUU_BASE+'/'}
def _facility_find(session,gphd):
    """Tìm cơ sở theo số giấy phép hoạt động -> (uuid, tên cơ sở). Phải KHỚP ĐÚNG số GPHĐ (không
    nhận kết quả gần giống). Ném ValueError nếu không tìm thấy."""
    if 'session' not in session.cookies.get_dict():
        _prime_tracuu_session(session)
    resp=session.post(f'{TRACUU_BASE}/cosokhamchuabenh',headers=_tracuu_json_headers(),
        json={'key_word':gphd,'checked':0,'page':1,'results_per_page':10},timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup=BeautifulSoup(resp.text,'html.parser')
    target=_normalize_code(gphd)
    seen=[]
    for h5 in soup.select('h5[data-class]'):
        uuid=(h5.get('data-class') or '').strip()
        span=h5.select_one('span')
        code=re.split(r'\s*-\s*\[',_clean_text(span.get_text()))[0].strip() if span else ''
        name_node=h5.find(string=True,recursive=False)
        name=_clean_text(name_node) if name_node else ''
        seen.append(code)
        if uuid and _normalize_code(code)==target:
            return uuid,name
    raise ValueError(f'Không tìm thấy cơ sở có số giấy phép {gphd} trên cổng (kết quả: {", ".join(seen) or "trống"}).')
def _facility_macoso(session,uuid):
    resp=session.get(f'{TRACUU_BASE}/detail/{uuid}',timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup=BeautifulSoup(resp.text,'html.parser')
    el=soup.select_one('input[name="macoso"]')
    return el.get('value','') if el else ''
def _facility_detail(session,uuid):
    """Trang chi tiết cơ sở -> (macoso, {nhãn chuẩn: giá trị}) gồm 'hours' (Thời gian hoạt động), 'address', 'status', 'form', 'kind'."""
    resp=session.get(f'{TRACUU_BASE}/detail/{uuid}',timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup=BeautifulSoup(resp.text,'html.parser')
    el=soup.select_one('input[name="macoso"]')
    meta={}
    keys={'thời gian hoạt động':'hours','địa chỉ':'address','tình trạng':'status','hình thức tổ chức':'form','loại hình':'kind'}
    for sp in soup.select('span.fw-bold'):
        lab=_clean_text(sp.get_text()).rstrip(':').strip().lower()
        k=keys.get(lab)
        if not k or k in meta:continue
        nxt=sp.find_next_sibling('span')
        if nxt is not None:meta[k]=_clean_text(nxt.get_text())
    return (el.get('value','') if el else ''),meta
def _facility_page(session,uuid,macoso,page):
    resp=session.post(f'{TRACUU_BASE}/api/v1/public/nv_cskcb?page={page}&results_per_page=10',
        headers=_tracuu_json_headers(),json={'page':page,'cosokcb_id':uuid,'text':'','macoso':macoso},timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()
def facility_objects_to_records(objects,gphd,facility_name='',into=None):
    """Chuyển danh sách đối tượng JSON nhân sự -> {mã chuẩn hoá: PractitionerRecord(source='tracuu_cs')}.
    Workplace.gphd = đúng giấy phép người dùng nhập (để so khớp verify_at_facility_detailed). Cùng
    1 mã xuất hiện nhiều dòng (nhiều giai đoạn) -> gộp thành nhiều Workplace."""
    out=into if into is not None else {}
    for o in objects:
        raw=str(o.get('so_cchn') or '').strip()
        code=_normalize_code(raw)
        if not code:continue
        wp=Workplace(gphd=gphd,facility_name=facility_name,position=_clean_text(o.get('vitri_hanhnghe')),
            khoa=_clean_text(o.get('chuyenkhoa')),ngay_lam=_clean_text(o.get('ngaylam')),
            ngay_nghi=_clean_text(o.get('ngaynghi')),thoi_gian=_clean_text(o.get('thoigian_lamviec')))
        scope=_clean_text(o.get('phamvi_hanhnghe'))
        rec=out.get(code)
        if rec is None:
            out[code]=PractitionerRecord(found=True,source='tracuu_cs',name=_clean_text(o.get('hovaten')),cchn=raw,
                position=wp.position,scope=scope,workplaces=[wp])
        else:
            rec.workplaces.append(wp)
            if scope and scope not in rec.scope:rec.scope=(rec.scope+'; '+scope) if rec.scope else scope
    return out
_FACILITY_SNAPSHOT=''   # đường dẫn bản lưu danh sách nhân sự (syt_store) đang chọn; rỗng = lấy từ mạng như cũ
_OFFLINE=False
def set_offline(flag):
    """Offline hoàn toàn: mọi lệnh gọi mạng (danh sách theo cơ sở, tra lẻ web 1/web 2) bị bỏ qua; chỉ dùng bản lưu."""
    global _OFFLINE
    _OFFLINE=bool(flag)
def configure_from_rule(rule,log=None):
    """Gọi trước khi chạy mục E/F (dùng chung):
    - CÓ chọn bản lưu (đã quét hoặc nhập từ Excel): chạy hoàn toàn OFFLINE trên bản đó (không tra mạng), GPHĐ lấy theo bản lưu nếu chưa nhập.
    - KHÔNG chọn: dùng bản lưu của GPHĐ trong AUTO_DAYS ngày; chưa có thì tự quét 1 bản mới (lưu lại) rồi dùng; quét lỗi thì lấy từ mạng như cũ."""
    log=log or (lambda m:None)
    snap=str(getattr(rule,'facility_snapshot','') or '')
    gphd=str(getattr(rule,'facility_gphd','') or '').strip()
    try:
        import syt_store
    except Exception:
        syt_store=None
    if not snap and gphd and syt_store is not None:
        try:
            path,err,fresh=syt_store.ensure_recent(gphd,log=log)
            if path:snap=path
            elif err:log(f'⚠ Không tạo được bản lưu tự động ({err}) — lấy danh sách từ mạng.')
        except Exception as e:
            log(f'⚠ Không tạo được bản lưu tự động ({e}) — lấy danh sách từ mạng.')
        explicit=False
    else:
        explicit=bool(snap)
    set_facility_snapshot(snap)
    set_offline(explicit)
    if snap and not gphd and syt_store is not None:
        try:
            meta,_=syt_store.load_snapshot(snap)
            rule.facility_gphd=str(meta.get('gphd','') or '')
        except Exception:pass
    if snap and syt_store is not None:
        try:
            meta,_=syt_store.load_snapshot(snap)
            if 'hours' in meta:
                k=_normalize_code(meta.get('gphd',''))
                _FAC_HOURS[k]=parse_facility_hours(meta.get('hours',''))
                _FACILITY_META[k]=dict(meta,name=meta.get('facility_name',''))
        except Exception:pass
    return snap
_HOURS_TRIED=set()
def resolve_facility_hours(gphd,log=None):
    """Bảo đảm biết "Thời gian hoạt động" của cơ sở: chưa biết hoặc cơ sở không ghi => tra lại trang cơ sở trên web 1 (mỗi cơ sở 1 lần/lượt chạy, bỏ qua khi offline).
    Vẫn không có => coi như làm nguyên ngày (xem _fac_day_window)."""
    log=log or (lambda m:None)
    key=_normalize_code(gphd)
    if not key or _FAC_HOURS.get(key):return
    if _OFFLINE or key in _HOURS_TRIED or requests is None or BeautifulSoup is None:return
    _HOURS_TRIED.add(key)
    sess=_new_session()
    try:
        uuid,name=_retry_transient(_facility_find,sess,gphd)
        _m,fmeta=_retry_transient(_facility_detail,sess,uuid)
        _FACILITY_META[key]=dict(_FACILITY_META.get(key) or {},**fmeta,name=name)
        _FAC_HOURS[key]=parse_facility_hours(fmeta.get('hours',''))
        log('Giờ hoạt động của cơ sở: '+(fmeta.get('hours') or 'cơ sở không ghi — coi như làm nguyên ngày'))
    except Exception as e:
        log(f'⚠ Không tra lại được giờ hoạt động của cơ sở ({e}) — coi như làm nguyên ngày.')
    finally:
        try:sess.close()
        except Exception:pass
def set_facility_snapshot(path):
    """Chọn bản lưu dùng cho fetch_facility_staff (UI/tác vụ gọi trước khi chạy). Rỗng/None = không dùng bản lưu."""
    global _FACILITY_SNAPSHOT
    _FACILITY_SNAPSHOT=str(path or '')
def fetch_facility_staff(gphd,log=None,cancelled=None,use_cache=True,snapshot=None,ignore_snapshot=False):
    """Lấy TOÀN BỘ danh sách nhân sự của cơ sở có số giấy phép `gphd` từ web 1. Trả về
    (records, error): records = {mã chuẩn hoá: PractitionerRecord}; error = '' nếu thành công.
    Kết quả được giữ trong bộ nhớ FACILITY_CACHE_TTL giây để Preview/Export liên tiếp khỏi lấy lại."""
    log=log or (lambda m:None)
    cancelled=cancelled or (lambda:False)
    key=_normalize_code(gphd)
    if not key:return {},'Chưa nhập số giấy phép hoạt động của cơ sở.'
    snap=_FACILITY_SNAPSHOT if snapshot is None else snapshot
    if snap and not ignore_snapshot:
        # Có bản lưu trên máy => dùng luôn, KHÔNG tra mạng (danh mục cơ sở hiếm khi đổi; muốn cập nhật thì quét bản mới).
        try:
            import syt_store
            meta,recs=syt_store.load_snapshot(snap)
            if _normalize_code(meta.get('gphd',''))==key:
                if 'hours' in meta:
                    _FAC_HOURS[key]=parse_facility_hours(meta.get('hours',''))
                    _FACILITY_META[key]=dict(meta,name=meta.get('facility_name',''))
                log(f'Dùng bản lưu danh sách nhân sự cơ sở {gphd} ({syt_store.label(dict(meta,count=len(recs)))}) — không tra mạng.')
                return recs,''
            log(f'⚠ Bản lưu đang chọn thuộc GPHĐ khác ({meta.get("gphd","")}) — bỏ qua, lấy từ mạng.')
        except Exception as e:
            log(f'⚠ Không đọc được bản lưu: {e} — lấy từ mạng.')
    if _OFFLINE and not ignore_snapshot:
        return {},'OFFLINE: bản lưu đã chọn không khớp GPHĐ của cơ sở (hoặc không đọc được) — không kết nối mạng.'
    if requests is None or BeautifulSoup is None:
        return {},'Thiếu thư viện requests/beautifulsoup4.'
    with _FACILITY_LOCK:
        hit=_FACILITY_CACHE.get(key)
    if use_cache and hit and time.time()-hit[0]<FACILITY_CACHE_TTL:
        log(f'Dùng lại danh sách nhân sự cơ sở {gphd} đã lấy ({len(hit[1])} mã).')
        return hit[1],''
    with _FACILITY_LOCK:
        fail=_FACILITY_FAIL.get(key)
    if use_cache and fail and time.time()-fail[0]<300:
        return {},fail[1]
    sess=_new_session()
    try:
        log(f'Đang lấy danh sách nhân sự của cơ sở {gphd} từ web 1 (tracuu.medinet.org.vn)...')
        uuid,name=_retry_transient(_facility_find,sess,gphd)
        macoso,fmeta=_retry_transient(_facility_detail,sess,uuid)
        _FACILITY_META[key]=dict(fmeta,name=name)
        _FAC_HOURS[key]=parse_facility_hours(fmeta.get('hours',''))
        first=_retry_transient(_facility_page,sess,uuid,macoso,1)
        total=int(first.get('total_pages') or 1)
        records={}
        facility_name_txt=name
        facility_objects_to_records(first.get('objects') or [],gphd,facility_name_txt,into=records)
        for page in range(2,total+1):
            if cancelled():return records,'Đã hủy.'
            time.sleep(FACILITY_PAGE_DELAY)
            data=_retry_transient(_facility_page,sess,uuid,macoso,page)
            facility_objects_to_records(data.get('objects') or [],gphd,facility_name_txt,into=records)
            if page%20==0 or page==total:log(f'Danh sách nhân sự cơ sở: trang {page}/{total}...')
    except Exception as e:
        msg=f'Không lấy được danh sách nhân sự theo cơ sở: {e}'
        with _FACILITY_LOCK:
            _FACILITY_FAIL[key]=(time.time(),msg)
        return {},msg
    finally:
        try:sess.close()
        except Exception:pass
    with _FACILITY_LOCK:
        _FACILITY_CACHE[key]=(time.time(),records,name)
    log(f'Đã lấy {len(records)} nhân sự của cơ sở "{name}" ({gphd}).')
    return records,''

def fill_records_chain(codes,records,log=None,cancelled=None):
    """Tra cho các mã `codes` (đã chuẩn hoá) theo thứ tự: (1) WEB 1 tra lẻ, TUẦN TỰ đúng 1 mã mỗi
    giây (WEB1_PER_CODE_DELAY); (2) mã web 1 lỗi/không tìm thấy -> WEB 2 (thongtin) song song như
    cũ. Kết quả ghi vào `records` (mã nào có sẵn một bản ghi cũ — vd từ danh sách theo cơ sở — mà
    cả 2 web đều không tra được thì GIỮ NGUYÊN bản ghi cũ). Web 1 có 1 lỗi kết nối là dừng hẳn web
    1 cho phần còn lại của lượt (WEB1_SWITCH_THRESHOLD) — các mã còn lại chuyển thẳng sang web 2.
    Không chuẩn hoá mã khi gửi đi ngoài việc bỏ khoảng trắng/viết hoa đã có sẵn ở `codes`."""
    log=log or (lambda m:None)
    cancelled=cancelled or (lambda:False)
    codes=list(dict.fromkeys(codes))
    if not codes:return
    if _OFFLINE:
        log(f'OFFLINE: bỏ qua tra cứu bổ sung {len(codes)} mã chưa có trong bản lưu (không kết nối mạng).')
        return
    old=dict((c,records[c]) for c in codes if c in records)
    got1=load_disk_cached(codes,'web1')
    for c,r in got1.items():records[c]=r
    todo=[c for c in codes if c not in got1]
    if got1:log(f'Dùng lại {len(got1)}/{len(codes)} mã web 1 từ cache (đã tra gần đây).')
    need_w2=[]
    w1_err={}
    if todo:
        if requests is None or BeautifulSoup is None:
            need_w2=list(todo)
        else:
            pool=_SessionPool(prime=True)
            breaker=_CircuitBreaker(threshold=WEB1_SWITCH_THRESHOLD,cooldown=WEB1_SWITCH_COOLDOWN)
            fresh={}
            n=len(todo)
            log(f'Tra lẻ web 1: {n} mã, đúng 1 mã/giây (~{n} giây)...')
            for i,c in enumerate(todo,1):
                if cancelled():break
                if breaker.blocked():
                    need_w2.append(c)
                    continue
                try:
                    rec=_fetch_with_403_recovery(pool,lookup_tracuu,c,breaker=breaker,log=log,site_label='web 1',retry_403=False)
                except Exception as e:
                    rec=PractitionerRecord(error=f'Lỗi tra cứu web 1: {e}')
                time.sleep(WEB1_PER_CODE_DELAY)
                if rec.found and not rec.error:
                    records[c]=rec;fresh[c]=rec
                else:
                    need_w2.append(c)
                    if rec.error:w1_err[c]=rec.error
                if i%20==0 or i==n:log(f'Đã tra web 1: {i}/{n} mã...')
            if fresh:save_disk_cached(fresh,'web1')
    if need_w2 and not cancelled():
        log(f'{len(need_w2)} mã web 1 không tra được/không có — tra tiếp web 2 (thongtin.medinet.org.vn)...')
        tmp={}
        _fetch_parallel(need_w2,tmp,lookup_thongtin,log,cancelled,'web 2 (dự phòng)',cache_which='web2')
        for c in need_w2:
            r2=tmp.get(c)
            if r2 is not None and r2.found and not r2.error:
                records[c]=r2
            elif c in old:
                records[c]=old[c]      # giữ bản ghi theo cơ sở đã có, 2 web đều không tra thêm được
            else:
                err=' | '.join(e for e in (w1_err.get(c,''),(r2.error if r2 else '')) if e)
                records[c]=PractitionerRecord(found=False,error=err)

def _needs_full_history(rec,facility_gphd,dts):
    """Bản ghi lấy từ danh sách theo cơ sở chỉ ghi giai đoạn đang hiệu lực -> nếu không xác nhận được
    (status != 'x') ở bất kỳ thời điểm nào thì cần tra lẻ để lấy đủ lịch sử các giai đoạn."""
    return any(verify_at_facility_detailed(rec,facility_gphd,dt)[0]!='x' for dt in dts)

# ---------------------------------------------------------------------------
# Site 2: thongtin.medinet.org.vn — form ASPX/DotNetNuke, có trạng thái (VIEWSTATE)
# ---------------------------------------------------------------------------
def _extract_aspx_state(html):
    soup=BeautifulSoup(html,'html.parser')
    def _val(name):
        el=soup.select_one(f'input[name="{name}"]')
        return el.get('value','') if el else ''
    return {'__VIEWSTATE':_val('__VIEWSTATE'),'__VIEWSTATEGENERATOR':_val('__VIEWSTATEGENERATOR'),
            '__VIEWSTATEENCRYPTED':_val('__VIEWSTATEENCRYPTED'),'__EVENTVALIDATION':_val('__EVENTVALIDATION')}
def _thongtin_get_state(session):
    resp=session.get(f'{THONGTIN_BASE}{THONGTIN_PAGE_PATH}',timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return _extract_aspx_state(resp.text)
def _thongtin_post(session,event_target,cchn,state):
    data={'StylesheetManager_TSSM':'',
          'ScriptManager_TSM':';;System.Web.Extensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35:en:d20f15a2-81e5-4f24-80b9-9fe501a4ce09:ea597d4b:b25378d2',
          '__EVENTTARGET':event_target,'__EVENTARGUMENT':'',
          '__VIEWSTATE':state.get('__VIEWSTATE',''),'__VIEWSTATEGENERATOR':state.get('__VIEWSTATEGENERATOR',''),
          '__VIEWSTATEENCRYPTED':state.get('__VIEWSTATEENCRYPTED',''),'__EVENTVALIDATION':state.get('__EVENTVALIDATION',''),
          'dnn$ctr419$TimKiemCCHNY$txtSoChungChiSearch':cchn,'dnn$ctr419$TimKiemCCHNY$txtHoTenSearch':'',
          'ScrollTop':'','__dnnVariable':''}
    resp=session.post(f'{THONGTIN_BASE}{THONGTIN_PAGE_PATH}',data=data,
        headers={'Referer':f'{THONGTIN_BASE}{THONGTIN_PAGE_PATH}','Origin':THONGTIN_BASE},
        timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text
def lookup_thongtin(session,cchn):
    try:
        state=_retry_transient(_thongtin_get_state,session)
        html1=_retry_transient(_thongtin_post,session,'dnn$ctr419$TimKiemCCHNY$btnSearchCCHN',cchn,state)
    except Exception as e:
        return PractitionerRecord(error=f'Lỗi kết nối thongtin.medinet.org.vn: {e}')
    soup1=BeautifulSoup(html1,'html.parser')
    grid=soup1.select_one('#dnn_ctr419_TimKiemCCHNY_grvCCHNY')
    if not grid:
        return PractitionerRecord(found=False,source='thongtin')
    rows=grid.select('tr')[1:]
    target=_normalize_code(cchn)
    row_idx=None
    for i,tr in enumerate(rows):
        tds=tr.find_all('td')
        if len(tds)<4:continue
        if _normalize_code(_clean_text(tds[3].get_text()))==target:
            row_idx=i
            break
    if row_idx is None and len(rows)==1:
        # Cùng lý do như lookup_tracuu — kết quả CHỈ CÓ 1 dòng nghĩa là chính trang đã
        # chọn cho từ khoá này rồi, chấp nhận luôn dù chuỗi hiển thị lệch định dạng nhỏ.
        row_idx=0
    if row_idx is None:
        return PractitionerRecord(found=False,source='thongtin')
    state2=_extract_aspx_state(html1)
    event_target=f'dnn$ctr419$TimKiemCCHNY$grvCCHNY$ctl{row_idx+2:02d}$btnSoChungChi'
    try:
        html2=_retry_transient(_thongtin_post,session,event_target,cchn,state2)
    except Exception as e:
        return PractitionerRecord(found=True,source='thongtin',error=f'Lỗi lấy chi tiết thongtin.medinet.org.vn: {e}')
    soup2=BeautifulSoup(html2,'html.parser')
    def _lbl(id_):
        el=soup2.select_one(f'#{id_}')
        return _clean_text(el.get_text()) if el else ''
    name=_lbl('dnn_ctr419_TimKiemCCHNY_lbHoTen')
    cchn_full=_lbl('dnn_ctr419_TimKiemCCHNY_lbSoChungChi')
    status=_lbl('dnn_ctr419_TimKiemCCHNY_lbTinhTrang')
    workplaces=[]
    wtable=soup2.select_one('#dnn_ctr419_TimKiemCCHNY_gvDSDONVI_CCHNY')
    if wtable:
        for tr in wtable.select('tr')[1:]:
            tds=tr.find_all('td')
            if len(tds)<7:continue
            facility_text=_clean_text(tds[1].get_text())
            m=re.match(r'Số giấy phép:\s*([^\s]+)\s*-\s*(.*)',facility_text)
            gphd_visible,facility=(m.group(1),m.group(2)) if m else ('',facility_text)
            gphd_hidden=_clean_text(tds[6].get_text())  # cột ẩn "Số GP" — sạch hơn, ưu tiên dùng
            workplaces.append(Workplace(gphd=gphd_hidden or gphd_visible,facility_name=facility.strip(),
                position='',khoa='',ngay_lam=_clean_text(tds[2].get_text()),ngay_nghi=_clean_text(tds[3].get_text()),
                thoi_gian=_clean_text(tds[4].get_text()),address=_clean_text(tds[5].get_text())))
    return PractitionerRecord(found=True,source='thongtin',name=name,cchn=cchn_full,status=status,workplaces=workplaces)

# ---------------------------------------------------------------------------
# API chung: tra 1 mã, ưu tiên site 1 rồi mới sang site 2; và bộ nhớ đệm theo tiến trình
# ---------------------------------------------------------------------------
def lookup_practitioner(cchn,session=None,sleep_between=SLEEP_BETWEEN_REQUESTS):
    """[LEGACY — KHÔNG được app.py/data.py/ui.py gọi tới nữa; đường dẫn chính thức hiện tại
    là prefetch_staff_records + combine_with_dataframe (xem _pipeline_fetch_web2_then_web1).
    Giữ lại cho tương thích/tham khảo, chưa xoá.] Tra 1 mã CCHN: ưu tiên thongtin.medinet.org.vn
    (web 2, nguồn chính), nếu KHÔNG TÌM THẤY mã (hoặc lỗi kỹ thuật) mới thử tracuu.medinet.org.vn
    (web 1). CHÚ Ý: không áp dụng pacing 10s/request riêng cho web1 ở đây — nếu định dùng lại
    hàm này cho tra hàng loạt, phải tự thêm giãn cách WEB1_FALLBACK_DELAY_SECONDS cho nhánh
    web1, nếu không sẽ lặp lại đúng lỗi đã gặp (web1 bị chặn khi bắn liên tiếp quá nhanh).
    Trường hợp "tìm thấy người nhưng sai nơi công tác/sai giờ" (row-dependent, cần biết
    facility_gphd + thời điểm cụ thể của TỪNG dòng dữ liệu) KHÔNG xử lý ở đây. Nên truyền
    `session` dùng chung (requests.Session()) cho nhiều lượt tra liên tiếp để giữ cookie và
    tránh mở kết nối mới mỗi lần."""
    if requests is None or BeautifulSoup is None:
        return PractitionerRecord(error='Thiếu thư viện requests/beautifulsoup4 — cần cài đặt để dùng tính năng tra cứu này.')
    own_session=session is None
    if own_session:
        session=_new_session()
    try:
        record2=lookup_thongtin(session,cchn)
        if record2.found:return record2
        time.sleep(sleep_between)
        record1=lookup_tracuu(session,cchn)
        if record1.found:return record1
        error=' | '.join(e for e in (record2.error,record1.error) if e)
        return PractitionerRecord(found=False,error=error)
    finally:
        if own_session:session.close()

class LookupCache:
    """Bộ nhớ đệm theo TIẾN TRÌNH (không lưu ra đĩa) — mỗi mã CCHN chỉ thực sự tra mạng 1
    lần trong 1 lượt chạy, dù mã đó lặp lại nhiều lần trong dữ liệu đang xử lý. Không nên
    coi là cache lâu dài giữa các lần chạy khác nhau — dữ liệu hành nghề có thể thay đổi."""
    def __init__(self):
        self._data={}
        self._lock=threading.Lock()
    def get(self,cchn,session=None,sleep_between=SLEEP_BETWEEN_REQUESTS):
        key=_normalize_code(cchn)
        with self._lock:
            if key in self._data:return self._data[key]
        record=lookup_practitioner(cchn,session=session,sleep_between=sleep_between)
        with self._lock:
            self._data[key]=record
        return record
    def __len__(self):
        return len(self._data)

# --- Cache tra cứu ra đĩa: hàm đọc/ghi -------------------------------------------------
def _record_to_jsonable(r):
    return {'found':r.found,'source':r.source,'name':r.name,'cchn':r.cchn,'status':r.status,'error':r.error,
        'scope':r.scope,'position':r.position,'nationality':r.nationality,
        'workplaces':[{'gphd':w.gphd,'facility_name':w.facility_name,'position':w.position,'khoa':w.khoa,
            'ngay_lam':w.ngay_lam,'ngay_nghi':w.ngay_nghi,'thoi_gian':w.thoi_gian,'address':w.address}
            for w in r.workplaces]}
def _record_from_jsonable(d):
    return PractitionerRecord(found=d.get('found',False),source=d.get('source',''),name=d.get('name',''),
        cchn=d.get('cchn',''),status=d.get('status',''),error=d.get('error',''),
        scope=d.get('scope',''),position=d.get('position',''),nationality=d.get('nationality',''),
        workplaces=[Workplace(**w) for w in d.get('workplaces') or []])

DISK_FMT=2
_disk_cache_lock=threading.Lock()
def _read_disk_cache():
    try:
        f=cache_file()
        if f.exists():
            return json.loads(f.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {}
def _write_disk_cache(raw):
    try:
        f=cache_file()
        tmp=f.with_suffix('.tmp')
        tmp.write_text(json.dumps(raw,ensure_ascii=False),encoding='utf-8')
        tmp.replace(f)
    except Exception:
        pass  # cache ra đĩa chỉ là tối ưu tốc độ, không được phép làm hỏng lượt tra cứu

def load_disk_cached(codes,which):
    """which: 'web1' hoặc 'web2'. codes ĐÃ chuẩn hoá sẵn. Trả về dict {mã: PractitionerRecord}
    cho các mã còn hạn (<CACHE_TTL_SECONDS) có sẵn kết quả `which` trong cache đĩa — dùng để
    khỏi tra mạng lại các mã đã biết trong CÙNG 1 buổi làm việc (nhiều lần chạy gần nhau)."""
    now=time.time()
    with _disk_cache_lock:
        raw=_read_disk_cache()
    out={}
    for code in codes:
        entry=raw.get(code)
        if not entry or (now-entry.get('ts',0))>=CACHE_TTL_SECONDS or entry.get('fmt',1)<DISK_FMT:continue  # fmt<2: bản cũ chưa đọc các dòng giai đoạn phụ
        rd=entry.get(which)
        if rd is None:continue
        try:out[code]=_record_from_jsonable(rd)
        except Exception:continue
    return out

def save_disk_cached(records,which):
    """Ghi/merge kết quả MỚI tra được vào cache đĩa, dưới đúng khoá `which`, giữ nguyên
    phần dữ liệu khác đã có (vd ghi web1 không xoá mất web2 đã lưu trước đó cho cùng mã) —
    CHỈ cache lại bản ghi found=True (THỰC SỰ tìm thấy). KHÔNG cache lại bản ghi có `error`
    (lỗi kỹ thuật/mạng nên thử lại ngay lần sau) — VÀ CŨNG KHÔNG cache lại found=False ("không
    tìm thấy"), vì đây có thể chỉ là 1 lần tra bị trục trặc tạm thời (rớt mạng giữa chừng,
    trang tạm thời không trả kết quả tìm kiếm...) chứ không chắc là trang THỰC SỰ không có mã
    này — nếu lỡ cache nhầm 1 lần "không tìm thấy" như vậy, các lượt chạy SAU trong 6 giờ tới
    sẽ cứ dùng lại kết quả sai đó (trong thời hạn cache) thay vì thử tra lại. Ưu tiên tra lại (chấp nhận chậm hơn
    1 chút) hơn là có nguy cơ báo sai "Không đăng ký với SYT" cho 1 mã THỰC SỰ có đăng ký."""
    usable={c:r for c,r in records.items() if not r.error and r.found}
    if not usable:return
    with _disk_cache_lock:
        raw=_read_disk_cache()
        now=time.time()
        for code,rec in usable.items():
            entry=raw.get(code) or {}
            entry['ts']=now
            entry['fmt']=DISK_FMT
            entry[which]=_record_to_jsonable(rec)
            raw[code]=entry
        _write_disk_cache(raw)

# ---------------------------------------------------------------------------
# Tích hợp vào GDP: đối chiếu (mã bác sĩ, thời điểm) trong dữ liệu đã lọc với 2 cổng
# tra cứu trên, kết hợp với FileDM_NHANVIENYTE (danh mục nhân viên nội bộ) và 1 giấy
# phép bệnh viện tự nhập, phân vào 3 trường hợp bất thường cần rà soát.
# ---------------------------------------------------------------------------
@dataclass
class MedinetVerifyRule:
    """Cấu hình cho 1 lượt đối chiếu. 2 cặp cột được tra ĐỘC LẬP (mỗi cặp bật/tắt riêng,
    mặc định cả 2 đều bật) — ví dụ cặp 1 = (MA_BAC_SI, NGAY_YL), cặp 2 =
    (MA_BSI_THUC_HIEN, NGAY_TH_YL). staff_dir_file là FileDM_NHANVIENYTE (hoặc file tương
    tự) dùng để biết mã có nằm trong danh mục nhân viên nội bộ hay không — chỉ cần đúng 1
    cột chứa mã CCHN (staff_dir_key_column), không cần tách riêng cột tên/chức danh cho
    tính năng này. facility_gphd là số giấy phép hoạt động của bệnh viện đang xử lý dữ
    liệu, tự nhập tay — dùng để tìm đúng dòng "Nơi công tác" khớp với bệnh viện NÀY trong
    kết quả tra cứu (một người có thể làm nhiều nơi). KHÔNG giới hạn số mã tra cứu — tự động
    tra hết toàn bộ mã CCHN duy nhất xuất hiện trong dữ liệu ĐÃ QUA LỌC (nếu có bộ lọc khác
    đang bật), dù nhiều hay ít."""
    enabled:bool=False
    pair1_enabled:bool=True
    pair1_doctor_column:str=''
    pair1_time_column:str=''
    pair2_enabled:bool=True
    pair2_doctor_column:str=''
    pair2_time_column:str=''
    staff_dir_file:str=''
    staff_dir_key_column:str=''
    facility_gphd:str=''
    export_mode:str='summary'  # 3 kiểu xuất (bấm 1 nút bấm nhỏ đổi vòng qua 3 kiểu ở ui.py):
                                #   'summary'    (mặc định) — CHỈ 1 sheet tổng hợp "KET_QUA",
                                #       gọn nhẹ nhất, không cần quét cột gốc .db.
                                #   'multi_sheet' — 1 file .xlsx, 5 sheet: KET_QUA + 4 sheet
                                #       chi tiết (1 sheet/loại lỗi, tên sheet = đúng tên
                                #       "Loại lỗi" — xem REASON_SHEET).
                                #   'multi_file' — TÁCH thành 5 file .xlsx riêng (tên file =
                                #       tên sheet tương ứng ở kiểu 'multi_sheet'), cho trường
                                #       hợp cần gửi riêng từng loại lỗi.
                                # 'multi_sheet'/'multi_file' đều cần dựng đủ chi tiết (như
                                # export_details=True cũ); chỉ 'summary' mới bỏ qua bước đó.
    web1_safe_pacing:bool=False  # tốc độ tra web 1 (bổ sung — tracuu.medinet.org.vn):
                                 #   False (mặc định — ƯU TIÊN TỐC ĐỘ): tra web 1 SONG SONG
                                 #       MAX_LOOKUP_WORKERS luồng, SLEEP_BETWEEN_REQUESTS=0.6s
                                 #       — giống hệt tốc độ cũ TRƯỚC khi đo được ngưỡng thật
                                 #       (~1 request/10s). Lỗi kết nối (bất kỳ loại nào) vẫn
                                 #       DỪNG HẲN web 1 ngay cho phần còn lại của lượt chạy
                                 #       (WEB1_SWITCH_THRESHOLD=1 — không đổi, không phụ thuộc
                                 #       option này), nên không có vòng lặp lỗi lặp lại — chỉ
                                 #       là có rủi ro bị chặn sớm hơn, cao hơn nếu web 1 có
                                 #       nhiều mã cần rà.
                                 #   True — AN TOÀN (CHẬM): tra web 1 TUẦN TỰ, đúng 1 luồng
                                 #       (WEB1_FALLBACK_WORKERS=1), nghỉ đúng
                                 #       WEB1_FALLBACK_DELAY_SECONDS=10.0s sau mỗi request —
                                 #       đúng ngưỡng an toàn đã đo bằng test_web1_dong_thoi_10s.py.
                                 #       Bật lên khi cần rà nhiều mã qua web 1 và muốn tránh bị
                                 #       chặn, chấp nhận chạy chậm hơn nhiều.
                                 # LƯU Ý: từ bản dùng "danh sách nhân sự theo cơ sở", web 1 tra lẻ luôn
                                 # tuần tự 1 mã/giây (WEB1_PER_CODE_DELAY) bất kể tuỳ chọn này; tuỳ chọn
                                 # này chỉ còn ý nghĩa với nhánh cũ _pipeline_fetch_web2_then_web1.
    scope_sheet:bool=True  # thêm sheet "PHAM_VI_DK_SYT" đối chiếu phạm vi đăng ký SYT với chi phí trong XML
    kho_file:str=''        # file kho gợi ý (kho_goi_y_*.xlsx) — sheet 'MA_CP DVKT' cho chương chuyên môn của DVKT
    loai_kcb:list=field(default_factory=list)  # chỉ xét các dòng có MA_LOAI_KCB thuộc danh sách này (vd ['01','03']); RỖNG = xuất hết, không lọc
    facility_snapshot:str=''  # đường dẫn bản lưu danh sách nhân sự cơ sở (syt_store); rỗng = lấy từ mạng như cũ
    syt_offline:bool=False  # offline hoàn toàn: chỉ dùng bản lưu, không gọi mạng (xem configure_from_rule)

REASON_LABELS={
    # ĐÚNG 4 loại lỗi (theo file mẫu m_u_2.xlsx đã chốt) — mỗi loại được kiểm tra ĐỘC LẬP
    # với các loại còn lại, nên 1 dòng .db có thể vướng ĐỒNG THỜI nhiều loại (vd vừa
    # "Không đăng ký với BHYT" vừa "Sai thứ làm việc") — khi đó ra NHIỀU dòng riêng ở
    # KET_QUA (1 dòng/loại), không còn if/elif chọn ĐÚNG 1 lý do như bản cũ:
    #   - 'ngoai_bhyt'   : mã KHÔNG có trong file nhân viên (staff_keys) — độc lập hoàn
    #                      toàn với kết quả tra cổng SYT.
    #   - 'sai_gio'      : có trong file nhân viên, cổng SYT có xác nhận nơi công tác +
    #                      đúng giai đoạn ngày làm-nghỉ, nhưng giờ cụ thể (check_dt) nằm
    #                      NGOÀI mọi khung giờ đã đăng ký cho đúng thứ đó.
    #   - 'sai_thu'      : như trên nhưng THỨ trong tuần của check_dt hoàn toàn KHÔNG có ca
    #                      trực nào đăng ký (khác 'sai_gio' — đây là sai NGÀY, không phải
    #                      sai GIỜ trong 1 ngày có ca).
    #   - 'khong_dk_syt' : không tìm thấy mã trên cổng, HOẶC tìm thấy mã nhưng không có nơi
    #                      công tác nào khớp giấy phép bệnh viện đã nhập, HOẶC khớp nơi công
    #                      tác nhưng check_dt nằm ngoài khoảng ngày làm việc-ngày nghỉ việc
    #                      đã đăng ký (verify_at_facility_detailed đã gộp sẵn cả 3 trường
    #                      hợp này vào đúng 1 status 'no_match', xem hàm đó) — dù vậy nếu mã
    #                      CÓ tồn tại trên cổng (chỉ là không khớp nơi/giai đoạn) thì cột
    #                      "Họ tên trên cổng" vẫn có thông tin, không để trống.
    'ngoai_bhyt':'Không đăng ký với BHYT',
    'sai_gio':'Sai giờ làm việc',
    'sai_thu':'Sai thứ làm việc',
    'khong_dk_syt':'Không đăng ký với SYT',
    'loi_mang':'Chưa tra cứu được (lỗi mạng)',   # không kết nối/không tra được cổng SYT — KHÔNG kết luận sai, cần chạy lại
}
REASON_EXPLAIN={  # "Giải thích lỗi" — nguyên văn cố định theo file mẫu, không đổi theo dữ liệu
    'ngoai_bhyt':'không có trong file nhân viên',
    'sai_gio':'Giờ làm việc không đăng ký trên cổng',
    'sai_thu':'Ngày làm việc không đăng ký trên cổng',
    'khong_dk_syt':'Không có CCHN tra cứu trên SYT',
    'loi_mang':'Không kết nối được cổng SYT nên chưa kiểm tra được',
}
REASON_SHEET=dict(REASON_LABELS)  # tên sheet chi tiết = ĐÚNG tên "Loại lỗi" (người dùng đã xác nhận)
# Các cột LẤY NGUYÊN VẸN từ .db gốc cho sheet chi tiết (chỉ lấy cột nào THỰC SỰ có trong dữ
# liệu — .db khác nhau có thể thiếu vài cột, bỏ qua cột thiếu chứ không báo lỗi). Sheet chi
# tiết = các cột này + 3 cột tính toán (NGAY_LAM_VIEC/NGAY_NGHI_VIEC/THOI_GIAN_LAM_VIEC) +
# 1 cột giá trị cụ thể (giờ/thứ chính xác của dòng đó) — RIÊNG 'ngoai_bhyt'/'khong_dk_syt'
# KHÔNG có cột giá trị cụ thể này (không phải vấn đề giờ/thứ cụ thể).
DETAIL_BASE_COLUMNS=['XML1_ID','KY_QT','MA_CSKCB','MA_LOAI_KCB','MA_LK','MA_BN','HO_TEN','MA_THE','NGAY_VAO','NGAY_RA',
    'MA_BENH','MA_BENHKHAC','ID_CP','LOAI_CP','MA_CP','MA_NHOM','TEN_CP','SO_DANG_KY','SO_LUONG','DON_GIA',
    'THANH_TIEN','TYLE_TT','NGAY_YL','T_BHTT_DTL','MUC_HUONG_DTL','MA_BAC_SI','NGAY_TH_YL','MA_BSI_THUC_HIEN','MA_KHOA','TEN_KHOA']
def _parse_vn_number(val):
    """Chuyển 1 giá trị số kiểu Việt Nam (.db có thể lưu dạng text, dấu phẩy là thập phân,
    dấu chấm là phân cách nghìn) thành float, để CỘNG DỒN đúng cho THANH_TIEN/T_BHTT_DTL ở
    sheet KET_QUA. Trả về 0.0 nếu rỗng/None/không parse được (không làm hỏng phép cộng)."""
    if val is None:return 0.0
    if isinstance(val,(int,float)):
        try:
            if pd.isna(val):return 0.0
        except Exception:pass
        return float(val)
    s=str(val).strip()
    if not s:return 0.0
    if ',' in s and '.' in s:
        s=s.replace('.','').replace(',','.')  # kiểu VN: "1.234.567,89" -> "1234567.89"
    elif ',' in s:
        s=s.replace(',','.')  # chỉ có dấu phẩy -> coi là dấu thập phân kiểu VN: "123,45" -> "123.45"
    elif '.' in s:
        # Chỉ có dấu chấm, không có phẩy — tiền VND thường ghi dấu chấm làm phân cách NGHÌN
        # (vd "30.000" nghĩa là 30 nghìn, không phải 30.0) — CHỈ coi là dấu thập phân thật
        # khi đúng 1 dấu chấm và phần sau có 1-2 chữ số (không phải nhóm 3 số kiểu phân cách
        # nghìn, vốn luôn đúng 3 chữ số mỗi nhóm).
        parts=s.split('.')
        if not(len(parts)==2 and len(parts[1]) in (1,2)):
            s=s.replace('.','')  # "30.000"/"1.234.567" -> phân cách nghìn, bỏ hết dấu chấm
    try:return float(s)
    except ValueError:return 0.0

def _read_staff_dir_df(path):
    path=Path(path)
    if not path.exists():raise ValueError(f'Không tìm thấy file danh mục nhân viên: {path}')
    return pd.read_excel(path,dtype=str,keep_default_na=False) if path.suffix.lower() in {'.xlsx','.xls'} else pd.read_csv(path,dtype=str,keep_default_na=False)
def load_staff_dir_keys(path,key_column):
    """Đọc 1 file danh mục nhân viên (bất kỳ file bảng nào, không riêng
    FileDM_NHANVIENYTE) và trả về set các mã (đã chuẩn hoá) trong key_column — dùng để
    biết nhanh 1 mã có nằm trong danh mục nội bộ hay không, không cần tra tên/chức danh
    cho tính năng đối chiếu này."""
    df=_read_staff_dir_df(path)
    if key_column not in df.columns:raise ValueError(f'File danh mục nhân viên không có cột "{key_column}".')
    return set(_normalize_code(v) for v in df[key_column] if str(v or '').strip())
def load_staff_dir_value_map(path,key_column,value_column):
    """Đọc 1 file danh mục nhân viên, trả về dict {mã đã chuẩn hoá (theo key_column): giá trị
    GỐC của value_column (giữ nguyên, không chuẩn hoá) ở dòng ĐẦU TIÊN gặp mã đó} — dùng chung
    cho các cột "tra thêm theo mã nhân viên" từ file danh mục (hiện tại: CHUCDANH_NN cho cột
    "Chức danh đăng ký BHYT" ở KET_QUA). Nếu file không có value_column, trả về dict rỗng
    (KHÔNG báo lỗi) — cột kết quả tương ứng chỉ để trống toàn bộ, không làm hỏng cả lượt xuất."""
    df=_read_staff_dir_df(path)
    if key_column not in df.columns:raise ValueError(f'File danh mục nhân viên không có cột "{key_column}".')
    if value_column not in df.columns:return {}
    out={}
    for k,v in zip(df[key_column],df[value_column]):
        ks=str(k or '').strip()
        if not ks:continue
        norm=_normalize_code(ks)
        if not norm or norm in out:continue
        out[norm]=str(v or '').strip()
    return out
CHUCDANH_BHYT_LABELS={
    # Đúng bảng mã CHUCDANH_NN -> tên chức danh đã chốt, cho cột "Chức danh đăng ký BHYT".
    '1':'Bác sỹ',
    '2':'Y sỹ',
    '3':'Điều dưỡng',
    '4':'Hộ sinh',
    '5':'Kỹ thuật y',
    '6':'Cử nhân tâm lý lâm sàng',
    '7':'Lương y',
    '8':'Dược sỹ',
    '9':'Người tham gia vào quá trình khám bệnh, chữa bệnh nhưng không cần phải cấp giấy phép hành nghề',
}
def _chuc_danh_bhyt_label(raw):
    """Chuẩn hoá NHẸ giá trị thô đọc từ CHUCDANH_NN (Excel có thể lưu số nguyên dạng float,
    vd "1.0" thay vì "1") rồi tra bảng CHUCDANH_BHYT_LABELS. Mã lạ (không phải 1-9) hoặc rỗng
    -> trả '' (để trống, không đoán bừa)."""
    s=str(raw or '').strip()
    if not s:return ''
    if s.endswith('.0') and s[:-2].isdigit():s=s[:-2]
    return CHUCDANH_BHYT_LABELS.get(s,'')
def load_staff_dir_raw_map(path,key_column):
    """Như load_staff_dir_keys, nhưng trả về dict {mã đã chuẩn hoá: giá trị GỐC (chưa chuẩn
    hoá, giữ nguyên định dạng trong file) đầu tiên gặp trong file} — dùng làm nguồn DỰ PHÒNG
    cho cột "Mã người hành nghề" ở KET_QUA khi web không tra được (xem combine_with_dataframe):
    thứ tự ưu tiên là (1) mã do CHÍNH cổng SYT trả về nếu tra được — đáng tin nhất, (2) mã
    đã đăng ký trong file danh mục nhân viên nội bộ nếu có, (3) mã thô lấy thẳng từ .db gốc —
    không bao giờ để trống ô này."""
    df=_read_staff_dir_df(path)
    if key_column not in df.columns:raise ValueError(f'File danh mục nhân viên không có cột "{key_column}".')
    out={}
    for v in df[key_column]:
        s=str(v or '').strip()
        if not s:continue
        norm=_normalize_code(s)
        if norm and norm not in out:
            out[norm]=s
    return out

# 10 mã loại hình khám bệnh, chữa bệnh (MA_LOAI_KCB) — dùng cho ô chọn ở mục F
LOAI_KCB_LABELS=[('01','Khám bệnh'),('02','Điều trị ngoại trú'),('03','Điều trị nội trú'),('04','Điều trị nội trú ban ngày'),
    ('05','Điều trị ngoại trú các bệnh mạn tính dài ngày liên tục trong năm (TT 46/2016/TT-BYT), có khám bệnh và lĩnh thuốc'),
    ('06','Điều trị lưu tại Trạm Y tế tuyến xã, Phòng khám đa khoa khu vực'),('07','Nhận thuốc theo hẹn (không khám bệnh)'),
    ('08','Điều trị ngoại trú các bệnh mạn tính dài ngày (TT 46/2016/TT-BYT), có khám bệnh, có thực hiện DVKT và/hoặc sử dụng thuốc'),
    ('09','Điều trị nội trú dưới 04 giờ'),('10','Các trường hợp khác')]
def kcb_codes(rule):
    """Tập mã loại KCB đã chọn, chuẩn hoá về số nguyên (rỗng = không lọc)."""
    out=[]
    for v in getattr(rule,'loai_kcb',None) or []:
        try:out.append(int(float(str(v).strip())))
        except ValueError:pass
    return sorted(set(out))
def medinet_lookup_columns(rule):
    """Trả về danh sách các cột (mã bác sĩ + thời gian của cặp nào đang bật, bỏ trùng) cần
    thiết TỐI THIỂU để đối chiếu CCHN — dùng để giới hạn truy vấn .db chỉ lấy đúng các cột
    này khi không cần giữ nguyên toàn bộ cột gốc (Preview, hoặc Export khi không xuất chi
    tiết) — tránh phải tải/quét mọi cột của .db (có thể rất lớn) khi chỉ vài cột là đủ."""
    cols=[]
    if rule.pair1_enabled and rule.pair1_doctor_column and rule.pair1_time_column:
        cols+=[rule.pair1_doctor_column,rule.pair1_time_column]
    if rule.pair2_enabled and rule.pair2_doctor_column and rule.pair2_time_column:
        cols+=[rule.pair2_doctor_column,rule.pair2_time_column]
    seen=set();out=[]
    for c in cols:
        if c not in seen:
            seen.add(c);out.append(c)
    return out
def _fetch_parallel(codes,target_dict,fetch_fn,log,cancelled,label,cache_which=None):
    """Tra 1 danh sách mã SONG SONG (ThreadPoolExecutor, MAX_LOOKUP_WORKERS luồng) thay vì
    tuần tự — nhanh hơn nhiều lần khi có hàng nghìn mã. CHỈ DÙNG CHO WEB 2 (thongtin — nguồn
    chính, đã kiểm chứng ổn định ở MAX_LOOKUP_WORKERS luồng song song): web 1 (tracuu) không
    bao giờ đi qua hàm này nữa (ngưỡng thật quá thấp — xem WEB1_FALLBACK_WORKERS/
    WEB1_FALLBACK_DELAY_SECONDS — luôn tra TUẦN TỰ qua _pipeline_fetch_web2_then_web1, không
    có hàm song song riêng). Mỗi LUỒNG dùng 1 session RIÊNG (xem _SessionPool) — không tạo
    session mới cho từng mã (đỡ phải mồi cookie lại mỗi lần), nhưng cũng KHÔNG dồn cả đợt
    (có thể hàng trăm/hàng nghìn mã) vào ĐÚNG 1 cookie duy nhất, để giảm nguy cơ bị hệ thống
    chống spam của server khoá cứng 1 cookie sau khi thấy nó gửi liên tục quá nhiều request.
    Nếu gặp 403 vẫn tự đổi cookie mới thử lại 1 lần (xem _fetch_with_403_recovery). Ghi kết
    quả thẳng vào target_dict (dict thường — an toàn vì mỗi mã chỉ được đúng 1 luồng gán,
    không có luồng nào đọc-sửa-ghi cùng 1 khoá). Giữ SLEEP_BETWEEN_REQUESTS sau mỗi lượt tra
    (tính theo từng luồng, không phải toàn cục) để không gửi request dồn dập tới server.
    cache_which (tuỳ chọn, luôn là 'web2' trong thực tế): nếu có, tra cache đĩa TRƯỚC — chỉ
    những mã KHÔNG có trong cache (hoặc đã hết hạn) mới thực sự tra mạng; kết quả mới tra
    được sẽ lưu lại vào cache đĩa sau khi xong, để lần chạy KẾ TIẾP trong cùng buổi làm việc
    khỏi tra lại."""
    if not codes:return
    remaining=codes
    if cache_which:
        cached=load_disk_cached(codes,cache_which)
        for c,r in cached.items():
            target_dict[c]=r
        remaining=[c for c in codes if c not in cached]
        if cached:
            log(f'Dùng lại {len(cached)}/{len(codes)} mã {label} từ cache (đã tra gần đây)...')
    if not remaining:return
    # Kiểm tra kết nối NHANH trước khi chạy hàng loạt (xem preflight_check ở
    # _pipeline_fetch_web2_then_web1 — cùng lý do: tránh mất thời gian tra từng mã chỉ để
    # nhận đúng 1 loại lỗi lặp lại hàng trăm/nghìn lần khi trang đã bị chặn/mất kết nối ngay
    # từ đầu).
    pf=preflight_check()
    site_ok,site_err=pf['web2']
    if not site_ok:
        log(f'⚠ {label} không gọi được ngay từ đầu (kiểm tra nhanh trước khi chạy hàng loạt): {site_err} — bỏ qua {len(remaining)} mã còn lại trong đợt này.')
        err_rec=PractitionerRecord(error=f'Không gọi được (kiểm tra kết nối trước khi chạy hàng loạt phát hiện lỗi): {site_err}')
        for c in remaining:
            target_dict[c]=err_rec
        return
    total=len(remaining)
    done=[0]
    lock=threading.Lock()
    fresh={}
    pool=_SessionPool(prime=False)
    breaker=_CircuitBreaker()  # ngưỡng mặc định — web2 đã chứng minh ổn định qua test thực tế.
    site_label='web 2'
    def worker(code):
        if cancelled():return
        # Breaker đã mở (tạm dừng do gặp lỗi liên tiếp) -> trả lỗi NGAY, KHÔNG gọi mạng — kiểm
        # tra ở ĐÂY (trước khi vào _fetch_with_403_recovery) để còn BỎ LUÔN cả time.sleep bên
        # dưới (sleep chỉ có ý nghĩa giãn cách request THẬT tới server — không gọi mạng thì
        # không cần chờ gì cả) và để log đúng tình trạng hiện tại, tránh gây hiểu lầm là vẫn
        # đang tiếp tục tra {site_label} cho các mã còn lại.
        skip_network=breaker is not None and breaker.blocked()
        if skip_network:
            rec=PractitionerRecord(error=f'Đã tạm dừng gọi {site_label} (vừa gặp lỗi kết nối liên tiếp trước đó trong lượt chạy này).')
        elif requests is None:
            rec=PractitionerRecord(error='Thiếu thư viện requests/beautifulsoup4 — cần cài đặt để dùng tính năng tra cứu này.')
        else:
            try:
                rec=_fetch_with_403_recovery(pool,fetch_fn,code,breaker=breaker,log=log,site_label=site_label,retry_403=True)
            except Exception as e:
                rec=PractitionerRecord(error=f'Lỗi tra cứu: {e}')
        target_dict[code]=rec
        fresh[code]=rec
        if not skip_network:
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        with lock:
            done[0]+=1
            if done[0]%20==0 or done[0]==total:
                note=' (đã tạm dừng, không còn gọi mạng tới đây nữa)' if skip_network else ''
                log(f'Đã tra {label}: {done[0]}/{total} mã...{note}')
    with ThreadPoolExecutor(max_workers=MAX_LOOKUP_WORKERS) as ex:
        futures=[ex.submit(worker,c) for c in remaining]
        for f in futures:
            f.result()
    if cache_which and fresh:
        save_disk_cached(fresh,cache_which)
def _pipeline_fetch_web2_then_web1(codes,records1,records2,code_to_dts,facility_gphd,log,cancelled,extra_web1_codes=None,web1_safe_pacing=False):
    """Tra cache đĩa trước (xem load_disk_cached) — mã nào còn hạn thì khỏi tra mạng lại. Với
    phần còn lại: tra WEB 2 (nguồn chính) SONG SONG cho `codes` (MAX_LOOKUP_WORKERS luồng);
    ngay khi có kết quả web 2 của 1 MÃ, kiểm tra LUÔN (dùng code_to_dts đã tính sẵn — {mã:
    [check_dt,...]}) xem mã đó có cần rà thêm WEB 1 hay không (web 2 không xác nhận được
    đúng nơi công tác/thời gian) — nếu cần thì NỘP NGAY task web 1, không đợi các mã khác
    tra web 2 xong hết. Kết quả: 2 việc "tra web 2" và "rà web 1 cho mã vừa xong" CHẠY CHỒNG
    LẤN nhau (mã A đang rà web 1 trong khi mã B/C/D vẫn còn đang tra web 2).
    web1_safe_pacing (xem MedinetVerifyRule.web1_safe_pacing — mặc định False, lựa chọn của
    người dùng, KHÔNG phải mặc định kỹ thuật an toàn nhất):
      - False (mặc định — ưu tiên tốc độ): web 1 tra SONG SONG MAX_LOOKUP_WORKERS luồng,
        SLEEP_BETWEEN_REQUESTS=0.6s/luồng — giống hệt tốc độ cũ. Lỗi kết nối (bất kỳ loại
        nào) vẫn DỪNG HẲN web 1 ngay cho phần còn lại của lượt chạy (WEB1_SWITCH_THRESHOLD=1
        — áp dụng CẢ 2 chế độ, không phụ thuộc option này) — nên không "lặp lỗi", chỉ là dễ
        chạm ngưỡng thật đã đo (~1 request/10s cho toàn hệ thống) sớm hơn nếu có nhiều mã
        cần rà qua web 1, khiến web 1 hay bị dừng sớm (quay về dùng dữ liệu web 2 đã có).
      - True (an toàn, chậm): web 1 tra TUẦN TỰ, đúng 1 luồng (WEB1_FALLBACK_WORKERS), nghỉ
        đúng WEB1_FALLBACK_DELAY_SECONDS=10.0s sau mỗi request — đúng ngưỡng an toàn đã đo
        bằng test_web1_dong_thoi_10s.py.
    Kết quả mới tra được (không tính mã lấy từ cache) sẽ lưu lại vào cache đĩa sau khi xong
    toàn bộ — records1 (web 2) dưới khoá cache 'web2', records2 (web 1) dưới khoá cache 'web1'.
    extra_web1_codes (tuỳ chọn): các mã đã BIẾT TRƯỚC là cần rà web 1 (vd đã có sẵn web 2 từ
    danh mục nhân viên, không cần tra lại web2 ở đây) — nộp NGAY từ đầu vào pool web 1.
    An toàn treo/crash:
    - Chờ theo từng đợt NGẮN (wait(...,timeout=1,FIRST_COMPLETED)) thay vì đợi 1 lần duy
      nhất cho tất cả — vòng lặp luôn có điểm dừng để kiểm tra cancelled(), không bao giờ
      chờ vô hạn ở 1 chỗ.
    - Mỗi request mạng đã có REQUEST_TIMEOUT riêng (xem lookup_tracuu/lookup_thongtin) nên
      1 request treo không thể làm treo cả pool.
    - Lỗi ở 1 future (nếu có) chỉ bỏ qua đúng future đó (try/except quanh .result()), không
      làm hỏng các future còn lại hay dừng cả vòng lặp.
    - Khi cancelled(): không nộp thêm task mới, để pool tự rút dần (drain) các task đang
      chạy dở rồi thoát — không bỏ luồng nào "treo lửng lơ" phía sau."""
    extra_web1_codes=extra_web1_codes or []
    if not codes and not extra_web1_codes:return
    # --- Cache đĩa trước: khỏi tra mạng lại các mã đã biết gần đây (xem load_disk_cached) ---
    cached1=load_disk_cached(codes,'web2')
    for c,r in cached1.items():records1[c]=r
    codes_net=[c for c in codes if c not in cached1]
    if cached1:log(f'Dùng lại {len(cached1)}/{len(codes)} mã web 2 từ cache (đã tra gần đây)...')
    # Với các mã đã lấy web2 từ cache, vẫn phải TỰ kiểm tra ngay ở đây xem có cần rà web1 hay
    # không (bình thường việc này diễn ra trong vòng lặp bên dưới ngay sau khi web2_task trả
    # về — nhưng mã lấy từ cache thì không đi qua web2_task).
    extra_from_cache=[]
    if facility_gphd:
        for c,r in cached1.items():
            dts=code_to_dts.get(c,[])
            if any(verify_at_facility_detailed(r,facility_gphd,dt)[0]!='x' for dt in dts):
                extra_from_cache.append(c)
    all_extra_web1=list(dict.fromkeys(list(extra_web1_codes)+extra_from_cache))
    cached2=load_disk_cached(all_extra_web1,'web1')
    for c,r in cached2.items():records2[c]=r
    extra_web1_net=[c for c in all_extra_web1 if c not in cached2]
    if cached2:log(f'Dùng lại {len(cached2)}/{len(all_extra_web1)} mã web 1 từ cache (đã tra gần đây)...')
    # --- Kiểm tra kết nối NHANH trước khi chạy hàng loạt (xem preflight_check) — nếu 1 trang
    # đã bị chặn/mất kết nối ngay từ request đầu tiên, KHÔNG mất thời gian tra từng mã (mỗi mã
    # còn có 1 lần thử lại riêng) chỉ để nhận đúng 1 loại lỗi lặp lại hàng trăm/nghìn lần — báo
    # lỗi NGAY cho toàn bộ mã liên quan, chỉ tốn 1-2 request thăm dò thay vì cả đợt.
    if codes_net or extra_web1_net:
        pf=preflight_check()
        if codes_net and not pf['web2'][0]:
            log(f'⚠ Web 2 không gọi được ngay từ đầu (kiểm tra nhanh trước khi chạy hàng loạt): {pf["web2"][1]} — bỏ qua tra web 2 cho {len(codes_net)} mã còn lại trong đợt này.')
            err_rec=PractitionerRecord(error=f'Web 2 không gọi được (kiểm tra kết nối trước khi chạy hàng loạt phát hiện lỗi): {pf["web2"][1]}')
            for c in codes_net:records1[c]=err_rec
            codes_net=[]
        if extra_web1_net and not pf['web1'][0]:
            log(f'⚠ Web 1 không gọi được ngay từ đầu (kiểm tra nhanh trước khi chạy hàng loạt): {pf["web1"][1]} — bỏ qua tra web 1 cho {len(extra_web1_net)} mã còn lại trong đợt này.')
            err_rec2=PractitionerRecord(error=f'Web 1 không gọi được (kiểm tra kết nối trước khi chạy hàng loạt phát hiện lỗi): {pf["web1"][1]}')
            for c in extra_web1_net:records2[c]=err_rec2
            extra_web1_net=[]
    total1=len(codes_net)
    counters={'w2':0,'w1':0,'w1_total':len(extra_web1_net)}
    fresh1={};fresh2={}
    # Mỗi LUỒNG dùng 1 session RIÊNG trong từng pool — xem giải thích đầy đủ ở docstring
    # _SessionPool: đỡ phải mồi cookie lại mỗi mã, nhưng KHÔNG dồn hết traffic của cả đợt vào
    # đúng 1 cookie. Tự đổi cookie mới + thử lại 1 lần nếu gặp 403 (xem _fetch_with_403_recovery) —
    # chỉ áp dụng cho web 2 (retry_403=True); web 1 KHÔNG thử lại 403 (xem web1_task bên dưới).
    web2_pool=_SessionPool(prime=False)
    web1_pool=_SessionPool(prime=True)
    # web2 (nguồn chính): ngưỡng mặc định (đã chứng minh ổn định qua test thực tế — 6 luồng, 0 lỗi).
    # web1 (bổ sung): CHỈ 1 lỗi kết nối là dừng hẳn việc gọi thêm web1 cho phần còn lại của lượt
    # chạy này (WEB1_SWITCH_THRESHOLD=1, WEB1_SWITCH_COOLDOWN gần như vô hạn) — web1 vốn đã rất
    # mong manh (ngưỡng thật ~1 request/10s), nên hễ lỗi là dừng ngay, khỏi phí thêm request vào
    # 1 trang có khả năng đang chặn; các mã không rà được qua web1 giữ nguyên trạng thái "không
    # xác nhận được" theo dữ liệu web2 đã có.
    web2_breaker=_CircuitBreaker()
    web1_breaker=_CircuitBreaker(threshold=WEB1_SWITCH_THRESHOLD,cooldown=WEB1_SWITCH_COOLDOWN)
    def web2_task(code):
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        if requests is None:
            rec=PractitionerRecord(error='Thiếu thư viện requests/beautifulsoup4 — cần cài đặt để dùng tính năng tra cứu này.')
        else:
            try:
                rec=_fetch_with_403_recovery(web2_pool,lookup_thongtin,code,breaker=web2_breaker,log=log,site_label='web 2',retry_403=True)
            except Exception as e:
                rec=PractitionerRecord(error=f'Lỗi tra cứu web 2: {e}')
        return ('web2',code,rec)
    web1_delay=WEB1_FALLBACK_DELAY_SECONDS if web1_safe_pacing else SLEEP_BETWEEN_REQUESTS
    web1_workers=WEB1_FALLBACK_WORKERS if web1_safe_pacing else MAX_LOOKUP_WORKERS
    def web1_task(code):
        # Giống _fetch_parallel.worker: kiểm tra breaker TRƯỚC để bỏ qua time.sleep (vô nghĩa
        # khi không hề gọi mạng) khi đã dừng hẳn web 1 — xem giải thích đầy đủ ở đó. Nghỉ
        # `web1_delay` SAU MỖI request thật — WEB1_FALLBACK_DELAY_SECONDS=10.0s (an toàn, 1
        # luồng) nếu web1_safe_pacing=True, hoặc SLEEP_BETWEEN_REQUESTS=0.6s (tốc độ cũ, song
        # song MAX_LOOKUP_WORKERS luồng) nếu False (mặc định — lựa chọn ưu tiên tốc độ).
        if web1_breaker.blocked():
            return ('web1',code,PractitionerRecord(error='Đã dừng tra web 1 cho phần còn lại của lượt chạy này (vừa gặp lỗi kết nối trước đó).'))
        if requests is None:
            rec=PractitionerRecord(error='Thiếu thư viện requests/beautifulsoup4 — cần cài đặt để dùng tính năng tra cứu này.')
        else:
            try:
                rec=_fetch_with_403_recovery(web1_pool,lookup_tracuu,code,breaker=web1_breaker,log=log,site_label='web 1',retry_403=False)
            except Exception as e:
                rec=PractitionerRecord(error=f'Lỗi tra cứu web 1: {e}')
            time.sleep(web1_delay)
        return ('web1',code,rec)
    # 2 POOL RIÊNG: web 2 (luôn MAX_LOOKUP_WORKERS luồng, nguồn chính) và web 1 (web1_workers
    # luồng — 1 nếu bật pacing an toàn, MAX_LOOKUP_WORKERS nếu không) — không bao giờ dùng
    # chung 1 pool, vì tốc độ an toàn của 2 trang có thể rất khác nhau.
    with ThreadPoolExecutor(max_workers=MAX_LOOKUP_WORKERS) as ex1,\
         ThreadPoolExecutor(max_workers=web1_workers) as ex2:
        pending={ex1.submit(web2_task,c) for c in codes_net}
        pending|={ex2.submit(web1_task,c) for c in extra_web1_net}
        while pending:
            if cancelled():break  # không nộp thêm task mới — cả 2 pool tự rút dần khi thoát khối `with`
            done,pending=wait(pending,timeout=1.0,return_when=FIRST_COMPLETED)
            for fut in done:
                try:
                    kind,code,rec=fut.result()
                except Exception as e:
                    log(f'⚠ Lỗi tra cứu mã (bỏ qua, tiếp tục các mã khác): {e}')
                    continue
                if kind=='web2':
                    records1[code]=rec
                    fresh1[code]=rec
                    counters['w2']+=1
                    if counters['w2']%20==0 or counters['w2']==total1:
                        log(f'Đã tra web 2: {counters["w2"]}/{total1} mã...')
                    if facility_gphd and not cancelled() and code not in cached2 and code not in extra_web1_net:
                        dts=code_to_dts.get(code,[])
                        if any(verify_at_facility_detailed(rec,facility_gphd,dt)[0]!='x' for dt in dts):
                            pending.add(ex2.submit(web1_task,code))
                            counters['w1_total']+=1
                else:
                    records2[code]=rec
                    fresh2[code]=rec
                    counters['w1']+=1
                    log_step=5 if web1_safe_pacing else 20
                    if counters['w1']%log_step==0 or counters['w1']==counters['w1_total']:
                        note=' (đã dừng hẳn, không còn gọi mạng tới web 1 nữa)' if web1_breaker.blocked() else ''
                        pace_note=f'~{web1_delay:.0f}s/mã' if web1_safe_pacing else f'{web1_workers} luồng'
                        log(f'Đã tra web 1 (bổ sung — {pace_note}): {counters["w1"]}/{counters["w1_total"]} mã...{note}')
    if fresh1:save_disk_cached(fresh1,'web2')
    if fresh2:save_disk_cached(fresh2,'web1')
def _build_pairs(rule):
    pairs=[]
    if rule.pair1_enabled and rule.pair1_doctor_column and rule.pair1_time_column:
        pairs.append(('Cặp 1','C1',rule.pair1_doctor_column,rule.pair1_time_column))
    if rule.pair2_enabled and rule.pair2_doctor_column and rule.pair2_time_column:
        pairs.append(('Cặp 2','C2',rule.pair2_doctor_column,rule.pair2_time_column))
    if not pairs:
        raise ValueError('Chưa bật cặp cột nào để đối chiếu.')
    return pairs

def prefetch_staff_records(staff_dir_file,staff_dir_key_column,session=None,progress=None,cancelled=None,log=None,facility_gphd=''):
    """PHA 1 (làm TRƯỚC, không đụng .db): lấy dữ liệu hành nghề cho mọi mã CCHN trong file danh mục
    nhân viên theo thứ tự ưu tiên MỚI:
      1. Danh sách nhân sự THEO CƠ SỞ (web 1, theo `facility_gphd` đã nhập) — 1 lần lấy cả cơ sở.
      2. Mã trong danh mục nhân viên mà danh sách trên KHÔNG có -> tra lẻ web 1, đúng 1 mã/giây.
      3. Mã web 1 lỗi/không có -> web 2 (thongtin.medinet.org.vn), như trước.
    Nếu không nhập facility_gphd (hoặc không lấy được danh sách) thì mọi mã đi thẳng bước 2-3.
    Trả về (staff_keys, records1) — records1 {mã chuẩn hoá: PractitionerRecord} (gồm cả nhân sự
    của cơ sở không nằm trong danh mục nhân viên) dùng làm cache cho combine_with_dataframe."""
    cancelled=cancelled or (lambda:False)
    log=log or (lambda msg:None)
    staff_keys=load_staff_dir_keys(staff_dir_file,staff_dir_key_column) if staff_dir_file else set()
    codes=sorted(staff_keys)
    records1={}
    if facility_gphd:
        fac,err=fetch_facility_staff(facility_gphd,log=log,cancelled=cancelled)
        if err:log(f'⚠ {err} — chuyển sang tra lẻ từng mã.')
        records1.update(fac)
    missing=[c for c in codes if c not in records1]
    if missing and not cancelled():
        log(f'{len(missing)}/{len(codes)} mã trong danh mục nhân viên chưa có trong danh sách theo cơ sở — tra lẻ web 1 rồi web 2...')
        fill_records_chain(missing,records1,log=log,cancelled=cancelled)
    return staff_keys,records1

def combine_with_dataframe(df,rule,staff_keys,records1,records2=None,session=None,log=None,cancelled=None,track_details=True):
    """PHA 2: kết hợp cache đã tra sẵn (records1 = dữ liệu WEB 2/nguồn chính — xem
    prefetch_staff_records) với 1 DataFrame (CHỈ CẦN các cột tra cứu — không cần các cột
    khác, đây chính là truy vấn .db NHẸ nói ở trên) để tính đúng/sai theo TỪNG dòng+thời
    điểm cụ thể.

    Làm theo 2 bước con để tránh tra cứu TUẦN TỰ (rất chậm khi có hàng nghìn mã):
      1. Xây map mã -> mọi check_dt (1 lượt quét df duy nhất, không mạng). Với mã CHƯA có
         sẵn web 2 (vd không nằm trong danh mục nhân viên): tra web 2 song song (nguồn
         chính), và NGAY khi có kết quả 1 mã, nếu web 2 không xác nhận được (không có mã,
         sai nơi công tác, hoặc sai giờ/thứ) thì nộp NGAY task web 1 (bổ sung) cho mã đó —
         2 việc "tra web 2 mã khác" và "rà web 1 cho mã vừa xong" chạy CHỒNG LẤN nhau (xem
         _pipeline_fetch_web2_then_web1), không đợi xong hết web 2 mới bắt đầu web 1. Với
         mã ĐÃ có sẵn web 2 (từ danh mục nhân viên): biết trước toàn bộ danh sách cần rà
         web 1 ngay lập tức. QUAN TRỌNG: web 1 luôn chạy TUẦN TỰ (1 luồng, 10s/request — xem
         WEB1_FALLBACK_WORKERS/WEB1_FALLBACK_DELAY_SECONDS), không song song như web 2.
      2. Quét lại df LẦN NỮA để build từng dòng kết quả — lúc này records1/records2 đã có
         sẵn đầy đủ, không còn request mạng nào trong vòng lặp này nữa (nhanh, thuần CPU).
    track_details=False bỏ qua việc gom nhóm theo lý do (dùng cho Preview, chỉ cần bảng tổng
    hợp, không cần dựng lại sheet chi tiết) — nhanh hơn 1 chút và không giữ index gốc.
    Trả về (summary_df, detail_sheets, records1, records2) — 2 cache trả về LẠI để bên gọi
    tái sử dụng cho lần combine tiếp theo (vd Export sau khi đã Preview, khỏi tra mạng lại
    những mã đã biết)."""
    log=log or (lambda msg:None)
    cancelled=cancelled or (lambda:False)
    if records2 is None:records2={}
    pairs=_build_pairs(rule)
    # Dự phòng cho cột "Mã người hành nghề" khi web không tra được — xem load_staff_dir_raw_map.
    # Đọc lại nhẹ (chỉ 1 cột) nên không đáng kể so với phần tra mạng; lỗi đọc file (vd file bị
    # xoá/đổi tên giữa chừng) không được làm hỏng cả lượt combine — bỏ qua, dùng dict rỗng, vẫn
    # còn nấc dự phòng cuối (.db gốc) nên cột này KHÔNG BAO GIỜ để trống.
    staff_raw_map={}
    staff_chucdanh_map={}
    if rule.staff_dir_file:
        try:
            staff_raw_map=load_staff_dir_raw_map(rule.staff_dir_file,rule.staff_dir_key_column)
        except Exception as e:
            log(f'⚠ Không đọc lại được file danh mục nhân viên để dự phòng cột "Mã người hành nghề": {e}')
        try:
            staff_chucdanh_map=load_staff_dir_value_map(rule.staff_dir_file,rule.staff_dir_key_column,'CHUCDANH_NN')
        except Exception as e:
            log(f'⚠ Không đọc được cột CHUCDANH_NN từ file danh mục nhân viên cho cột "Chức danh đăng ký BHYT": {e}')

    # Tính TRƯỚC 1 LẦN DUY NHẤT (vector hoá — nhanh hơn RẤT NHIỀU so với gọi lặp lại theo
    # từng dòng) mã đã chuẩn hoá + thời gian đã parse cho MỖI cặp cột — dùng lại y nguyên
    # cho cả bước xây code_to_dts LẪN bước dựng từng dòng kết quả bên dưới (Bước 2), thay vì
    # gọi lại pd.to_datetime()/_normalize_code() theo từng Ô một (rất chậm khi có hàng triệu
    # dòng — đây chính là chỗ "im lặng không log" sau khi tra cứu mạng xong, dễ tưởng nhầm
    # là treo trong khi thực ra vẫn đang chạy, chỉ là chạy rất chậm).
    pair_codes={}
    pair_dts={}
    for pi,(_,_,doc_col,time_col) in enumerate(pairs):
        if doc_col not in df.columns or time_col not in df.columns:continue
        _col=df[doc_col];_cache={}
        def _sp(v,_cache=_cache):
            k=v if isinstance(v,str) else repr(v)
            r=_cache.get(k)
            if r is None:r=_cache[k]=tuple(split_doctor_codes(v))
            return r
        pair_codes[pi]=pd.Series([_sp(v) for v in _col.to_numpy(dtype=object)],dtype=object).to_numpy()
        pair_dts[pi]=pd.to_datetime(df[time_col],errors='coerce').to_numpy()

    # Xây map mã -> danh sách mọi check_dt mã đó xuất hiện cùng trong df, trong 1 lượt quét
    # DUY NHẤT (không mạng) — dùng để: (a) biết mã nào cần tra web 2 (nguồn chính), và (b)
    # ngay khi có kết quả web 2 của 1 mã, biết NGAY mã đó có cần rà thêm web 1 (bổ sung) hay
    # không mà không cần quét lại df lần thứ 2.
    code_to_dts={}
    for pi in pair_codes:
        codes_arr=pair_codes[pi]
        dts_arr=pair_dts[pi]
        for items,dt in zip(codes_arr,dts_arr):
            if not items or pd.isna(dt):continue
            _d=pd.Timestamp(dt).to_pydatetime()
            for code,_ in items:
                code_to_dts.setdefault(code,[]).append(_d)

    # Thứ tự nguồn (mới): danh sách nhân sự theo cơ sở (đã nạp ở prefetch_staff_records, hoặc nạp ở
    # đây nếu chưa có) -> mã thiếu/chưa xác nhận được: tra lẻ web 1 (1 mã/giây) -> lỗi: web 2.
    if rule.facility_gphd and not cancelled() and not any(str(getattr(r,'source','')).startswith('tracuu_cs') for r in records1.values()):
        fac,err=fetch_facility_staff(rule.facility_gphd,log=log,cancelled=cancelled)
        if err:log(f'⚠ {err} — chuyển sang tra lẻ từng mã.')
        for c,r in fac.items():
            if c not in records1:records1[c]=r
    if rule.facility_gphd and not cancelled():resolve_facility_hours(rule.facility_gphd,log=log)
    missing1=sorted(c for c in code_to_dts if c not in records1)
    recheck=[]
    if rule.facility_gphd:
        for code,dts in code_to_dts.items():
            rec=records1.get(code)
            if rec is None or getattr(rec,'source','')!='tracuu_cs':continue  # 'tracuu_cs_checked' = đã tra lẻ rồi, không tra lại
            if _needs_full_history(rec,rule.facility_gphd,dts):
                recheck.append(code)
        recheck=sorted(recheck)
    todo=missing1+recheck
    if todo and not cancelled():
        if missing1 and not _OFFLINE:log(f'Có {len(missing1)} mã chưa có dữ liệu (không có trong danh sách theo cơ sở/danh mục nhân viên) — tra lẻ web 1 (1 mã/giây), lỗi thì web 2...')
        if recheck and not _OFFLINE:log(f'Có {len(recheck)} mã trong danh sách theo cơ sở chưa xác nhận được ở thời điểm đang xét (danh sách chỉ ghi giai đoạn hiện hành) — tra lẻ web 1 để lấy đủ lịch sử, lỗi thì web 2...')
        fill_records_chain(todo,records1,log=log,cancelled=cancelled)
        for c in recheck:
            r=records1.get(c)
            if r is not None and r.source=='tracuu_cs':r.source='tracuu_cs_checked'  # đã thử tra lẻ nhưng không thêm được gì

    # Bước 2: build kết quả — GỘP theo (cặp, mã, loại lỗi): mỗi mã vi phạm ra ĐÚNG 1 dòng
    # trong KET_QUA cho MỖI loại lỗi mà nó vướng (dù mã đó xuất hiện ở hàng nghìn dòng .db
    # gốc), với THANH_TIEN/T_BHTT_DTL là TỔNG cộng dồn, Từ ngày/Đến ngày là ngày sớm
    # nhất/muộn nhất vi phạm — trên mọi dòng .db gốc thuộc đúng mã+loại lỗi đó. "Không đăng
    # ký với BHYT" và các loại lỗi phía SYT (sai giờ/sai thứ/không đăng ký SYT) được kiểm
    # tra ĐỘC LẬP với nhau (không phải if/elif chọn đúng 1 lý do) — 1 mã có thể vướng ĐỒNG
    # THỜI cả 2 phía, khi đó ra 2 dòng riêng (đánh dấu 'bold'=True để bên xuất Excel tô đậm
    # cả 2 dòng đó, xem export_medinet_verification ở data.py). records1/records2 đã đủ,
    # không còn request mạng nào trong vòng lặp này nữa.
    groups={}       # (pi,code,cat) -> {'code_raw','wp','disp_record','sum_thanh_tien','sum_tbhtt','min_dt','max_dt','thu_set'}
    both_sides={}   # (pi,code) -> {'bhyt':bool,'syt':bool} — để biết dòng nào cần tô đậm (vướng CẢ 2 phía)
    detail_rows={}  # sheet_name -> list of {'idx','NGAY_LAM_VIEC','NGAY_NGHI_VIEC','THOI_GIAN_LAM_VIEC','check_dt','thu','reason_value'} (track_details=True only)
    error_record=PractitionerRecord(error='OFFLINE: mã không có trong bản lưu danh sách nhân sự (không tra cứu mạng).' if _OFFLINE else 'Thiếu thư viện requests/beautifulsoup4 — cần cài đặt để dùng tính năng tra cứu này.')
    has_thanh_tien='THANH_TIEN' in df.columns
    has_tbhtt='T_BHTT_DTL' in df.columns
    n_rows=len(df)
    log_every=max(50000,n_rows//20) if n_rows>100000 else None  # chỉ log tiến trình khi df đủ lớn để đáng lo "tưởng treo"

    # --- Tối ưu tốc độ, KHÔNG đổi logic/kết quả (bảo toàn dữ liệu là ưu tiên cao nhất) -------
    # 1) Bỏ hẳn df.iterrows() — đây chính là chỗ CHẬM đã tự ghi chú ở trên (mỗi lần iterrows()
    #    phải dựng lại 1 pandas Series/dòng, ép kiểu dữ liệu — rất tốn với hàng triệu dòng).
    #    Thay bằng đọc thẳng từng mảng numpy đã trích sẵn theo VỊ TRÍ (pos) — cho ĐÚNG y hệt
    #    giá trị gốc, chỉ khác cách truy cập, không có phép biến đổi/toán học nào khác nên
    #    không thể lệch kết quả so với bản dùng iterrows().
    # 2) Nhớ lại (memo hoá) kết quả verify_at_facility_detailed()/find_facility_workplace()
    #    theo đúng (mã, thời điểm) — 2 hàm này là HÀM THUẦN (cùng đầu vào luôn ra cùng đầu ra,
    #    records1/records2/rule.facility_gphd không đổi trong suốt 1 lượt combine này), nên
    #    nhớ lại tuyệt đối KHÔNG làm sai kết quả, chỉ tránh tính lại khi cùng 1 mã bác sĩ có
    #    NHIỀU dòng chi phí (MA_CP) cùng chung 1 thời điểm y lệnh (NGAY_YL) — rất phổ biến với
    #    dữ liệu XML1 (1 lượt khám ra nhiều dòng thuốc/vật tư/dịch vụ, cùng chung NGAY_YL).
    #    Cache khai báo LẠI TỪ ĐẦU mỗi lần gọi combine_with_dataframe (không giữ qua lần gọi
    #    khác) để không bao giờ dùng nhầm cache cũ khi records1/records2 đã đổi.
    _verify_memo={}
    def _verify_memo_get(which,records,code,check_dt):
        mk=(which,code,check_dt)
        v=_verify_memo.get(mk)
        if v is None:
            record=records.get(code,error_record)
            v=verify_at_facility_detailed(record,rule.facility_gphd,check_dt) if rule.facility_gphd else ('no_match','','')
            _verify_memo[mk]=v
        return v
    _wp_memo={}
    def _wp_memo_get(disp_record,check_dt):
        wk=(id(disp_record),check_dt)
        if wk not in _wp_memo:
            _wp_memo[wk]=find_facility_workplace(disp_record,rule.facility_gphd,check_dt) if rule.facility_gphd else None
        return _wp_memo[wk]

    idx_arr=df.index.to_numpy()
    doc_raw_arrs={pi:df[doc_col].to_numpy(dtype=object) for pi,(_,_,doc_col,_) in enumerate(pairs) if pi in pair_codes}
    bsth_arr=df['MA_BSI_THUC_HIEN'].to_numpy(dtype=object) if 'MA_BSI_THUC_HIEN' in df.columns else None
    thanh_tien_arr=df['THANH_TIEN'].to_numpy() if has_thanh_tien else None
    tbhtt_arr=df['T_BHTT_DTL'].to_numpy() if has_tbhtt else None

    for pos in range(n_rows):
        if cancelled():break
        if log_every and pos>0 and pos%log_every==0:
            log(f'Đang dựng kết quả: {pos:,}/{n_rows:,} dòng ({100*pos//n_rows}%)...')
        idx=idx_arr[pos]
        for pi,(pair_label,pair_short,doc_col,time_col) in enumerate(pairs):
            if pi not in pair_codes:continue
            items=pair_codes[pi][pos]
            if not items:continue
            dt_raw=pair_dts[pi][pos]
            if pd.isna(dt_raw):continue
            # Cột 'MA_BSI_THUC_HIEN' ở KET_QUA: CHỈ ghi khi chính cột MA_BSI_THUC_HIEN (của cặp này) vi phạm; cặp khác -> để trống.
            cell_raw=bsth_arr[pos] if (bsth_arr is not None and doc_col=='MA_BSI_THUC_HIEN') else None
            check_dt=pd.Timestamp(dt_raw).to_pydatetime()
            # MỖI mã trong ô (tách bởi ';') được tra ĐỘC LẬP; mã nào vi phạm thì cả dòng vi phạm,
            # các mã vi phạm CÙNG loại lỗi trong 1 dòng được gom lại thành 1 dòng KET_QUA (ngăn ';').
            viol={}   # cat -> list of (code,code_raw,disp_record,wp)
            for code,code_raw in items:
                # records1 = kết quả web 2 (nguồn chính), records2 = kết quả web 1 (bổ sung).
                status1,gio1,thu1=_verify_memo_get('1',records1,code,check_dt)
                record1=records1.get(code,error_record)
                confirmed=(status1=='x')
                disp_record,disp_status,disp_gio,disp_thu=record1,status1,gio1,thu1
                if not confirmed and rule.facility_gphd and code in records2:
                    status2,gio2,thu2=_verify_memo_get('2',records2,code,check_dt)
                    if status2=='x':
                        confirmed=True
                    elif not record1.found:
                        record2=records2[code]
                        disp_record,disp_status,disp_gio,disp_thu=record2,status2,gio2,thu2
                in_staff=code in staff_keys
                cats=[]
                if staff_keys and not in_staff:cats.append('ngoai_bhyt')   # không có file nhân viên => không xét loại này (đã có 'Không đăng ký với SYT')
                if not confirmed:
                    if disp_status=='wrong_time':cats.append('sai_gio')
                    elif disp_status=='wrong_day':cats.append('sai_thu')
                    elif _net_error(record1) or (code in records2 and _net_error(records2[code])):cats.append('loi_mang')   # không tra được vì lỗi mạng => KHÔNG kết luận "không đăng ký"
                    else:cats.append('khong_dk_syt')
                if not cats:continue
                wp=_wp_memo_get(disp_record,check_dt)
                bs=both_sides.setdefault((pi,code),{'bhyt':False,'syt':False})
                if 'ngoai_bhyt' in cats:bs['bhyt']=True
                if any(c in ('sai_gio','sai_thu','khong_dk_syt') for c in cats):bs['syt']=True
                for cat in cats:
                    rv=disp_gio if cat=='sai_gio' else (disp_thu if cat=='sai_thu' else None)
                    viol.setdefault(cat,[]).append((code,code_raw,disp_record,wp,rv,disp_thu))
            for cat,plist in viol.items():
                key=(pi,cat,tuple(x[0] for x in plist))
                g=groups.get(key)
                if g is None:
                    g={'persons':[{'code':x[0],'code_raw':x[1],'wp':x[3],'disp_record':x[2],'thu_set':set(),
                                   'note':_build_note(cat,x[2],rule.facility_gphd,check_dt)} for x in plist],
                       'raws':[],'sum_thanh_tien':0.0,'sum_tbhtt':0.0,'min_dt':check_dt,'max_dt':check_dt}
                    groups[key]=g
                else:
                    if check_dt<g['min_dt']:g['min_dt']=check_dt
                    if check_dt>g['max_dt']:g['max_dt']=check_dt
                rs='' if (cell_raw is None or (not isinstance(cell_raw,str) and pd.isna(cell_raw))) else str(cell_raw).strip()
                if rs and rs not in g['raws']:g['raws'].append(rs)
                if has_thanh_tien:g['sum_thanh_tien']+=_parse_vn_number(thanh_tien_arr[pos])
                if has_tbhtt:g['sum_tbhtt']+=_parse_vn_number(tbhtt_arr[pos])
                if cat=='sai_thu':
                    for per,x in zip(g['persons'],plist):
                        if x[5]:per['thu_set'].add(x[5])
                if track_details:
                    sheet_name=REASON_SHEET[cat]
                    rvs=[x[4] for x in plist if x[4]]
                    detail_rows.setdefault(sheet_name,[]).append({
                        'idx':idx,
                        'NGAY_LAM_VIEC':'; '.join((x[3].ngay_lam if x[3] else '') for x in plist) if len(plist)>1 else (plist[0][3].ngay_lam if plist[0][3] else ''),
                        'NGAY_NGHI_VIEC':'; '.join((x[3].ngay_nghi if x[3] else '') for x in plist) if len(plist)>1 else (plist[0][3].ngay_nghi if plist[0][3] else ''),
                        'THOI_GIAN_LAM_VIEC':'; '.join((x[3].thoi_gian if x[3] else '') for x in plist) if len(plist)>1 else (plist[0][3].thoi_gian if plist[0][3] else ''),
                        'reason_value':('; '.join(rvs) if len(plist)>1 else plist[0][4])})

    def _vai_tro(pi):
        # CHỈ tên vai trò — bỏ phần giải thích trong ngoặc (đó là phần thuyết minh, không
        # phải nội dung dữ liệu; người dùng đã yêu cầu bỏ phần này khỏi cột).
        return 'Người chỉ định' if pi==0 else 'Người thực hiện'
    def _ma_hanh_nghe(disp_record,code,code_raw):
        # Thứ tự ưu tiên (KHÔNG BAO GIỜ để trống ô này):
        #   1. Mã do CHÍNH cổng SYT trả về, nếu tra được — đáng tin nhất, đúng định dạng
        #      chuẩn của cổng.
        #   2. Web không tra được (lỗi hoặc không tìm thấy) -> mã đã đăng ký trong file danh
        #      mục nhân viên nội bộ, nếu có (staff_raw_map — xem load_staff_dir_raw_map).
        #   3. Không có trong file nhân viên (hoặc không cấu hình file này) -> mã thô lấy
        #      thẳng từ .db gốc, như trước đây.
        if disp_record.found and disp_record.cchn:
            return disp_record.cchn
        raw_staff=staff_raw_map.get(code)
        if raw_staff:
            return raw_staff
        s=str(code_raw).strip() if code_raw is not None else ''
        return s if s else code
    def _explain(cat,P):
        """Cột 'Giải thích' = giải thích cố định của loại lỗi + chi tiết riêng từng mã (trước đây tách ở 'Giải thích lỗi' và 'Ghi chú')."""
        base=REASON_EXPLAIN[cat]
        if cat=='ngoai_bhyt':return base[:1].upper()+base[1:]
        notes=[]
        for p in P:
            n=str(p.get('note') or '').strip()
            if n and n not in notes:notes.append(n)
        return base+('. '+' '.join(notes) if notes else '')
    summary_rows=[]
    def _j(vals):
        if not any(str(v).strip() for v in vals):return ''
        return '; '.join(vals) if len(vals)>1 else vals[0]
    for (pi,cat,_codes),g in groups.items():
        P=g['persons']
        bold=any(both_sides.get((pi,p['code']),{}).get('bhyt') and both_sides.get((pi,p['code']),{}).get('syt') for p in P)
        out={
            'Lý do':REASON_LABELS[cat],
            'Giải thích':_explain(cat,P),
            'Vai trò':_vai_tro(pi),
            'Chức danh đăng ký BHYT':_j([_chuc_danh_bhyt_label(staff_chucdanh_map.get(p['code'])) for p in P]),
            'Mã người hành nghề':_j([_ma_hanh_nghe(p['disp_record'],p['code'],p['code_raw']) for p in P]),
            'Họ tên trên cổng':_j([p['disp_record'].name if p['disp_record'].found else '' for p in P]),
            'Lịch đăng ký trên cổng':_j([p['wp'].thoi_gian if p['wp'] else '' for p in P]),
            'Ngày bắt đầu làm việc (cổng)':_j([p['wp'].ngay_lam if p['wp'] else '' for p in P]),
            'Ngày nghỉ việc (cổng)':_j([p['wp'].ngay_nghi if p['wp'] else '' for p in P]),
            'Các thứ ghi nhận':_j([', '.join(sorted(p['thu_set'],key=lambda t:_WEEKDAY_MAP.get(t,99))) for p in P]),
            'Từ ngày':g['min_dt'].strftime('%d/%m/%Y'),
            'Đến ngày':g['max_dt'].strftime('%d/%m/%Y'),
            'Thành tiền':g['sum_thanh_tien'],
            'Tiền BHYT đã thanh toán (đ)':g['sum_tbhtt'],
            'MA_BSI_THUC_HIEN':' | '.join(g['raws']),
            'Giải trình':'',
            '_BOLD':bold,
        }
        summary_rows.append(out)
    summary_cols=['STT','Lý do','Giải thích','Vai trò','Chức danh đăng ký BHYT','Mã người hành nghề','Họ tên trên cổng',
                  'Lịch đăng ký trên cổng','Ngày bắt đầu làm việc (cổng)','Ngày nghỉ việc (cổng)',
                  'Các thứ ghi nhận','Từ ngày','Đến ngày','Thành tiền','Tiền BHYT đã thanh toán (đ)',
                  'MA_BSI_THUC_HIEN','Giải trình','_BOLD']
    if summary_rows:
        summary_df=pd.DataFrame(summary_rows)
        summary_df.insert(0,'STT',range(1,len(summary_df)+1))
        summary_df=summary_df[summary_cols]
    else:
        summary_df=pd.DataFrame(columns=summary_cols)
    detail_sheets={}
    if track_details:
        for sheet_name,rows_info in detail_rows.items():
            idxs=[r['idx'] for r in rows_info]
            base_cols=[c for c in DETAIL_BASE_COLUMNS if c in df.columns]
            sub=df.loc[idxs,base_cols].reset_index(drop=True) if base_cols else pd.DataFrame(index=range(len(idxs)))
            sub['NGAY_LAM_VIEC']=[r['NGAY_LAM_VIEC'] for r in rows_info]
            sub['NGAY_NGHI_VIEC']=[r['NGAY_NGHI_VIEC'] for r in rows_info]
            sub['THOI_GIAN_LAM_VIEC']=[r['THOI_GIAN_LAM_VIEC'] for r in rows_info]
            cat_of=[k for k,v in REASON_SHEET.items() if v==sheet_name][0]
            expl=REASON_EXPLAIN[cat_of][:1].upper()+REASON_EXPLAIN[cat_of][1:]
            if sheet_name in (REASON_LABELS['sai_gio'],REASON_LABELS['sai_thu']):
                gt=[r['reason_value'] for r in rows_info]
                lab='giờ' if cat_of=='sai_gio' else 'thứ'
                sub.insert(0,'Giải thích',[(f'{expl}. Giá trị {lab} ghi nhận: {v}' if v else expl) for v in gt])
            else:
                sub.insert(0,'Giải thích',expl)
            sub.insert(0,'Lý do',REASON_LABELS[cat_of])
            detail_sheets[sheet_name]=sub
            log(f'Sheet "{sheet_name}": {len(sub)} dòng.')
    return summary_df,detail_sheets,records1,records2


def run_verification(df,rule,progress=None,cancelled=None,log=None):
    """Tiện ích 1-lần-gọi cho tương thích ngược / dùng độc lập: tự lấy mã cần tra TỪ CHÍNH df
    (không phải từ danh mục nhân viên trước) rồi gọi combine_with_dataframe với cache rỗng —
    hàm đó tự tra song song mọi mã còn thiếu (xem _fetch_parallel). Luồng CHÍNH của app (Áp
    dụng bộ lọc -> Preview -> Export) KHÔNG dùng hàm này nữa — xem prefetch_staff_records +
    combine_with_dataframe, tách pha để làm xong việc tra mạng (theo danh mục nhân viên,
    không phụ thuộc .db) TRƯỚC khi đụng đến .db (có thể rất lớn, chỉ cần truy vấn nhẹ vài
    cột sau khi đã biết cần tra ai).
    Trả về (summary_df, detail_sheets) — chữ ký CŨ, bỏ 2 cache ở cuối để không phá code gọi
    hàm này từ trước (nếu còn)."""
    log=log or (lambda msg:None)
    cancelled=cancelled or (lambda:False)
    staff_keys=load_staff_dir_keys(rule.staff_dir_file,rule.staff_dir_key_column) if rule.staff_dir_file else set()
    summary_df,detail_sheets,_,_=combine_with_dataframe(df,rule,staff_keys,{},log=log,cancelled=cancelled,track_details=True)
    return summary_df,detail_sheets
