"""Đối chiếu FILE DANH MỤC NHÂN VIÊN do cơ sở tự khai (vd DM_NVYT_IMPORT) với danh sách nhân sự trên cổng SYT (mục F) — KHÔNG cần dữ liệu .db.
Kết quả: chính file của đơn vị (giữ nguyên bố cục/định dạng) + các cột SYT & KET_QUA_DOI_CHIEU thêm vào SAU dữ liệu; ô nào sai tô đỏ.
Thêm sheet "SYT co - DM thieu" (người SYT đang ghi làm tại cơ sở mà danh mục không có) và "Tong hop".
Không phụ thuộc Qt. File đầu ra luôn là file MỚI (không ghi đè)."""
import re
import unicodedata
from datetime import datetime, date
from pathlib import Path

RED='FFFF9999'


def _nd(s):
    s=unicodedata.normalize('NFD',str(s or '')).replace('đ','d').replace('Đ','D')
    s=''.join(c for c in s if unicodedata.category(c)!='Mn')
    return re.sub(r'\s+',' ',s.lower().replace('\xa0',' ')).strip()


def _head(s):
    return re.sub(r'[^a-z0-9]+','_',_nd(s)).strip('_')


def _parse_date(v):
    if v is None or v=='':return None
    if isinstance(v,datetime):return v.date()
    if isinstance(v,date):return v
    t=str(v).strip()
    if re.fullmatch(r'\d{8}',t):
        try:return datetime.strptime(t,'%Y%m%d').date()
        except ValueError:return None
    for f in ('%d/%m/%Y','%Y-%m-%d','%d-%m-%Y','%Y-%m-%d %H:%M:%S'):
        try:return datetime.strptime(t,f).date()
        except ValueError:pass
    return None


def _fmt(d):
    return d.strftime('%d/%m/%Y') if d else ''


def _dir_days(txt):
    from medinet_lookup import _WEEKDAY_MAP
    out=[]
    for m in re.findall(r'T[2-7]|CN',str(txt or '').upper()):
        d=_WEEKDAY_MAP[m]
        if d not in out:out.append(d)
    return out


def _dir_window(txt):
    from datetime import time as dtime
    t=str(txt or '').strip()
    m=re.fullmatch(r'(\d{2})(\d{2})\s*-\s*(\d{2})(\d{2})',t)
    if m:
        a,b,c,d=map(int,m.groups())
    else:
        m=re.search(r'(\d{1,2})[h:](\d{2})?\s*(?:-|–|đến|->)\s*(\d{1,2})[h:](\d{2})?',t)
        if not m:return None
        a,b,c,d=int(m.group(1)),int(m.group(2) or 0),int(m.group(3)),int(m.group(4) or 0)
    if a>24 or c>24 or b>59 or d>59:return None
    return (dtime(a%24,b),dtime(c%24 if c!=24 else 23,d if c!=24 else 59))


def _scope_terms(txt):
    t=_nd(txt)
    t=re.sub(r'kham benh|chua benh|chuyen khoa|chuyen mon','|',t)
    return [x.strip(' ,;.-') for x in re.split(r'[|,;]+',t) if x.strip(' ,;.-')]


def run(xlsx_path,key_col,gphd='',snapshot='',fresh=False,out_path=None,log=None,cancelled=None):
    """Trả (đường dẫn file kết quả, dict tóm tắt, lỗi). snapshot: bản lưu chọn sẵn => OFFLINE; trống => tự dùng/quét (fresh=True: quét mới trước)."""
    import openpyxl
    from openpyxl.styles import PatternFill, Font
    from openpyxl.utils import get_column_letter
    import medinet_lookup as ML
    import syt_store as SS
    log=log or (lambda m:None)
    cancelled=cancelled or (lambda:False)
    gphd=str(gphd or '').strip()
    # ---- cấu hình nguồn SYT
    class _R:pass
    r=_R();r.facility_gphd=gphd;r.facility_snapshot=str(snapshot or '')
    if not r.facility_snapshot and fresh:
        if not gphd:return '',{},'Tra cứu mới nhất cần nhập GPHĐ của cơ sở.'
        log(f'Tra cứu mới nhất: quét lại danh sách nhân sự cơ sở {gphd} từ cổng...')
        path,err=SS.scan(gphd,log=log,cancelled=cancelled)
        if err:return '',{},'Không quét được từ cổng: '+err
    if not r.facility_snapshot and not gphd:return '',{},'Chưa nhập GPHĐ và cũng chưa chọn bản lưu danh sách nhân sự.'
    snap=ML.configure_from_rule(r,log=log)
    gphd=str(r.facility_gphd or gphd).strip()
    offline=ML._OFFLINE
    if snap:
        try:
            meta,recs=SS.load_snapshot(snap)
        except Exception as e:
            return '',{},f'Không đọc được bản lưu: {e}'
    else:
        recs,err=ML.fetch_facility_staff(gphd,log=log,cancelled=cancelled)
        if err:return '',{},err
        meta={'gphd':gphd,'fetched_at':datetime.now().isoformat(timespec='seconds')}
    ML.resolve_facility_hours(gphd,log=log)
    gkey=ML._normalize_code(gphd)
    # ---- đọc file danh mục
    try:
        wb=openpyxl.load_workbook(xlsx_path)
    except Exception as e:
        return '',{},f'Không mở được file danh mục: {e}'
    ws=None;hrow=1;cols={}
    for cand in wb.worksheets:
        for rr in range(1,6):
            heads={_head(c.value):c.column for c in cand[rr] if c.value is not None}
            if _head(key_col) in heads:
                ws,hrow,cols=cand,rr,heads;break
        if ws:break
    if ws is None:return '',{},f'Không tìm thấy cột "{key_col}" trong file danh mục.'
    def col(*names):
        for n in names:
            if _head(n) in cols:return cols[_head(n)]
        return None
    c_key=cols[_head(key_col)]
    c_name,c_scope,c_pos=col('HO_TEN'),col('TEN_PHAMVI_CM','PHAM_VI'),col('TEN_VI_TRI','VI_TRI')
    c_hr,c_wk,c_from=col('THOIGIAN_NGAY'),col('THOIGIAN_TUAN'),col('TU_NGAY')
    # ---- mã chưa có trong danh sách theo cơ sở: tra lẻ (chỉ khi online)
    rows=[]
    for rr in range(hrow+1,ws.max_row+1):
        raw=ws.cell(rr,c_key).value
        if raw is None or str(raw).strip()=='':continue
        rows.append((rr,str(raw).strip(),ML._normalize_code(raw)))
    missing=sorted({c for _,_,c in rows if c and c not in recs})
    if missing and not offline and not cancelled():
        log(f'Có {len(missing)} mã không nằm trong danh sách theo cơ sở — tra lại từng mã trên web...')
        ML.fill_records_chain(missing,recs,log=log,cancelled=cancelled)
    today=datetime.now()
    today_d=today.date()
    # ---- cột thêm vào sau dữ liệu
    base=ws.max_column
    new_heads=['SYT_HO_TEN','SYT_PHAM_VI','SYT_VI_TRI','SYT_NGAY_LAM','SYT_NGAY_NGHI','SYT_LICH','KET_QUA_DOI_CHIEU']
    hdr_src=ws.cell(hrow,base)
    for i,h in enumerate(new_heads,1):
        c=ws.cell(hrow,base+i,h)
        c.font=Font(bold=True)
        if hdr_src.has_style:
            c._style=hdr_src._style
            c.font=Font(bold=True)
    red=PatternFill('solid',start_color=RED,end_color=RED)
    stats={'tong':0,'khop':0,'khong_co_syt':0,'co_loi':0}
    loi_count={}
    seen_codes=set()
    for rr,raw,code in rows:
        if cancelled():return '',{},'Đã hủy.'
        stats['tong']+=1
        seen_codes.add(code)
        rec=recs.get(code)
        wps=[w for w in (rec.workplaces if rec else []) if ML._normalize_code(w.gphd)==gkey] if rec and rec.found else []
        issues=[];bad_cells=set()
        def bad(cell,msg,key):
            issues.append(msg);loi_count[key]=loi_count.get(key,0)+1
            if cell:bad_cells.add(cell)
        if not rec or not rec.found or not wps:
            stats['khong_co_syt']+=1
            msg='Không tìm thấy mã này trên SYT' if not rec or not rec.found else 'Có mã nhưng không có nơi công tác tại cơ sở này'
            bad(c_key,msg,'Không có trên SYT/cơ sở')
            ref=[]
        else:
            def period(w):
                return _parse_date(w.ngay_lam),_parse_date(w.ngay_nghi)
            def contains(w,d):
                s,e=period(w)
                return not ((s and d<s) or (e and d>e))
            active=[w for w in wps if contains(w,today_d)]
            start=_parse_date(ws.cell(rr,c_from).value) if c_from else None
            # ngày bắt đầu phải nằm trong thời gian đăng ký trên SYT
            if c_from and start is None and ws.cell(rr,c_from).value not in (None,''):
                bad(c_from,'Ngày bắt đầu không đọc được','Ngày bắt đầu')
            elif start is not None and not any(contains(w,start) for w in wps):
                firsts=[period(w)[0] for w in wps if period(w)[0]]
                lasts=[period(w)[1] for w in wps if period(w)[1]]
                if firsts and start<min(firsts):bad(c_from,f'Ngày bắt đầu {_fmt(start)} trước ngày đăng ký SYT ({_fmt(min(firsts))})','Ngày bắt đầu')
                elif lasts and start>max(lasts):bad(c_from,f'Ngày bắt đầu {_fmt(start)} sau ngày nghỉ trên SYT ({_fmt(max(lasts))})','Ngày bắt đầu')
                else:bad(c_from,f'Ngày bắt đầu {_fmt(start)} không nằm trong giai đoạn nào trên SYT','Ngày bắt đầu')
            if not active:
                lasts=[period(w)[1] for w in wps if period(w)[1]]
                bad(c_from or c_key,'SYT ghi đã nghỉ việc'+(f' từ {_fmt(max(lasts))}' if lasts else '')+' — hiện không còn làm tại cơ sở','Đã nghỉ trên SYT')
            ref=active or ([w for w in wps if start and contains(w,start)]) or wps
            if c_name and rec.name and _nd(ws.cell(rr,c_name).value)!=_nd(rec.name):
                bad(c_name,f'Họ tên khác SYT ("{rec.name}")','Họ tên')
            if c_scope:
                dscope=ws.cell(rr,c_scope).value
                sscope=_nd(rec.scope)
                if dscope and sscope:
                    miss=[t for t in _scope_terms(dscope) if t not in sscope]
                    if miss:bad(c_scope,'Phạm vi chuyên môn không có trên SYT: '+', '.join(miss),'Phạm vi chuyên môn')
            if c_pos:
                dpos=_nd(ws.cell(rr,c_pos).value)
                spos=' | '.join(_nd(x) for x in [rec.position]+[w.position for w in ref] if x)
                if dpos and spos and dpos not in spos:
                    bad(c_pos,'Vị trí không khớp SYT ("'+(ref[0].position if ref and ref[0].position else rec.position)+'")','Vị trí')
            # lịch: từng thứ trong danh mục phải có trong lịch SYT, giờ nằm trong khung SYT
            ddays=_dir_days(ws.cell(rr,c_wk).value) if c_wk else []
            dwin=_dir_window(ws.cell(rr,c_hr).value) if c_hr else None
            if ddays or dwin:
                sched=[]
                for w in ref:
                    sched.append(ML.parse_schedule_ex(w.thoi_gian))
                if sched and any(sched):
                    lab={v:k for k,v in ML._WEEKDAY_MAP.items()}
                    no_day=[];no_hr=[]
                    for d in ddays or sorted({k for s in sched for k in s}):
                        wins=[ML._resolve_window(x,gphd,d) for s in sched for x in s.get(d,[])]
                        if not wins:no_day.append(lab[d]);continue
                        if dwin and not any(a<=dwin[0] and dwin[1]<=b for a,b in wins):no_hr.append(lab[d])
                    if no_day:bad(c_wk,'Thứ không có trong lịch SYT: '+', '.join(no_day),'Thứ làm việc')
                    if no_hr:bad(c_hr,'Giờ làm nằm ngoài khung SYT ở: '+', '.join(no_hr),'Giờ làm việc')
                else:
                    issues.append('SYT không ghi lịch — chưa đối chiếu được giờ/thứ')
        # ---- ghi các cột thêm
        vals=['','','','','','','']
        if rec and rec.found:
            vals[0]=rec.name
            vals[1]=rec.scope
            vals[2]=' | '.join(dict.fromkeys(x for x in ([w.position for w in ref]+([] if ref else [rec.position])) if x))
            vals[3]=' | '.join(w.ngay_lam for w in ref)
            vals[4]=' | '.join(w.ngay_nghi for w in ref)
            vals[5]=' | '.join(w.thoi_gian for w in ref)
        hard=[m for m in issues if not m.startswith('SYT không ghi lịch')]
        vals[6]=('; '.join(issues)) if issues else 'Khớp'
        if not hard:stats['khop']+=1
        else:stats['co_loi']+=1
        for i,v in enumerate(vals,1):
            ws.cell(rr,base+i,v)
        for c in bad_cells:ws.cell(rr,c).fill=red
        if hard:ws.cell(rr,base+7).fill=red
    for i,wd in enumerate((26,40,30,14,14,48,70),1):
        ws.column_dimensions[get_column_letter(base+i)].width=wd
    # ---- người SYT đang làm nhưng danh mục không có
    ws2=wb.create_sheet('SYT co - DM thieu')
    ws2.append(['STT','Họ tên','Số chứng chỉ','Vị trí','Phạm vi hành nghề','Ngày làm (SYT)','Ngày nghỉ (SYT)','Lịch (SYT)'])
    for c in ws2[1]:c.font=Font(bold=True)
    n=0
    for code,rec in recs.items():
        if code in seen_codes or not rec.found:continue
        act=[w for w in rec.workplaces if ML._normalize_code(w.gphd)==gkey
             and not ((_parse_date(w.ngay_lam) and today_d<_parse_date(w.ngay_lam)) or (_parse_date(w.ngay_nghi) and today_d>_parse_date(w.ngay_nghi)))]
        if not act:continue
        n+=1
        ws2.append([n,rec.name,rec.cchn,' | '.join(dict.fromkeys(w.position for w in act if w.position)) or rec.position,rec.scope,
                    ' | '.join(w.ngay_lam for w in act),' | '.join(w.ngay_nghi for w in act),' | '.join(w.thoi_gian for w in act)])
    for i,wd in enumerate((6,28,22,34,50,14,14,60),1):ws2.column_dimensions[get_column_letter(i)].width=wd
    stats['syt_thieu_trong_dm']=n
    # ---- tổng hợp
    ws3=wb.create_sheet('Tong hop')
    info=ML.facility_info(gphd)
    for row in [('Đối chiếu danh mục nhân viên với SYT',''),('Cơ sở',meta.get('facility_name','') or info.get('name','')),('GPHĐ',gphd),
                ('Nguồn danh sách SYT',('Bản lưu '+Path(snap).name+' (offline)') if (snap and offline) else ('Bản lưu '+Path(snap).name if snap else 'Tra mạng')),
                ('Thời điểm lấy dữ liệu SYT',str(meta.get('fetched_at',''))),('Ngày đối chiếu',today.strftime('%d/%m/%Y %H:%M')),
                ('Giờ hoạt động của cơ sở',info.get('hours','') or meta.get('hours','') or '(không ghi — coi như nguyên ngày)'),
                ('',''),('Tổng số dòng danh mục',stats['tong']),('Khớp',stats['khop']),('Có sai lệch',stats['co_loi']),
                ('  trong đó không có trên SYT/cơ sở',stats['khong_co_syt']),('SYT đang làm nhưng danh mục thiếu',n),('',''),('Số lỗi theo loại','')]:
        ws3.append(list(row))
    for k,v in sorted(loi_count.items(),key=lambda x:-x[1]):ws3.append(['  '+k,v])
    ws3['A1'].font=Font(bold=True,size=13)
    ws3.column_dimensions['A'].width=40;ws3.column_dimensions['B'].width=60
    # ---- lưu (không ghi đè)
    if not out_path:
        src=Path(xlsx_path)
        out=src.with_name(f"{src.stem}_DoiChieuSYT_{today.strftime('%d-%m-%Y')}.xlsx")
    else:
        out=Path(out_path)
    k=1;base_out=out
    while out.exists():
        k+=1;out=base_out.with_name(f'{base_out.stem}_{k}.xlsx')
    try:
        wb.save(out)
    except Exception as e:
        return '',stats,f'Không ghi được file kết quả: {e}'
    stats['loi_theo_loai']=loi_count
    return str(out),stats,''
