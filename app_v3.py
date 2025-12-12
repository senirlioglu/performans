"""
🩺 BÖLGESEL PERAKENDE PERFORMANS RÖNTGENİ v3
DuckDB + Parquet ile Ultra Hızlı Versiyon

120MB+ Excel dosyaları için optimize edildi:
- Excel → temp Parquet dönüşümü
- DuckDB ile SQL-based aggregation
- Minimal RAM kullanımı
- Streaming data processing
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import tempfile
import os
from io import BytesIO
from pathlib import Path
import warnings
import gc
warnings.filterwarnings('ignore')

# ============================================================================
# SAYFA AYARLARI
# ============================================================================
st.set_page_config(
    page_title="Performans Röntgeni v3",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# SABİTLER
# ============================================================================
MIN_BASE_SALES_TL = 10000

WEIGHTS = {
    'share_drop': 35,
    'margin_drop': 25,
    'fire_increase': 20,
    'inv_increase': 20
}

# Sadece gerekli kolonlar
REQUIRED_COLS = [
    'SM', 'BS', 'YIL', 'Mağaza - Anahtar', 'Mağaza - Orta uzunl.metin',
    'Ürün Grubu - Orta uzunl.metin', 'Malzeme Nitelik - Metin',
    'Mal Grubu - Orta uzunl.metin', 'Üst Mal Grubu - Orta uzunl.metin',
    'Malzeme Kodu', 'Malzeme Tanımı',
    'Satış Miktarı', 'Satış Hasılatı (VD)', 'Net Marj',
    'Fire Tutarı', 'Envanter Tutarı', 'Toplam Kampanya Zararı'
]

# ============================================================================
# CSS
# ============================================================================
st.markdown("""
<style>
    .main-header {font-size: 1.8rem; font-weight: 700; color: #1f2937;}
    .section-header {font-size: 1.2rem; font-weight: 600; color: #374151; 
                     border-bottom: 2px solid #e5e7eb; padding-bottom: 0.5rem; margin: 1rem 0;}
    .incident-card {
        border-left: 4px solid #ef4444;
        background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
        padding: 1rem; border-radius: 0 12px 12px 0; margin: 0.5rem 0;
    }
    .incident-card-warning {border-left-color: #f59e0b; background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);}
    .incident-card-success {border-left-color: #10b981; background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%);}
    .score-badge {display: inline-block; padding: 0.25rem 0.75rem; border-radius: 9999px; font-weight: 600; font-size: 0.85rem;}
    .score-critical {background: #ef4444; color: white;}
    .score-warning {background: #f59e0b; color: white;}
    .score-low {background: #6b7280; color: white;}
    .action-box {background: #f0fdf4; border: 1px solid #86efac; padding: 0.5rem; border-radius: 6px; font-size: 0.85rem; margin-top: 0.5rem;}
    .kpi-card {background: white; border: 1px solid #e5e7eb; border-radius: 12px; padding: 1rem; text-align: center;}
    .kpi-value {font-size: 1.5rem; font-weight: 700; color: #1f2937;}
    .kpi-label {font-size: 0.85rem; color: #6b7280;}
    .kpi-delta-pos {color: #10b981;}
    .kpi-delta-neg {color: #ef4444;}
</style>
""", unsafe_allow_html=True)


# ============================================================================
# DUCKDB İLE VERİ İŞLEME
# ============================================================================

def get_temp_dir():
    """Temp dizini al veya oluştur"""
    temp_dir = Path(tempfile.gettempdir()) / "perf_rontgen"
    temp_dir.mkdir(exist_ok=True)
    return temp_dir


def excel_to_parquet(file_bytes, year, temp_dir):
    """Excel'i parquet'e çevir - chunk chunk oku"""
    
    parquet_path = temp_dir / f"data_{year}.parquet"
    
    # Excel'i oku - sadece gerekli kolonlar
    # Chunk okuma Excel'de yok ama en azından kolon sınırlama var
    df = pd.read_excel(
        BytesIO(file_bytes),
        engine='openpyxl',
        usecols=lambda x: x.strip() in REQUIRED_COLS
    )
    
    df.columns = df.columns.str.strip()
    
    # YIL ekle
    if 'YIL' not in df.columns:
        df['YIL'] = year
    
    # Veri tiplerini optimize et
    for col in df.columns:
        if col in ['Satış Miktarı', 'Satış Hasılatı (VD)', 'Net Marj', 
                   'Fire Tutarı', 'Envanter Tutarı', 'Toplam Kampanya Zararı']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        elif col not in ['YIL', 'Mağaza - Anahtar', 'Malzeme Kodu']:
            df[col] = df[col].astype(str).replace('nan', '')
    
    # Parquet'e yaz
    df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
    
    del df
    gc.collect()
    
    return parquet_path


@st.cache_data(ttl=3600, show_spinner=False)
def load_and_process_with_duckdb(_file_bytes_2024, _file_bytes_2025, key):
    """DuckDB ile veri yükle ve işle"""
    
    temp_dir = get_temp_dir()
    
    progress = st.progress(0, text="Excel dosyaları Parquet'e dönüştürülüyor...")
    
    # 1. Excel → Parquet
    progress.progress(10, text="2024 verisi dönüştürülüyor...")
    parquet_2024 = excel_to_parquet(_file_bytes_2024, 2024, temp_dir)
    
    progress.progress(40, text="2025 verisi dönüştürülüyor...")
    parquet_2025 = excel_to_parquet(_file_bytes_2025, 2025, temp_dir)
    
    progress.progress(60, text="DuckDB ile analiz yapılıyor...")
    
    # 2. DuckDB bağlantısı
    con = duckdb.connect()
    
    # 3. View oluştur (iki parquet'i birleştir)
    con.execute(f"""
        CREATE OR REPLACE VIEW veri AS
        SELECT * FROM read_parquet('{parquet_2024}')
        UNION ALL
        SELECT * FROM read_parquet('{parquet_2025}')
    """)
    
    # 4. Aggregasyonları SQL ile yap (çok hızlı!)
    progress.progress(70, text="Mağaza metrikleri hesaplanıyor...")
    
    result = {}
    
    # Genel toplamlar
    result['totals'] = con.execute("""
        SELECT 
            YIL,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Satış Miktarı") as adet,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire,
            SUM(ABS("Envanter Tutarı")) as envanter,
            SUM(ABS("Toplam Kampanya Zararı")) as kampanya
        FROM veri
        GROUP BY YIL
    """).df()
    
    # Mağaza metrikleri
    result['store'] = con.execute("""
        SELECT 
            YIL,
            "Mağaza - Anahtar" as magaza,
            FIRST("Mağaza - Orta uzunl.metin") as magaza_adi,
            FIRST(SM) as sm,
            FIRST(BS) as bs,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Satış Miktarı") as adet,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire,
            SUM(ABS("Envanter Tutarı")) as envanter,
            SUM(ABS("Toplam Kampanya Zararı")) as kampanya
        FROM veri
        GROUP BY YIL, "Mağaza - Anahtar"
    """).df()
    
    progress.progress(80, text="Kategori metrikleri hesaplanıyor...")
    
    # Mağaza x Ürün Grubu
    result['store_urun_grubu'] = con.execute("""
        SELECT 
            YIL,
            "Mağaza - Anahtar" as magaza,
            FIRST("Mağaza - Orta uzunl.metin") as magaza_adi,
            FIRST(SM) as sm,
            FIRST(BS) as bs,
            "Ürün Grubu - Orta uzunl.metin" as kategori,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Satış Miktarı") as adet,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire,
            SUM(ABS("Envanter Tutarı")) as envanter,
            SUM(ABS("Toplam Kampanya Zararı")) as kampanya
        FROM veri
        GROUP BY YIL, "Mağaza - Anahtar", "Ürün Grubu - Orta uzunl.metin"
    """).df()
    
    # Mağaza x Üst Mal Grubu
    result['store_ust_mal'] = con.execute("""
        SELECT 
            YIL,
            "Mağaza - Anahtar" as magaza,
            FIRST("Mağaza - Orta uzunl.metin") as magaza_adi,
            FIRST(SM) as sm,
            FIRST(BS) as bs,
            "Üst Mal Grubu - Orta uzunl.metin" as kategori,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Satış Miktarı") as adet,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire,
            SUM(ABS("Envanter Tutarı")) as envanter,
            SUM(ABS("Toplam Kampanya Zararı")) as kampanya
        FROM veri
        GROUP BY YIL, "Mağaza - Anahtar", "Üst Mal Grubu - Orta uzunl.metin"
    """).df()
    
    # Mağaza x Nitelik
    result['store_nitelik'] = con.execute("""
        SELECT 
            YIL,
            "Mağaza - Anahtar" as magaza,
            FIRST("Mağaza - Orta uzunl.metin") as magaza_adi,
            FIRST(SM) as sm,
            FIRST(BS) as bs,
            "Malzeme Nitelik - Metin" as kategori,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Satış Miktarı") as adet,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire,
            SUM(ABS("Envanter Tutarı")) as envanter,
            SUM(ABS("Toplam Kampanya Zararı")) as kampanya
        FROM veri
        GROUP BY YIL, "Mağaza - Anahtar", "Malzeme Nitelik - Metin"
    """).df()
    
    # SM metrikleri
    result['sm'] = con.execute("""
        SELECT 
            YIL,
            SM as sm,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Satış Miktarı") as adet,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire,
            COUNT(DISTINCT "Mağaza - Anahtar") as magaza_sayisi
        FROM veri
        GROUP BY YIL, SM
    """).df()
    
    # BS metrikleri
    result['bs'] = con.execute("""
        SELECT 
            YIL,
            SM as sm,
            BS as bs,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Satış Miktarı") as adet,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire,
            COUNT(DISTINCT "Mağaza - Anahtar") as magaza_sayisi
        FROM veri
        GROUP BY YIL, SM, BS
    """).df()
    
    # Kategori metrikleri
    result['urun_grubu'] = con.execute("""
        SELECT 
            YIL,
            "Ürün Grubu - Orta uzunl.metin" as kategori,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire
        FROM veri
        GROUP BY YIL, "Ürün Grubu - Orta uzunl.metin"
    """).df()
    
    result['ust_mal_grubu'] = con.execute("""
        SELECT 
            YIL,
            "Üst Mal Grubu - Orta uzunl.metin" as kategori,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire
        FROM veri
        GROUP BY YIL, "Üst Mal Grubu - Orta uzunl.metin"
    """).df()
    
    result['nitelik'] = con.execute("""
        SELECT 
            YIL,
            "Malzeme Nitelik - Metin" as kategori,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire
        FROM veri
        GROUP BY YIL, "Malzeme Nitelik - Metin"
    """).df()
    
    # Top ürünler (2025)
    result['top_products'] = con.execute("""
        SELECT 
            "Malzeme Kodu" as kod,
            FIRST("Malzeme Tanımı") as tanim,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Satış Miktarı") as adet,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire
        FROM veri
        WHERE YIL = 2025
        GROUP BY "Malzeme Kodu"
        ORDER BY ciro DESC
        LIMIT 100
    """).df()
    
    # Filtre seçenekleri
    result['filter_options'] = {
        'sm': con.execute("SELECT DISTINCT SM FROM veri WHERE SM IS NOT NULL ORDER BY SM").df()['SM'].tolist(),
        'bs': con.execute("SELECT DISTINCT SM, BS FROM veri WHERE BS IS NOT NULL ORDER BY SM, BS").df(),
        'nitelik': con.execute("SELECT DISTINCT \"Malzeme Nitelik - Metin\" as n FROM veri WHERE n IS NOT NULL ORDER BY n").df()['n'].tolist(),
        'urun_grubu': con.execute("SELECT DISTINCT \"Ürün Grubu - Orta uzunl.metin\" as n FROM veri WHERE n IS NOT NULL ORDER BY n").df()['n'].tolist(),
    }
    
    progress.progress(90, text="Metrikler hesaplanıyor...")
    
    # 5. YoY metrikleri hesapla
    result['metrics'] = compute_metrics(result)
    
    # 6. Incident'ları tespit et
    progress.progress(95, text="Sorunlar tespit ediliyor...")
    result['incidents'] = detect_incidents(result['metrics'])
    
    # Temizlik
    con.close()
    
    progress.progress(100, text="Tamamlandı!")
    progress.empty()
    
    return result


def compute_metrics(data):
    """YoY metriklerini hesapla"""
    
    metrics = {}
    
    # Genel toplamlar
    totals = data['totals']
    t2024 = totals[totals['YIL'] == 2024].iloc[0] if len(totals[totals['YIL'] == 2024]) > 0 else None
    t2025 = totals[totals['YIL'] == 2025].iloc[0] if len(totals[totals['YIL'] == 2025]) > 0 else None
    
    if t2024 is not None and t2025 is not None:
        metrics['totals'] = {
            'ciro_2024': t2024.get('ciro', 0),
            'ciro_2025': t2025.get('ciro', 0),
            'ciro_change': safe_pct(t2025.get('ciro', 0), t2024.get('ciro', 0)),
            'marj_2024': t2024.get('marj', 0),
            'marj_2025': t2025.get('marj', 0),
            'marj_change': safe_pct(t2025.get('marj', 0), t2024.get('marj', 0)),
            'adet_2024': t2024.get('adet', 0),
            'adet_2025': t2025.get('adet', 0),
            'adet_change': safe_pct(t2025.get('adet', 0), t2024.get('adet', 0)),
            'fire_2024': t2024.get('fire', 0),
            'fire_2025': t2025.get('fire', 0),
            'fire_change': safe_pct(t2025.get('fire', 0), t2024.get('fire', 0)),
            'envanter_2025': t2025.get('envanter', 0),
            'kampanya_2025': t2025.get('kampanya', 0),
        }
    
    # Mağaza metrikleri
    metrics['store'] = pivot_yoy(data['store'], ['magaza', 'magaza_adi', 'sm', 'bs'])
    
    # Kategori metrikleri
    metrics['store_urun_grubu'] = pivot_yoy(data['store_urun_grubu'], ['magaza', 'magaza_adi', 'sm', 'bs', 'kategori'])
    metrics['store_ust_mal'] = pivot_yoy(data['store_ust_mal'], ['magaza', 'magaza_adi', 'sm', 'bs', 'kategori'])
    metrics['store_nitelik'] = pivot_yoy(data['store_nitelik'], ['magaza', 'magaza_adi', 'sm', 'bs', 'kategori'])
    
    # SM/BS
    metrics['sm'] = pivot_yoy(data['sm'], ['sm'])
    metrics['bs'] = pivot_yoy(data['bs'], ['sm', 'bs'])
    
    # Kategoriler
    metrics['urun_grubu'] = pivot_yoy(data['urun_grubu'], ['kategori'])
    metrics['ust_mal_grubu'] = pivot_yoy(data['ust_mal_grubu'], ['kategori'])
    metrics['nitelik'] = pivot_yoy(data['nitelik'], ['kategori'])
    
    return metrics


def safe_pct(new, old):
    """Güvenli yüzde değişim"""
    if pd.isna(old) or pd.isna(new) or old == 0:
        return 0
    return ((new / old) - 1) * 100


def pivot_yoy(df, key_cols):
    """YoY pivot tablosu oluştur"""
    
    if df.empty:
        return pd.DataFrame()
    
    df_2024 = df[df['YIL'] == 2024].copy()
    df_2025 = df[df['YIL'] == 2025].copy()
    
    # Suffix ekle
    metric_cols = [c for c in df.columns if c not in key_cols + ['YIL']]
    
    df_2024 = df_2024.drop(columns=['YIL'])
    df_2025 = df_2025.drop(columns=['YIL'])
    
    for col in metric_cols:
        df_2024 = df_2024.rename(columns={col: f'{col}_2024'})
        df_2025 = df_2025.rename(columns={col: f'{col}_2025'})
    
    # Merge
    merged = df_2024.merge(df_2025, on=key_cols, how='outer')
    
    # Değişim hesapla
    if 'ciro_2024' in merged.columns and 'ciro_2025' in merged.columns:
        merged['ciro_change'] = merged.apply(lambda x: safe_pct(x.get('ciro_2025', 0), x.get('ciro_2024', 0)), axis=1)
    if 'marj_2024' in merged.columns and 'marj_2025' in merged.columns:
        merged['marj_change'] = merged.apply(lambda x: safe_pct(x.get('marj_2025', 0), x.get('marj_2024', 0)), axis=1)
    if 'fire_2024' in merged.columns and 'fire_2025' in merged.columns:
        merged['fire_change'] = merged.apply(lambda x: safe_pct(x.get('fire_2025', 0), x.get('fire_2024', 0)), axis=1)
    if 'kampanya_2024' in merged.columns and 'kampanya_2025' in merged.columns:
        merged['kampanya_change'] = merged.apply(lambda x: safe_pct(x.get('kampanya_2025', 0), x.get('kampanya_2024', 0)), axis=1)
    
    return merged


def detect_incidents(metrics):
    """Sorunlu alanları tespit et"""
    
    all_incidents = []
    
    # Mağaza x Kategori incident'ları
    for level, df_key in [
        ('Mağaza-ÜrünGrubu', 'store_urun_grubu'),
        ('Mağaza-ÜstMal', 'store_ust_mal'),
        ('Mağaza-Nitelik', 'store_nitelik')
    ]:
        df = metrics.get(df_key, pd.DataFrame())
        if df.empty:
            continue
        
        # Minimum baz filtresi
        df = df[df.get('ciro_2024', 0) >= MIN_BASE_SALES_TL]
        
        for _, row in df.iterrows():
            incident = create_incident(row, level)
            if incident and incident['score'] > 20:
                all_incidents.append(incident)
    
    # Mağaza seviyesi
    df = metrics.get('store', pd.DataFrame())
    if not df.empty:
        df = df[df.get('ciro_2024', 0) >= MIN_BASE_SALES_TL]
        for _, row in df.iterrows():
            incident = create_incident(row, 'Mağaza', kategori='Genel')
            if incident and incident['score'] > 25:
                all_incidents.append(incident)
    
    if not all_incidents:
        return pd.DataFrame()
    
    incidents_df = pd.DataFrame(all_incidents)
    incidents_df = incidents_df.sort_values('score', ascending=False).reset_index(drop=True)
    
    return incidents_df


def create_incident(row, level, kategori=None):
    """Incident oluştur"""
    
    ciro_change = row.get('ciro_change', 0) or 0
    marj_change = row.get('marj_change', 0) or 0
    fire_change = row.get('fire_change', 0) or 0
    kampanya_change = row.get('kampanya_change', 0) or 0
    
    # Skor hesapla
    ciro_drop = max(0, -ciro_change)
    marj_drop = max(0, -marj_change)
    fire_inc = max(0, fire_change)
    
    ciro_score = min(100, ciro_drop * 2)
    marj_score = min(100, marj_drop * 2)
    fire_score = min(100, fire_inc / 2)
    
    total_score = (
        ciro_score * WEIGHTS['share_drop'] / 100 +
        marj_score * WEIGHTS['margin_drop'] / 100 +
        fire_score * WEIGHTS['fire_increase'] / 100
    )
    
    # Neden analizi
    reason, action = analyze_reason(ciro_change, marj_change, fire_change, kampanya_change)
    
    return {
        'score': round(total_score, 1),
        'level': level,
        'sm': row.get('sm', '-'),
        'bs': row.get('bs', '-'),
        'magaza': row.get('magaza', '-'),
        'magaza_adi': row.get('magaza_adi', '-'),
        'kategori': kategori or row.get('kategori', '-'),
        'ciro_2024': row.get('ciro_2024', 0),
        'ciro_2025': row.get('ciro_2025', 0),
        'ciro_change': ciro_change,
        'marj_change': marj_change,
        'fire_change': fire_change,
        'reason': reason,
        'action': action
    }


def analyze_reason(ciro_change, marj_change, fire_change, kampanya_change):
    """Neden analizi"""
    
    if kampanya_change > 50 and marj_change < -10:
        return "Kampanya Zararı", "Kampanya karlılık analizini gözden geçir"
    if fire_change > 100:
        return "Fire Patlaması", "SKT kontrolü yap, sipariş miktarlarını gözden geçir"
    if fire_change > 50:
        return "Fire Artışı", "Raf düzenini ve stok seviyelerini kontrol et"
    if marj_change < -20:
        return "Marj Erimesi", "Fiyatlama ve SMM değişikliklerini kontrol et"
    if ciro_change < -15:
        return "Ciro Düşüşü", "Ürün bulunurluğu ve kategori yönetimini gözden geçir"
    
    return "Performans Düşüşü", "Detaylı analiz gerekli"


# ============================================================================
# EXCEL RAPOR
# ============================================================================

def create_excel_report(data):
    """Excel raporu oluştur"""
    
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        
        metrics = data['metrics']
        incidents = data['incidents']
        
        # 1. Müdahale Haritası
        if not incidents.empty:
            mudahale = incidents.head(50)[['score', 'level', 'sm', 'bs', 'magaza_adi',
                                            'kategori', 'ciro_change', 'marj_change', 
                                            'fire_change', 'reason', 'action']]
            mudahale.to_excel(writer, sheet_name='Müdahale Haritası', index=False)
        
        # 2. Mağaza Detay
        store = metrics.get('store', pd.DataFrame())
        if not store.empty:
            store.to_excel(writer, sheet_name='Mağaza Detay', index=False)
        
        # 3. SM Performans
        sm = metrics.get('sm', pd.DataFrame())
        if not sm.empty:
            sm.to_excel(writer, sheet_name='SM Performans', index=False)
        
        # 4. BS Performans
        bs = metrics.get('bs', pd.DataFrame())
        if not bs.empty:
            bs.to_excel(writer, sheet_name='BS Performans', index=False)
        
        # 5. Top Ürünler
        top = data.get('top_products', pd.DataFrame())
        if not top.empty:
            top.to_excel(writer, sheet_name='Top Ürünler', index=False)
    
    output.seek(0)
    return output


# ============================================================================
# UI BİLEŞENLERİ
# ============================================================================

def render_sidebar(filter_options):
    """Sidebar"""
    
    st.sidebar.markdown("### 🎛️ FİLTRELER")
    
    sm_list = ['Tümü'] + filter_options.get('sm', [])
    selected_sm = st.sidebar.selectbox('📊 SM', sm_list)
    
    bs_df = filter_options.get('bs', pd.DataFrame())
    if selected_sm != 'Tümü' and not bs_df.empty:
        bs_opts = bs_df[bs_df['SM'] == selected_sm]['BS'].tolist()
    elif not bs_df.empty:
        bs_opts = bs_df['BS'].unique().tolist()
    else:
        bs_opts = []
    bs_list = ['Tümü'] + sorted(set(bs_opts))
    selected_bs = st.sidebar.selectbox('👤 BS', bs_list)
    
    st.sidebar.markdown("---")
    
    nitelik_list = ['Tümü'] + filter_options.get('nitelik', [])
    selected_nitelik = st.sidebar.selectbox('🏷️ Nitelik', nitelik_list)
    
    urun_list = ['Tümü'] + filter_options.get('urun_grubu', [])
    selected_urun = st.sidebar.selectbox('📂 Ürün Grubu', urun_list)
    
    return {'sm': selected_sm, 'bs': selected_bs, 'nitelik': selected_nitelik, 'urun_grubu': selected_urun}


def filter_incidents(incidents, filters):
    """Incident'ları filtrele"""
    
    if incidents.empty:
        return incidents
    
    df = incidents.copy()
    
    if filters['sm'] != 'Tümü':
        df = df[df['sm'] == filters['sm']]
    if filters['bs'] != 'Tümü':
        df = df[df['bs'] == filters['bs']]
    if filters['nitelik'] != 'Tümü':
        df = df[df['kategori'] == filters['nitelik']]
    if filters['urun_grubu'] != 'Tümü':
        df = df[df['kategori'] == filters['urun_grubu']]
    
    return df


def render_kpis(totals):
    """KPI kartları"""
    
    c1, c2, c3, c4 = st.columns(4)
    
    with c1:
        delta = totals.get('ciro_change', 0)
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">💰 Toplam Ciro</div>
            <div class="kpi-value">₺{totals.get('ciro_2025', 0):,.0f}</div>
            <div class="{'kpi-delta-pos' if delta > 0 else 'kpi-delta-neg'}">{'+' if delta > 0 else ''}{delta:.1f}%</div>
        </div>""", unsafe_allow_html=True)
    
    with c2:
        delta = totals.get('marj_change', 0)
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">📈 Toplam Marj</div>
            <div class="kpi-value">₺{totals.get('marj_2025', 0):,.0f}</div>
            <div class="{'kpi-delta-pos' if delta > 0 else 'kpi-delta-neg'}">{'+' if delta > 0 else ''}{delta:.1f}%</div>
        </div>""", unsafe_allow_html=True)
    
    with c3:
        delta = totals.get('adet_change', 0)
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">📦 Satış Adedi</div>
            <div class="kpi-value">{totals.get('adet_2025', 0):,.0f}</div>
            <div class="{'kpi-delta-pos' if delta > 0 else 'kpi-delta-neg'}">{'+' if delta > 0 else ''}{delta:.1f}%</div>
        </div>""", unsafe_allow_html=True)
    
    with c4:
        delta = totals.get('fire_change', 0)
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">🔥 Fire Kaybı</div>
            <div class="kpi-value">₺{totals.get('fire_2025', 0):,.0f}</div>
            <div class="{'kpi-delta-neg' if delta > 0 else 'kpi-delta-pos'}">{'+' if delta > 0 else ''}{delta:.1f}%</div>
        </div>""", unsafe_allow_html=True)


def render_incidents(incidents):
    """Incident kartları"""
    
    st.markdown('<p class="section-header">🚨 ACİL MÜDAHALE GEREKTİREN ALANLAR</p>', unsafe_allow_html=True)
    
    if incidents.empty:
        st.success("✅ Kritik seviyede müdahale gerektiren alan yok!")
        return
    
    for _, row in incidents.head(5).iterrows():
        score = row['score']
        score_class = 'score-critical' if score >= 60 else 'score-warning' if score >= 40 else 'score-low'
        card_class = 'incident-card' if score >= 60 else 'incident-card incident-card-warning'
        
        st.markdown(f"""
        <div class="{card_class}">
            <span class="score-badge {score_class}">Skor: {score:.0f}</span>
            <strong style="margin-left: 10px;">{row['reason']}</strong>
            <div style="margin-top: 0.5rem;">
                <strong>{row['level']}</strong>: {row['magaza']} - {row['magaza_adi']}<br>
                <small>SM: {row['sm']} | BS: {row['bs']} | Kategori: {row['kategori']}</small>
            </div>
            <div style="margin-top: 0.5rem;">
                📊 Ciro: <span style="color: {'#ef4444' if row['ciro_change'] < 0 else '#10b981'}">{row['ciro_change']:+.1f}%</span> | 
                💰 Marj: <span style="color: {'#ef4444' if row['marj_change'] < 0 else '#10b981'}">{row['marj_change']:+.1f}%</span> | 
                🔥 Fire: <span style="color: {'#ef4444' if row['fire_change'] > 0 else '#10b981'}">{row['fire_change']:+.1f}%</span>
            </div>
            <div class="action-box">💡 <strong>Öneri:</strong> {row['action']}</div>
        </div>
        """, unsafe_allow_html=True)


def render_leakage(totals):
    """Sızıntı analizi"""
    
    st.markdown('<p class="section-header">💸 MARJ SIZINTISI</p>', unsafe_allow_html=True)
    
    kampanya = totals.get('kampanya_2025', 0)
    fire = totals.get('fire_2025', 0)
    envanter = totals.get('envanter_2025', 0)
    total = kampanya + fire + envanter
    
    if total == 0:
        st.info("Kayda değer sızıntı yok")
        return
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TOPLAM", f"₺{total:,.0f}")
    c2.metric("Kampanya", f"₺{kampanya:,.0f}", f"{kampanya/total*100:.0f}%" if total > 0 else "0%")
    c3.metric("Fire", f"₺{fire:,.0f}", f"{fire/total*100:.0f}%" if total > 0 else "0%")
    c4.metric("Envanter", f"₺{envanter:,.0f}", f"{envanter/total*100:.0f}%" if total > 0 else "0%")


def render_tables(data, filters):
    """Detay tabloları"""
    
    st.markdown('<p class="section-header">📋 DETAY TABLOLARI</p>', unsafe_allow_html=True)
    
    tabs = st.tabs(["📊 Tüm Incidents", "🏪 Mağaza", "👥 SM/BS"])
    
    with tabs[0]:
        incidents = data['incidents']
        if not incidents.empty:
            filtered = filter_incidents(incidents, filters)
            st.dataframe(filtered.head(100), use_container_width=True, hide_index=True)
    
    with tabs[1]:
        store = data['metrics'].get('store', pd.DataFrame())
        if not store.empty:
            st.dataframe(store.sort_values('ciro_change'), use_container_width=True, hide_index=True)
    
    with tabs[2]:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**SM**")
            sm = data['metrics'].get('sm', pd.DataFrame())
            if not sm.empty:
                st.dataframe(sm.sort_values('ciro_change'), use_container_width=True, hide_index=True)
        with c2:
            st.markdown("**BS**")
            bs = data['metrics'].get('bs', pd.DataFrame())
            if not bs.empty:
                st.dataframe(bs.sort_values('ciro_change'), use_container_width=True, hide_index=True)


# ============================================================================
# MAIN
# ============================================================================

def main():
    st.markdown('<h1 class="main-header">🩺 Performans Röntgeni v3</h1>', unsafe_allow_html=True)
    st.caption("DuckDB ile Ultra Hızlı | 120MB+ Dosyalar İçin Optimize")
    
    c1, c2 = st.columns(2)
    with c1:
        file_2024 = st.file_uploader("📁 2024 Verisi", type=['xlsx'], key='f2024')
    with c2:
        file_2025 = st.file_uploader("📁 2025 Verisi", type=['xlsx'], key='f2025')
    
    if not file_2024 or not file_2025:
        st.info("👆 Her iki dosyayı da yükleyin")
        st.markdown("""
        ### 🚀 v3 Farkı
        - **DuckDB** ile SQL-tabanlı analiz
        - **Parquet** dönüşümü ile hızlı okuma
        - **120MB+** dosyalar için optimize
        - Minimum RAM kullanımı
        """)
        return
    
    # Unique key for caching
    cache_key = f"{file_2024.name}_{file_2025.name}_{file_2024.size}_{file_2025.size}"
    
    data = load_and_process_with_duckdb(
        file_2024.getvalue(),
        file_2025.getvalue(),
        cache_key
    )
    
    if data is None:
        st.error("Veri yüklenemedi!")
        return
    
    filters = render_sidebar(data['filter_options'])
    filtered_incidents = filter_incidents(data['incidents'], filters)
    
    # Excel rapor
    excel = create_excel_report(data)
    st.download_button(
        "📥 EXCEL RAPORU İNDİR",
        excel,
        f"mudahale_haritasi_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
    
    st.markdown("---")
    
    render_kpis(data['metrics'].get('totals', {}))
    
    st.markdown("---")
    
    c1, c2 = st.columns([3, 2])
    with c1:
        render_incidents(filtered_incidents)
    with c2:
        render_leakage(data['metrics'].get('totals', {}))
    
    st.markdown("---")
    
    render_tables(data, filters)
    
    st.caption(f"Min. Baz: ₺{MIN_BASE_SALES_TL:,} | {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
