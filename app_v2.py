"""
🎯 BÖLGESEL PERAKENDE PERFORMANS RÖNTGENİ v2
Sorun Bulucu / Müdahale Haritası

Özellikler:
- Incident Scoring (0-100 puan)
- Otomatik Neden Tespiti
- Aksiyon Önerileri
- Minimum Baz Filtresi (küçük bazları eleme)
- 6 Sekmeli Excel Rapor
- DuckDB + Parquet ile hızlı okuma (opsiyonel)
"""

import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import warnings
import gc
# scipy removed - not needed
warnings.filterwarnings('ignore')

# ============================================================================
# SAYFA AYARLARI
# ============================================================================
st.set_page_config(
    page_title="Performans Röntgeni v2",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# SABİTLER VE EŞİKLER
# ============================================================================

# Minimum baz eşiği - 2024'te bu TL altı satışı olanları incident'a alma
MIN_BASE_SALES_TL = 10000

# Incident skor ağırlıkları (toplam 100)
WEIGHTS = {
    'share_drop': 35,      # Ciro payı düşüşü
    'margin_drop': 25,     # Marj erimesi
    'fire_increase': 20,   # Fire artışı
    'inv_increase': 20     # Envanter artışı
}

# Renk eşikleri
THRESHOLDS = {
    'critical': -20,    # %20'den fazla düşüş = kırmızı
    'warning': -10,     # %10-20 düşüş = sarı
    'good': 10          # %10'dan fazla artış = yeşil
}

# Gerekli kolonlar
REQUIRED_COLS = [
    'SM', 'BS', 'YIL', 'Mağaza - Anahtar', 'Mağaza - Orta uzunl.metin',
    'Ürün Grubu - Orta uzunl.metin', 'Malzeme Nitelik - Metin',
    'Mal Grubu - Orta uzunl.metin', 'Üst Mal Grubu - Orta uzunl.metin',
    'Malzeme Kodu', 'Malzeme Tanımı',
    'Satış Miktarı', 'Satış Hasılatı (VD)', 'Net Marj', 'Net Marj Oranı',
    'Fire Tutarı', 'Envanter Tutarı', 'Toplam Kampanya Zararı', 'Toplam İndirim'
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
        padding: 1rem;
        border-radius: 0 12px 12px 0;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .incident-card-warning {
        border-left-color: #f59e0b;
        background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);
    }
    .incident-card-success {
        border-left-color: #10b981;
        background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%);
    }
    
    .score-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .score-critical {background: #ef4444; color: white;}
    .score-warning {background: #f59e0b; color: white;}
    .score-low {background: #6b7280; color: white;}
    
    .reason-tag {
        display: inline-block;
        padding: 0.2rem 0.5rem;
        background: #e5e7eb;
        border-radius: 4px;
        font-size: 0.8rem;
        margin-right: 0.5rem;
    }
    
    .action-box {
        background: #f0fdf4;
        border: 1px solid #86efac;
        padding: 0.5rem;
        border-radius: 6px;
        font-size: 0.85rem;
        margin-top: 0.5rem;
    }
    
    .kpi-card {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .kpi-value {font-size: 1.5rem; font-weight: 700; color: #1f2937;}
    .kpi-label {font-size: 0.85rem; color: #6b7280;}
    .kpi-delta-pos {color: #10b981; font-size: 0.9rem;}
    .kpi-delta-neg {color: #ef4444; font-size: 0.9rem;}
</style>
""", unsafe_allow_html=True)


# ============================================================================
# VERİ YÜKLEME (OPTİMİZE)
# ============================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def load_and_process_data(file_bytes_2024, file_bytes_2025, name_2024, name_2025):
    """Veriyi yükle, optimize et, aggregate et"""
    
    progress = st.progress(0, text="Veriler yükleniyor...")
    
    # 1. Dosyaları oku (sadece gerekli kolonlar)
    progress.progress(10, text="2024 verisi okunuyor...")
    df_2024 = pd.read_excel(
        BytesIO(file_bytes_2024),
        engine='openpyxl',
        usecols=lambda x: x.strip() in REQUIRED_COLS
    )
    df_2024['YIL'] = 2024
    
    progress.progress(30, text="2025 verisi okunuyor...")
    df_2025 = pd.read_excel(
        BytesIO(file_bytes_2025),
        engine='openpyxl',
        usecols=lambda x: x.strip() in REQUIRED_COLS
    )
    df_2025['YIL'] = 2025
    
    # 2. Birleştir
    progress.progress(50, text="Veriler birleştiriliyor...")
    df = pd.concat([df_2024, df_2025], ignore_index=True)
    df.columns = df.columns.str.strip()
    
    # Bellek temizle
    del df_2024, df_2025
    gc.collect()
    
    # 3. Veri tiplerini optimize et
    progress.progress(60, text="Bellek optimize ediliyor...")
    for col in ['SM', 'BS', 'Mağaza - Orta uzunl.metin', 'Ürün Grubu - Orta uzunl.metin',
                'Malzeme Nitelik - Metin', 'Mal Grubu - Orta uzunl.metin', 
                'Üst Mal Grubu - Orta uzunl.metin', 'Malzeme Tanımı']:
        if col in df.columns:
            df[col] = df[col].astype('category')
    
    for col in ['Satış Miktarı', 'Satış Hasılatı (VD)', 'Net Marj', 'Fire Tutarı', 
                'Envanter Tutarı', 'Toplam Kampanya Zararı']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float32')
    
    # 4. Aggregate tabloları oluştur
    progress.progress(70, text="Özet tablolar hesaplanıyor...")
    aggregates = build_aggregates(df)
    
    # 5. Metrikleri hesapla
    progress.progress(85, text="Metrikler hesaplanıyor...")
    metrics = compute_all_metrics(aggregates)
    
    # 6. Incident'ları tespit et
    progress.progress(95, text="Sorunlar tespit ediliyor...")
    incidents = detect_incidents(metrics)
    
    progress.progress(100, text="Tamamlandı!")
    progress.empty()
    
    return {
        'raw': df,
        'aggregates': aggregates,
        'metrics': metrics,
        'incidents': incidents,
        'filter_options': extract_filter_options(df)
    }


def build_aggregates(df):
    """Tüm aggregate tabloları oluştur"""
    
    agg_cols = {
        'Satış Hasılatı (VD)': 'sum',
        'Satış Miktarı': 'sum',
        'Net Marj': 'sum',
        'Fire Tutarı': 'sum',
        'Envanter Tutarı': 'sum',
        'Toplam Kampanya Zararı': 'sum'
    }
    
    aggregates = {}
    
    # Mağaza bazlı
    aggregates['store'] = df.groupby(['YIL', 'Mağaza - Anahtar', 'SM', 'BS', 
                                       'Mağaza - Orta uzunl.metin']).agg(agg_cols).reset_index()
    
    # Mağaza x Ürün Grubu
    aggregates['store_urun_grubu'] = df.groupby(
        ['YIL', 'Mağaza - Anahtar', 'SM', 'BS', 'Mağaza - Orta uzunl.metin',
         'Ürün Grubu - Orta uzunl.metin']).agg(agg_cols).reset_index()
    
    # Mağaza x Üst Mal Grubu
    aggregates['store_ust_mal'] = df.groupby(
        ['YIL', 'Mağaza - Anahtar', 'SM', 'BS', 'Mağaza - Orta uzunl.metin',
         'Üst Mal Grubu - Orta uzunl.metin']).agg(agg_cols).reset_index()
    
    # Mağaza x Nitelik
    aggregates['store_nitelik'] = df.groupby(
        ['YIL', 'Mağaza - Anahtar', 'SM', 'BS', 'Mağaza - Orta uzunl.metin',
         'Malzeme Nitelik - Metin']).agg(agg_cols).reset_index()
    
    # SM bazlı
    aggregates['sm'] = df.groupby(['YIL', 'SM']).agg(agg_cols).reset_index()
    
    # BS bazlı
    aggregates['bs'] = df.groupby(['YIL', 'SM', 'BS']).agg(agg_cols).reset_index()
    
    # Kategori bazlı (tüm bölge)
    aggregates['urun_grubu'] = df.groupby(['YIL', 'Ürün Grubu - Orta uzunl.metin']).agg(agg_cols).reset_index()
    aggregates['ust_mal_grubu'] = df.groupby(['YIL', 'Üst Mal Grubu - Orta uzunl.metin']).agg(agg_cols).reset_index()
    aggregates['mal_grubu'] = df.groupby(['YIL', 'Mal Grubu - Orta uzunl.metin']).agg(agg_cols).reset_index()
    aggregates['nitelik'] = df.groupby(['YIL', 'Malzeme Nitelik - Metin']).agg(agg_cols).reset_index()
    
    # Genel toplamlar
    aggregates['totals'] = df.groupby('YIL').agg(agg_cols).reset_index()
    
    return aggregates


def extract_filter_options(df):
    """Filtre seçeneklerini çıkar"""
    return {
        'sm': sorted(df['SM'].dropna().unique().tolist()),
        'bs_by_sm': df.groupby('SM')['BS'].apply(lambda x: sorted(x.dropna().unique().tolist())).to_dict(),
        'stores_by_bs': df.groupby('BS').apply(
            lambda x: x[['Mağaza - Anahtar', 'Mağaza - Orta uzunl.metin']].drop_duplicates().values.tolist()
        ).to_dict(),
        'nitelik': sorted(df['Malzeme Nitelik - Metin'].dropna().unique().tolist()),
        'urun_grubu': sorted(df['Ürün Grubu - Orta uzunl.metin'].dropna().unique().tolist()),
        'ust_mal_by_urun': df.groupby('Ürün Grubu - Orta uzunl.metin')['Üst Mal Grubu - Orta uzunl.metin'].apply(
            lambda x: sorted(x.dropna().unique().tolist())).to_dict(),
        'mal_by_ust': df.groupby('Üst Mal Grubu - Orta uzunl.metin')['Mal Grubu - Orta uzunl.metin'].apply(
            lambda x: sorted(x.dropna().unique().tolist())).to_dict(),
    }


# ============================================================================
# METRİK HESAPLAMA
# ============================================================================

def safe_pct_change(new, old):
    """Güvenli yüzde değişim"""
    if pd.isna(old) or pd.isna(new) or old == 0:
        return np.nan
    return ((new / old) - 1) * 100


def compute_all_metrics(aggregates):
    """Tüm metrikleri hesapla"""
    
    metrics = {}
    
    # 1. Mağaza metrikleri
    metrics['store'] = compute_yoy_metrics(
        aggregates['store'], 
        ['Mağaza - Anahtar', 'SM', 'BS', 'Mağaza - Orta uzunl.metin']
    )
    
    # 2. Mağaza x Ürün Grubu metrikleri (incident tespiti için ana tablo)
    metrics['store_urun_grubu'] = compute_yoy_metrics(
        aggregates['store_urun_grubu'],
        ['Mağaza - Anahtar', 'SM', 'BS', 'Mağaza - Orta uzunl.metin', 'Ürün Grubu - Orta uzunl.metin']
    )
    
    # 3. Mağaza x Üst Mal Grubu
    metrics['store_ust_mal'] = compute_yoy_metrics(
        aggregates['store_ust_mal'],
        ['Mağaza - Anahtar', 'SM', 'BS', 'Mağaza - Orta uzunl.metin', 'Üst Mal Grubu - Orta uzunl.metin']
    )
    
    # 4. Mağaza x Nitelik
    metrics['store_nitelik'] = compute_yoy_metrics(
        aggregates['store_nitelik'],
        ['Mağaza - Anahtar', 'SM', 'BS', 'Mağaza - Orta uzunl.metin', 'Malzeme Nitelik - Metin']
    )
    
    # 5. SM metrikleri
    metrics['sm'] = compute_yoy_metrics(aggregates['sm'], ['SM'])
    
    # 6. BS metrikleri
    metrics['bs'] = compute_yoy_metrics(aggregates['bs'], ['SM', 'BS'])
    
    # 7. Kategori metrikleri
    metrics['urun_grubu'] = compute_yoy_metrics(aggregates['urun_grubu'], ['Ürün Grubu - Orta uzunl.metin'])
    metrics['ust_mal_grubu'] = compute_yoy_metrics(aggregates['ust_mal_grubu'], ['Üst Mal Grubu - Orta uzunl.metin'])
    metrics['nitelik'] = compute_yoy_metrics(aggregates['nitelik'], ['Malzeme Nitelik - Metin'])
    
    # 8. Genel toplamlar
    totals = aggregates['totals']
    t2024 = totals[totals['YIL'] == 2024].iloc[0] if len(totals[totals['YIL'] == 2024]) > 0 else None
    t2025 = totals[totals['YIL'] == 2025].iloc[0] if len(totals[totals['YIL'] == 2025]) > 0 else None
    
    if t2024 is not None and t2025 is not None:
        metrics['totals'] = {
            'ciro_2024': t2024['Satış Hasılatı (VD)'],
            'ciro_2025': t2025['Satış Hasılatı (VD)'],
            'ciro_change': safe_pct_change(t2025['Satış Hasılatı (VD)'], t2024['Satış Hasılatı (VD)']),
            'marj_2024': t2024['Net Marj'],
            'marj_2025': t2025['Net Marj'],
            'marj_change': safe_pct_change(t2025['Net Marj'], t2024['Net Marj']),
            'adet_2024': t2024['Satış Miktarı'],
            'adet_2025': t2025['Satış Miktarı'],
            'adet_change': safe_pct_change(t2025['Satış Miktarı'], t2024['Satış Miktarı']),
            'fire_2024': abs(t2024['Fire Tutarı']),
            'fire_2025': abs(t2025['Fire Tutarı']),
            'fire_change': safe_pct_change(abs(t2025['Fire Tutarı']), abs(t2024['Fire Tutarı'])),
            'envanter_2025': abs(t2025['Envanter Tutarı']),
            'kampanya_2025': abs(t2025['Toplam Kampanya Zararı']),
        }
    
    return metrics


def compute_yoy_metrics(agg_df, key_cols):
    """Yıllık karşılaştırma metriklerini hesapla"""
    
    df_2024 = agg_df[agg_df['YIL'] == 2024].copy()
    df_2025 = agg_df[agg_df['YIL'] == 2025].copy()
    
    # Suffix ekle
    df_2024 = df_2024.rename(columns={
        'Satış Hasılatı (VD)': 'Ciro_2024',
        'Satış Miktarı': 'Adet_2024',
        'Net Marj': 'Marj_2024',
        'Fire Tutarı': 'Fire_2024',
        'Envanter Tutarı': 'Envanter_2024',
        'Toplam Kampanya Zararı': 'Kampanya_2024'
    }).drop(columns=['YIL'])
    
    df_2025 = df_2025.rename(columns={
        'Satış Hasılatı (VD)': 'Ciro_2025',
        'Satış Miktarı': 'Adet_2025',
        'Net Marj': 'Marj_2025',
        'Fire Tutarı': 'Fire_2025',
        'Envanter Tutarı': 'Envanter_2025',
        'Toplam Kampanya Zararı': 'Kampanya_2025'
    }).drop(columns=['YIL'])
    
    # Birleştir
    merged = df_2024.merge(df_2025, on=key_cols, how='outer')
    
    # Değişimleri hesapla
    merged['Ciro_Change'] = merged.apply(lambda x: safe_pct_change(x['Ciro_2025'], x['Ciro_2024']), axis=1)
    merged['Marj_Change'] = merged.apply(lambda x: safe_pct_change(x['Marj_2025'], x['Marj_2024']), axis=1)
    merged['Adet_Change'] = merged.apply(lambda x: safe_pct_change(x['Adet_2025'], x['Adet_2024']), axis=1)
    merged['Fire_Change'] = merged.apply(
        lambda x: safe_pct_change(abs(x['Fire_2025']) if pd.notna(x['Fire_2025']) else 0,
                                   abs(x['Fire_2024']) if pd.notna(x['Fire_2024']) else 0), axis=1)
    merged['Envanter_Change'] = merged.apply(
        lambda x: safe_pct_change(abs(x['Envanter_2025']) if pd.notna(x['Envanter_2025']) else 0,
                                   abs(x['Envanter_2024']) if pd.notna(x['Envanter_2024']) else 0), axis=1)
    merged['Kampanya_Change'] = merged.apply(
        lambda x: safe_pct_change(abs(x['Kampanya_2025']) if pd.notna(x['Kampanya_2025']) else 0,
                                   abs(x['Kampanya_2024']) if pd.notna(x['Kampanya_2024']) else 0), axis=1)
    
    # Marj değişimi TL
    merged['Marj_Change_TL'] = merged['Marj_2025'].fillna(0) - merged['Marj_2024'].fillna(0)
    
    # Kayıp oranı
    merged['Loss_2024'] = abs(merged['Fire_2024'].fillna(0)) + abs(merged['Envanter_2024'].fillna(0))
    merged['Loss_2025'] = abs(merged['Fire_2025'].fillna(0)) + abs(merged['Envanter_2025'].fillna(0))
    merged['LossRate_2024'] = merged['Loss_2024'] / merged['Ciro_2024'].replace(0, np.nan) * 100
    merged['LossRate_2025'] = merged['Loss_2025'] / merged['Ciro_2025'].replace(0, np.nan) * 100
    merged['LossRate_Change'] = merged['LossRate_2025'] - merged['LossRate_2024']
    
    return merged


# ============================================================================
# INCIDENT TESPİTİ VE SKORLAMA
# ============================================================================

def detect_incidents(metrics):
    """Sorunlu alanları tespit et ve skorla"""
    
    all_incidents = []
    
    # 1. Mağaza x Ürün Grubu incident'ları
    df = metrics['store_urun_grubu'].copy()
    df = df[df['Ciro_2024'] >= MIN_BASE_SALES_TL]  # Minimum baz filtresi
    
    for _, row in df.iterrows():
        incident = create_incident(row, 'Mağaza-ÜrünGrubu', 
                                    row.get('Ürün Grubu - Orta uzunl.metin', ''))
        if incident and incident['score'] > 20:  # Sadece anlamlı olanları al
            all_incidents.append(incident)
    
    # 2. Mağaza x Üst Mal Grubu incident'ları
    df = metrics['store_ust_mal'].copy()
    df = df[df['Ciro_2024'] >= MIN_BASE_SALES_TL]
    
    for _, row in df.iterrows():
        incident = create_incident(row, 'Mağaza-ÜstMal',
                                    row.get('Üst Mal Grubu - Orta uzunl.metin', ''))
        if incident and incident['score'] > 20:
            all_incidents.append(incident)
    
    # 3. Mağaza x Nitelik incident'ları
    df = metrics['store_nitelik'].copy()
    df = df[df['Ciro_2024'] >= MIN_BASE_SALES_TL]
    
    for _, row in df.iterrows():
        incident = create_incident(row, 'Mağaza-Nitelik',
                                    row.get('Malzeme Nitelik - Metin', ''))
        if incident and incident['score'] > 20:
            all_incidents.append(incident)
    
    # 4. Mağaza seviyesi incident'ları
    df = metrics['store'].copy()
    df = df[df['Ciro_2024'] >= MIN_BASE_SALES_TL]
    
    for _, row in df.iterrows():
        incident = create_incident(row, 'Mağaza', 'Genel')
        if incident and incident['score'] > 25:
            all_incidents.append(incident)
    
    # DataFrame'e çevir ve skorla sırala
    if not all_incidents:
        return pd.DataFrame()
    
    incidents_df = pd.DataFrame(all_incidents)
    incidents_df = incidents_df.sort_values('score', ascending=False).reset_index(drop=True)
    
    return incidents_df


def create_incident(row, level, category):
    """Tek bir incident oluştur"""
    
    # Skorları hesapla (z-score benzeri normalizasyon)
    ciro_drop = -row.get('Ciro_Change', 0) if pd.notna(row.get('Ciro_Change')) else 0
    marj_drop = -row.get('Marj_Change', 0) if pd.notna(row.get('Marj_Change')) else 0
    fire_increase = row.get('Fire_Change', 0) if pd.notna(row.get('Fire_Change')) else 0
    inv_increase = row.get('Envanter_Change', 0) if pd.notna(row.get('Envanter_Change')) else 0
    
    # Negatif değerleri sıfırla (artış/düşüş yönünü zaten ayarladık)
    ciro_drop = max(0, ciro_drop)
    marj_drop = max(0, marj_drop)
    fire_increase = max(0, fire_increase)
    inv_increase = max(0, inv_increase)
    
    # Normalize et (0-100 arasına çek)
    ciro_score = min(100, ciro_drop * 2)  # %50 düşüş = 100 puan
    marj_score = min(100, marj_drop * 2)
    fire_score = min(100, fire_increase / 2)  # %200 artış = 100 puan
    inv_score = min(100, inv_increase / 2)
    
    # Ağırlıklı toplam skor
    total_score = (
        ciro_score * WEIGHTS['share_drop'] / 100 +
        marj_score * WEIGHTS['margin_drop'] / 100 +
        fire_score * WEIGHTS['fire_increase'] / 100 +
        inv_score * WEIGHTS['inv_increase'] / 100
    )
    
    # Neden analizi
    reason, action = analyze_reason(row)
    
    return {
        'score': round(total_score, 1),
        'level': level,
        'sm': row.get('SM', '-'),
        'bs': row.get('BS', '-'),
        'magaza': row.get('Mağaza - Anahtar', '-'),
        'magaza_adi': row.get('Mağaza - Orta uzunl.metin', '-'),
        'kategori': category,
        'ciro_2024': row.get('Ciro_2024', 0),
        'ciro_2025': row.get('Ciro_2025', 0),
        'ciro_change': row.get('Ciro_Change', 0),
        'marj_2024': row.get('Marj_2024', 0),
        'marj_2025': row.get('Marj_2025', 0),
        'marj_change': row.get('Marj_Change', 0),
        'marj_change_tl': row.get('Marj_Change_TL', 0),
        'fire_2024': row.get('Fire_2024', 0),
        'fire_2025': row.get('Fire_2025', 0),
        'fire_change': row.get('Fire_Change', 0),
        'envanter_change': row.get('Envanter_Change', 0),
        'kampanya_change': row.get('Kampanya_Change', 0),
        'loss_rate_2025': row.get('LossRate_2025', 0),
        'reason': reason,
        'action': action
    }


def analyze_reason(row):
    """Otomatik neden tespiti ve aksiyon önerisi"""
    
    kampanya_change = row.get('Kampanya_Change', 0) if pd.notna(row.get('Kampanya_Change')) else 0
    marj_change = row.get('Marj_Change', 0) if pd.notna(row.get('Marj_Change')) else 0
    fire_change = row.get('Fire_Change', 0) if pd.notna(row.get('Fire_Change')) else 0
    envanter_change = row.get('Envanter_Change', 0) if pd.notna(row.get('Envanter_Change')) else 0
    ciro_change = row.get('Ciro_Change', 0) if pd.notna(row.get('Ciro_Change')) else 0
    adet_change = row.get('Adet_Change', 0) if pd.notna(row.get('Adet_Change')) else 0
    
    # Öncelik sırası ile neden tespiti
    if kampanya_change > 50 and marj_change < -10:
        return "Kampanya Zararı", "Kampanya karlılık analizini gözden geçir, düşük marjlı kampanyaları azalt"
    
    if fire_change > 100:
        return "Fire Patlaması", "SKT kontrolü yap, raf düzenini kontrol et, sipariş miktarlarını gözden geçir"
    
    if envanter_change > 100 and ciro_change < -5:
        return "Stok/Envanter Problemi", "Sayım yap, yerleşimi kontrol et, kayıp önleme ekibini bilgilendir"
    
    if fire_change > 50 and envanter_change > 50:
        return "Genel Kayıp Artışı", "Mağaza denetimi planla, operasyonel kontrolleri sıkılaştır"
    
    if adet_change < -20 and ciro_change < -15:
        return "Trafik/Talep Düşüşü", "Ürün bulunurluğunu kontrol et, rekabet analizi yap"
    
    if marj_change < -20 and kampanya_change < 20:
        return "Marj Erimesi", "SMM değişikliği kontrol et, fiyatlama stratejisini gözden geçir"
    
    if ciro_change < -10:
        return "Ciro Düşüşü", "Kategori yönetimini ve ürün karmasını gözden geçir"
    
    return "Genel Performans Düşüşü", "Detaylı analiz gerekli"


# ============================================================================
# BAŞARI TESPİTİ
# ============================================================================

def find_successes(metrics):
    """Başarılı alanları bul (Best Practice)"""
    
    successes = []
    
    df = metrics['store'].copy()
    df = df[df['Ciro_2024'] >= MIN_BASE_SALES_TL]
    
    # Ciro artışı yüksek
    for _, row in df[df['Ciro_Change'] > 25].iterrows():
        successes.append({
            'tip': '📈 Ciro Artışı',
            'alan': f"{row['Mağaza - Anahtar']} - {row['Mağaza - Orta uzunl.metin']}",
            'sm': row['SM'],
            'bs': row['BS'],
            'deger': f"+{row['Ciro_Change']:.1f}%",
            'detay': f"₺{row['Ciro_2024']:,.0f} → ₺{row['Ciro_2025']:,.0f}"
        })
    
    # Marj artışı yüksek
    for _, row in df[df['Marj_Change'] > 30].iterrows():
        successes.append({
            'tip': '💰 Marj Artışı',
            'alan': f"{row['Mağaza - Anahtar']} - {row['Mağaza - Orta uzunl.metin']}",
            'sm': row['SM'],
            'bs': row['BS'],
            'deger': f"+{row['Marj_Change']:.1f}%",
            'detay': f"₺{row['Marj_2024']:,.0f} → ₺{row['Marj_2025']:,.0f}"
        })
    
    # Fire azalması
    for _, row in df[df['Fire_Change'] < -30].iterrows():
        if pd.notna(row['Fire_2024']) and abs(row['Fire_2024']) > 1000:
            successes.append({
                'tip': '🔥 Fire Azalması',
                'alan': f"{row['Mağaza - Anahtar']} - {row['Mağaza - Orta uzunl.metin']}",
                'sm': row['SM'],
                'bs': row['BS'],
                'deger': f"{row['Fire_Change']:.1f}%",
                'detay': f"₺{abs(row['Fire_2024']):,.0f} → ₺{abs(row['Fire_2025']):,.0f}"
            })
    
    return pd.DataFrame(successes) if successes else pd.DataFrame()


# ============================================================================
# EXCEL RAPOR
# ============================================================================

def create_excel_report(data, filter_info=""):
    """6 sekmeli kapsamlı Excel raporu"""
    
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        
        metrics = data['metrics']
        incidents = data['incidents']
        
        # 1. MÜDAHALE HARİTASI
        if not incidents.empty:
            mudahale = incidents.head(50)[['score', 'level', 'sm', 'bs', 'magaza', 'magaza_adi',
                                            'kategori', 'ciro_change', 'marj_change', 'fire_change',
                                            'reason', 'action']].copy()
            mudahale.columns = ['Skor', 'Seviye', 'SM', 'BS', 'Mağaza', 'Mağaza Adı',
                                'Kategori', 'Ciro Değişim %', 'Marj Değişim %', 'Fire Değişim %',
                                'Neden', 'Aksiyon']
            mudahale.to_excel(writer, sheet_name='Müdahale Haritası', index=False)
        
        # 2. MARJ SIZINTISI
        totals = metrics.get('totals', {})
        sizinti_data = [
            ['MARJ SIZINTISI ANALİZİ', '', ''],
            ['', '', ''],
            ['Kaynak', '2025 Tutar (TL)', 'Açıklama'],
            ['Kampanya Zararı', totals.get('kampanya_2025', 0), 'Kampanya kaynaklı kar kaybı'],
            ['Fire Kaybı', totals.get('fire_2025', 0), 'Bozulma ve hurda kayıpları'],
            ['Envanter Kaybı', totals.get('envanter_2025', 0), 'Sayım farkları ve kayıplar'],
            ['', '', ''],
            ['TOPLAM SIZINTI', totals.get('kampanya_2025', 0) + totals.get('fire_2025', 0) + totals.get('envanter_2025', 0), '']
        ]
        pd.DataFrame(sizinti_data).to_excel(writer, sheet_name='Marj Sızıntısı', index=False, header=False)
        
        # 3. GELİŞEN ALANLAR
        successes = find_successes(metrics)
        if not successes.empty:
            successes.to_excel(writer, sheet_name='Gelişen Alanlar', index=False)
        
        # 4. TÜM INCIDENT LİSTESİ
        if not incidents.empty:
            incidents.to_excel(writer, sheet_name='Tüm Incidents', index=False)
        
        # 5. ÜRÜN RÖNTGENİ (Top performanslar)
        if 'raw' in data:
            df_2025 = data['raw'][data['raw']['YIL'] == 2025]
            top_ciro = df_2025.groupby(['Malzeme Kodu', 'Malzeme Tanımı']).agg({
                'Satış Hasılatı (VD)': 'sum',
                'Satış Miktarı': 'sum',
                'Net Marj': 'sum',
                'Fire Tutarı': 'sum'
            }).reset_index().nlargest(50, 'Satış Hasılatı (VD)')
            top_ciro.to_excel(writer, sheet_name='Top 50 Ürün', index=False)
        
        # 6. SM/BS ÖZET
        if 'sm' in metrics:
            metrics['sm'].to_excel(writer, sheet_name='SM Özet', index=False)
        if 'bs' in metrics:
            metrics['bs'].to_excel(writer, sheet_name='BS Özet', index=False)
    
    output.seek(0)
    return output


# ============================================================================
# SIDEBAR
# ============================================================================

def render_sidebar(filter_options):
    """Filtre paneli"""
    
    st.sidebar.markdown("### 🎛️ FİLTRELER")
    
    # SM
    sm_list = ['Tümü'] + filter_options.get('sm', [])
    selected_sm = st.sidebar.selectbox('📊 SM', sm_list)
    
    # BS
    if selected_sm != 'Tümü':
        bs_opts = filter_options.get('bs_by_sm', {}).get(selected_sm, [])
    else:
        bs_opts = list(set([bs for bss in filter_options.get('bs_by_sm', {}).values() for bs in bss]))
    bs_list = ['Tümü'] + sorted(bs_opts)
    selected_bs = st.sidebar.selectbox('👤 BS', bs_list)
    
    st.sidebar.markdown("---")
    
    # Nitelik
    nitelik_list = ['Tümü'] + filter_options.get('nitelik', [])
    selected_nitelik = st.sidebar.selectbox('🏷️ Nitelik', nitelik_list)
    
    # Ürün Grubu
    urun_list = ['Tümü'] + filter_options.get('urun_grubu', [])
    selected_urun = st.sidebar.selectbox('📂 Ürün Grubu', urun_list)
    
    # Üst Mal Grubu
    if selected_urun != 'Tümü':
        ust_opts = filter_options.get('ust_mal_by_urun', {}).get(selected_urun, [])
    else:
        ust_opts = list(set([u for us in filter_options.get('ust_mal_by_urun', {}).values() for u in us]))
    ust_list = ['Tümü'] + sorted(ust_opts)
    selected_ust = st.sidebar.selectbox('📁 Üst Mal Grubu', ust_list)
    
    st.sidebar.markdown("---")
    
    # Ayarlar
    st.sidebar.markdown("### ⚙️ AYARLAR")
    min_base = st.sidebar.number_input('Min. Baz Satış (TL)', value=MIN_BASE_SALES_TL, step=1000)
    
    if st.sidebar.button('🔄 Temizle', use_container_width=True):
        st.rerun()
    
    return {
        'sm': selected_sm,
        'bs': selected_bs,
        'nitelik': selected_nitelik,
        'urun_grubu': selected_urun,
        'ust_mal_grubu': selected_ust,
        'min_base': min_base
    }


def apply_filters_to_incidents(incidents, filters):
    """Filtreleri incident'lara uygula"""
    
    if incidents.empty:
        return incidents
    
    filtered = incidents.copy()
    
    if filters['sm'] != 'Tümü':
        filtered = filtered[filtered['sm'] == filters['sm']]
    
    if filters['bs'] != 'Tümü':
        filtered = filtered[filtered['bs'] == filters['bs']]
    
    if filters['nitelik'] != 'Tümü':
        filtered = filtered[filtered['kategori'] == filters['nitelik']]
    
    if filters['urun_grubu'] != 'Tümü':
        filtered = filtered[filtered['kategori'] == filters['urun_grubu']]
    
    if filters['ust_mal_grubu'] != 'Tümü':
        filtered = filtered[filtered['kategori'] == filters['ust_mal_grubu']]
    
    return filtered


# ============================================================================
# ANA EKRAN BİLEŞENLERİ
# ============================================================================

def render_kpis(totals):
    """KPI kartları"""
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        delta_class = "kpi-delta-pos" if totals.get('ciro_change', 0) > 0 else "kpi-delta-neg"
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">💰 Toplam Ciro</div>
            <div class="kpi-value">₺{totals.get('ciro_2025', 0):,.0f}</div>
            <div class="{delta_class}">{'+' if totals.get('ciro_change', 0) > 0 else ''}{totals.get('ciro_change', 0):.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        delta_class = "kpi-delta-pos" if totals.get('marj_change', 0) > 0 else "kpi-delta-neg"
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">📈 Toplam Marj</div>
            <div class="kpi-value">₺{totals.get('marj_2025', 0):,.0f}</div>
            <div class="{delta_class}">{'+' if totals.get('marj_change', 0) > 0 else ''}{totals.get('marj_change', 0):.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        delta_class = "kpi-delta-pos" if totals.get('adet_change', 0) > 0 else "kpi-delta-neg"
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">📦 Satış Adedi</div>
            <div class="kpi-value">{totals.get('adet_2025', 0):,.0f}</div>
            <div class="{delta_class}">{'+' if totals.get('adet_change', 0) > 0 else ''}{totals.get('adet_change', 0):.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        delta_class = "kpi-delta-neg" if totals.get('fire_change', 0) > 0 else "kpi-delta-pos"
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">🔥 Fire Kaybı</div>
            <div class="kpi-value">₺{totals.get('fire_2025', 0):,.0f}</div>
            <div class="{delta_class}">{'+' if totals.get('fire_change', 0) > 0 else ''}{totals.get('fire_change', 0):.1f}%</div>
        </div>
        """, unsafe_allow_html=True)


def render_incident_cards(incidents, max_cards=5):
    """Acil müdahale kartlarını göster"""
    
    st.markdown('<p class="section-header">🚨 ACİL MÜDAHALE GEREKTİREN ALANLAR</p>', unsafe_allow_html=True)
    
    if incidents.empty:
        st.success("✅ Kritik seviyede müdahale gerektiren alan tespit edilmedi!")
        return
    
    for idx, row in incidents.head(max_cards).iterrows():
        score = row['score']
        
        # Skor badge rengi
        if score >= 60:
            score_class = "score-critical"
            card_class = "incident-card"
        elif score >= 40:
            score_class = "score-warning"
            card_class = "incident-card incident-card-warning"
        else:
            score_class = "score-low"
            card_class = "incident-card incident-card-warning"
        
        st.markdown(f"""
        <div class="{card_class}">
            <div style="display: flex; justify-content: space-between; align-items: start;">
                <div>
                    <span class="score-badge {score_class}">Skor: {score:.0f}</span>
                    <span class="reason-tag">{row['reason']}</span>
                </div>
            </div>
            <div style="margin-top: 0.5rem;">
                <strong>{row['level']}</strong>: {row['magaza']} - {row['magaza_adi']}<br>
                <small>SM: {row['sm']} | BS: {row['bs']} | Kategori: {row['kategori']}</small>
            </div>
            <div style="margin-top: 0.5rem; font-size: 0.9rem;">
                📊 Ciro: <span style="color: {'#ef4444' if row['ciro_change'] < 0 else '#10b981'}">{row['ciro_change']:+.1f}%</span> | 
                💰 Marj: <span style="color: {'#ef4444' if row['marj_change'] < 0 else '#10b981'}">{row['marj_change']:+.1f}%</span> | 
                🔥 Fire: <span style="color: {'#ef4444' if row['fire_change'] > 0 else '#10b981'}">{row['fire_change']:+.1f}%</span>
            </div>
            <div class="action-box">
                💡 <strong>Öneri:</strong> {row['action']}
            </div>
        </div>
        """, unsafe_allow_html=True)


def render_leakage_analysis(totals):
    """Marj sızıntısı analizi"""
    
    st.markdown('<p class="section-header">💸 MARJ SIZINTISI ANALİZİ</p>', unsafe_allow_html=True)
    
    kampanya = totals.get('kampanya_2025', 0)
    fire = totals.get('fire_2025', 0)
    envanter = totals.get('envanter_2025', 0)
    total_leak = kampanya + fire + envanter
    
    if total_leak == 0:
        st.info("Kayda değer sızıntı tespit edilmedi")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("TOPLAM SIZINTI", f"₺{total_leak:,.0f}")
    
    with col2:
        pct = (kampanya / total_leak * 100) if total_leak > 0 else 0
        st.metric("Kampanya Zararı", f"₺{kampanya:,.0f}", f"{pct:.0f}%")
    
    with col3:
        pct = (fire / total_leak * 100) if total_leak > 0 else 0
        st.metric("Fire Kaybı", f"₺{fire:,.0f}", f"{pct:.0f}%")
    
    with col4:
        pct = (envanter / total_leak * 100) if total_leak > 0 else 0
        st.metric("Envanter Kaybı", f"₺{envanter:,.0f}", f"{pct:.0f}%")
    
    # En büyük sızıntı kaynağını belirle
    max_source = max([('Kampanya', kampanya), ('Fire', fire), ('Envanter', envanter)], key=lambda x: x[1])
    if max_source[1] > 0:
        st.warning(f"⚠️ En büyük sızıntı kaynağı: **{max_source[0]}** (₺{max_source[1]:,.0f})")


def render_success_cards(metrics):
    """Başarı hikayelerini göster"""
    
    st.markdown('<p class="section-header">🌟 GELİŞEN ALANLAR (Best Practice)</p>', unsafe_allow_html=True)
    
    successes = find_successes(metrics)
    
    if successes.empty:
        st.info("📊 Öne çıkan gelişim alanı tespit edilmedi")
        return
    
    for _, row in successes.head(5).iterrows():
        st.markdown(f"""
        <div class="incident-card incident-card-success">
            <strong>{row['tip']}</strong><br>
            {row['alan']}<br>
            <small>SM: {row['sm']} | BS: {row['bs']}</small><br>
            <strong style="color: #10b981;">{row['deger']}</strong> ({row['detay']})
        </div>
        """, unsafe_allow_html=True)


def render_detail_tables(data, filters):
    """Detay tabloları"""
    
    st.markdown('<p class="section-header">📋 DETAY TABLOLARI</p>', unsafe_allow_html=True)
    
    tabs = st.tabs(["📊 Tüm Incidents", "🏪 Mağaza", "👥 SM/BS", "📦 Kategori"])
    
    with tabs[0]:
        incidents = data['incidents']
        if not incidents.empty:
            filtered = apply_filters_to_incidents(incidents, filters)
            display_cols = ['score', 'level', 'sm', 'bs', 'magaza_adi', 'kategori',
                           'ciro_change', 'marj_change', 'fire_change', 'reason']
            st.dataframe(filtered[display_cols].head(100), use_container_width=True, hide_index=True)
        else:
            st.info("Incident bulunamadı")
    
    with tabs[1]:
        store_metrics = data['metrics'].get('store', pd.DataFrame())
        if not store_metrics.empty:
            display = store_metrics[['Mağaza - Anahtar', 'Mağaza - Orta uzunl.metin', 'SM', 'BS',
                                      'Ciro_2024', 'Ciro_2025', 'Ciro_Change',
                                      'Marj_Change', 'Fire_Change']].sort_values('Ciro_Change')
            st.dataframe(display, use_container_width=True, hide_index=True)
    
    with tabs[2]:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**SM Performans**")
            sm = data['metrics'].get('sm', pd.DataFrame())
            if not sm.empty:
                st.dataframe(sm.sort_values('Ciro_Change'), use_container_width=True, hide_index=True)
        with col2:
            st.markdown("**BS Performans**")
            bs = data['metrics'].get('bs', pd.DataFrame())
            if not bs.empty:
                st.dataframe(bs.sort_values('Ciro_Change'), use_container_width=True, hide_index=True)
    
    with tabs[3]:
        cat_tabs = st.tabs(["Ürün Grubu", "Üst Mal Grubu", "Nitelik"])
        with cat_tabs[0]:
            ug = data['metrics'].get('urun_grubu', pd.DataFrame())
            if not ug.empty:
                st.dataframe(ug.sort_values('Ciro_Change'), use_container_width=True, hide_index=True)
        with cat_tabs[1]:
            um = data['metrics'].get('ust_mal_grubu', pd.DataFrame())
            if not um.empty:
                st.dataframe(um.sort_values('Ciro_Change'), use_container_width=True, hide_index=True)
        with cat_tabs[2]:
            nt = data['metrics'].get('nitelik', pd.DataFrame())
            if not nt.empty:
                st.dataframe(nt.sort_values('Ciro_Change'), use_container_width=True, hide_index=True)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Ana uygulama"""
    
    st.markdown('<h1 class="main-header">🩺 Bölgesel Performans Röntgeni v2</h1>', unsafe_allow_html=True)
    st.caption("Sorun Bulucu / Müdahale Haritası | Kasım 2024 → Kasım 2025")
    
    # Dosya yükleme
    col1, col2 = st.columns(2)
    with col1:
        file_2024 = st.file_uploader("📁 2024 Kasım Verisi", type=['xlsx'], key='f2024')
    with col2:
        file_2025 = st.file_uploader("📁 2025 Kasım Verisi", type=['xlsx'], key='f2025')
    
    if not file_2024 or not file_2025:
        st.info("👆 Her iki dosyayı da yükleyin")
        
        st.markdown("""
        ### 🩺 Bu Dashboard Ne Yapar?
        
        **Rapor değil, TEŞHİS aracı.** Tıpkı doktorun MR'a bakıp "sorun burada" demesi gibi.
        
        **Özellikler:**
        - 🎯 **Incident Scoring**: Her sorun 0-100 puan alır
        - 🔍 **Otomatik Neden Tespiti**: Kampanya mı? Fire mı? Envanter mi?
        - 💡 **Aksiyon Önerileri**: Ne yapmalısın?
        - 📊 **Minimum Baz Filtresi**: Küçük bazlar seni yanıltmaz
        - 📥 **6 Sekmeli Excel Rapor**: Al, yöneticiye gönder
        
        **Skor Ağırlıkları:**
        - Ciro Payı Düşüşü: %35
        - Marj Erimesi: %25
        - Fire Artışı: %20
        - Envanter Artışı: %20
        """)
        return
    
    # Veri yükleme
    data = load_and_process_data(
        file_2024.getvalue(), file_2025.getvalue(),
        file_2024.name, file_2025.name
    )
    
    if data is None:
        st.error("Veri yüklenemedi!")
        return
    
    # Filtreler
    filters = render_sidebar(data['filter_options'])
    
    # Filtrelenmiş incident'lar
    filtered_incidents = apply_filters_to_incidents(data['incidents'], filters)
    
    # Excel rapor butonu
    excel = create_excel_report(data)
    st.download_button(
        "📥 EXCEL RAPORU İNDİR (6 Sekme)",
        excel,
        f"mudahale_haritasi_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
    
    st.markdown("---")
    
    # KPI'lar
    render_kpis(data['metrics'].get('totals', {}))
    
    st.markdown("---")
    
    # Ana içerik - 2 sütun
    col1, col2 = st.columns([3, 2])
    
    with col1:
        render_incident_cards(filtered_incidents, max_cards=5)
    
    with col2:
        render_leakage_analysis(data['metrics'].get('totals', {}))
        st.markdown("---")
        render_success_cards(data['metrics'])
    
    st.markdown("---")
    
    # Detay tabloları
    render_detail_tables(data, filters)
    
    # Footer
    st.markdown("---")
    st.caption(f"Min. Baz Eşiği: ₺{MIN_BASE_SALES_TL:,} | Son güncelleme: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
