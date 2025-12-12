"""
🎯 SATIŞ KARAR SİSTEMİ v5
━━━━━━━━━━━━━━━━━━━━━━━━
Bu bir dashboard değil, KARAR sistemi.
3 dakikada teşhis, neden, aksiyon.

Mimari: Excel → Parquet → DuckDB → Karar
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import tempfile
import os
from io import BytesIO
import warnings
import gc

warnings.filterwarnings('ignore')

# ============================================================================
# SAYFA AYARLARI
# ============================================================================
st.set_page_config(
    page_title="Satış Karar Sistemi",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# SABİTLER
# ============================================================================

# Sadece bu nitelikler analiz edilecek
GECERLI_NITELIKLER = ['Spot', 'Grup Spot', 'Regule', 'Kasa Aktivitesi', 'Bölgesel']

# Kolon eşleştirme (Excel'deki isim → kısa isim)
KOLON_MAP = {
    'SM': 'SM',
    'BS': 'BS',
    'Mağaza - Anahtar': 'Magaza_Kod',
    'Mağaza - Orta uzunl.metin': 'Magaza_Ad',
    'Malzeme Nitelik - Metin': 'Nitelik',
    'Ürün Grubu - Orta uzunl.metin': 'Urun_Grubu',
    'Üst Mal Grubu - Orta uzunl.metin': 'Ust_Mal',
    'Mal Grubu - Orta uzunl.metin': 'Mal_Grubu',
    'Malzeme Kodu': 'Urun_Kod',
    'Malzeme Tanımı': 'Urun_Ad',
    'Satış Miktarı': 'Adet',
    'Satış Hasılatı (VD)': 'Ciro',
    'Net Marj': 'Marj',
    'Fire Tutarı': 'Fire',
    'Envanter Tutarı': 'Envanter',
    'Toplam Kampanya Zararı': 'Kampanya_Zarar'
}

# Okunacak kolonlar (Excel'deki isimler)
GEREKLI_KOLONLAR = list(KOLON_MAP.keys())

# Numerik kolonlar
NUMERIK_KOLONLAR = ['Adet', 'Ciro', 'Marj', 'Fire', 'Envanter', 'Kampanya_Zarar']

# ============================================================================
# CSS
# ============================================================================
st.markdown("""
<style>
    .main-title {
        font-size: 2rem;
        font-weight: 700;
        color: #1e293b;
        margin-bottom: 0;
    }
    .sub-title {
        font-size: 1rem;
        color: #64748b;
        margin-bottom: 1.5rem;
    }
    
    /* KPI Kartları */
    .kpi-row {
        display: flex;
        gap: 1rem;
        margin: 1rem 0;
    }
    .kpi-card {
        flex: 1;
        background: white;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 1.25rem;
        text-align: center;
    }
    .kpi-label {
        font-size: 0.8rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .kpi-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: #1e293b;
        margin: 0.25rem 0;
    }
    .kpi-delta {
        font-size: 0.9rem;
        font-weight: 600;
    }
    .delta-up { color: #10b981; }
    .delta-down { color: #ef4444; }
    
    /* Karar Kartları */
    .karar-card {
        background: white;
        border: 1px solid #e2e8f0;
        border-left: 4px solid #3b82f6;
        border-radius: 0 12px 12px 0;
        padding: 1rem;
        margin: 0.75rem 0;
    }
    .karar-card-red { border-left-color: #ef4444; background: #fef2f2; }
    .karar-card-green { border-left-color: #10b981; background: #f0fdf4; }
    .karar-card-yellow { border-left-color: #f59e0b; background: #fffbeb; }
    
    .karar-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.5rem;
    }
    .karar-title {
        font-weight: 600;
        color: #1e293b;
    }
    .karar-badge {
        font-size: 0.75rem;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-weight: 500;
    }
    .badge-red { background: #fee2e2; color: #dc2626; }
    .badge-green { background: #dcfce7; color: #16a34a; }
    .badge-yellow { background: #fef3c7; color: #d97706; }
    
    .karar-metrics {
        font-size: 0.85rem;
        color: #475569;
        margin: 0.5rem 0;
    }
    .karar-neden {
        font-size: 0.85rem;
        background: #f1f5f9;
        padding: 0.5rem;
        border-radius: 6px;
        margin-top: 0.5rem;
    }
    .karar-aksiyon {
        font-size: 0.85rem;
        color: #0369a1;
        margin-top: 0.5rem;
    }
    
    /* Section */
    .section-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #334155;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e2e8f0;
        margin: 1.5rem 0 1rem 0;
    }
    
    /* Filtre Info */
    .filter-badge {
        display: inline-block;
        background: #f1f5f9;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-size: 0.85rem;
        color: #475569;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# 1. VERİ OKUMA VE PARQUET DÖNÜŞÜMÜ
# ============================================================================

def get_temp_path():
    """Geçici dosya yolu oluştur"""
    return os.path.join(tempfile.gettempdir(), "karar_sistemi")


def excel_to_parquet(file_bytes: bytes, yil: int, temp_dir: str) -> str:
    """
    Excel'i oku → optimize et → Parquet'e yaz
    SADECE 1 KEZ ÇALIŞIR
    """
    
    # Parquet dosya yolu
    parquet_path = os.path.join(temp_dir, f"veri_{yil}.parquet")
    
    # Excel'i oku - SADECE gerekli kolonlar
    df = pd.read_excel(
        BytesIO(file_bytes),
        engine='openpyxl',
        usecols=lambda x: x.strip() in GEREKLI_KOLONLAR
    )
    
    # Kolon isimlerini temizle ve kısalt
    df.columns = df.columns.str.strip()
    df = df.rename(columns=KOLON_MAP)
    
    # YIL ekle
    df['Yil'] = yil
    
    # Nitelik filtresi - sadece geçerli nitelikler
    if 'Nitelik' in df.columns:
        df = df[df['Nitelik'].isin(GECERLI_NITELIKLER)]
    
    # Numerik kolonları optimize et (float32)
    for col in NUMERIK_KOLONLAR:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float32')
    
    # String kolonları temizle
    string_cols = ['SM', 'BS', 'Magaza_Kod', 'Magaza_Ad', 'Nitelik', 
                   'Urun_Grubu', 'Ust_Mal', 'Mal_Grubu', 'Urun_Kod', 'Urun_Ad']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['nan', 'None', 'NaN', '<NA>'], '')
    
    # Parquet'e yaz
    df.to_parquet(parquet_path, engine='pyarrow', index=False)
    
    satir_sayisi = len(df)
    del df
    gc.collect()
    
    return parquet_path, satir_sayisi


@st.cache_data(ttl=3600, show_spinner=False)
def veri_yukle(bytes_2024: bytes, bytes_2025: bytes, cache_key: str) -> dict:
    """
    Ana veri yükleme fonksiyonu
    Excel → Parquet → Filtre seçenekleri
    """
    
    # Temp klasör
    temp_dir = get_temp_path()
    os.makedirs(temp_dir, exist_ok=True)
    
    progress = st.progress(0, text="📂 2024 verisi okunuyor...")
    
    # 2024 Excel → Parquet
    path_2024, sayi_2024 = excel_to_parquet(bytes_2024, 2024, temp_dir)
    
    progress.progress(45, text="📂 2025 verisi okunuyor...")
    
    # 2025 Excel → Parquet
    path_2025, sayi_2025 = excel_to_parquet(bytes_2025, 2025, temp_dir)
    
    progress.progress(80, text="🔧 Filtre seçenekleri hazırlanıyor...")
    
    # DuckDB ile filtre seçeneklerini al
    con = duckdb.connect()
    
    # View oluştur
    con.execute(f"""
        CREATE VIEW veri AS
        SELECT * FROM parquet_scan('{path_2024}')
        UNION ALL
        SELECT * FROM parquet_scan('{path_2025}')
    """)
    
    # Filtre seçenekleri
    filtreler = {}
    
    filtreler['sm'] = con.execute(
        "SELECT DISTINCT SM FROM veri WHERE SM != '' ORDER BY SM"
    ).fetchdf()['SM'].tolist()
    
    filtreler['nitelik'] = con.execute(
        "SELECT DISTINCT Nitelik FROM veri WHERE Nitelik != '' ORDER BY Nitelik"
    ).fetchdf()['Nitelik'].tolist()
    
    # BS by SM
    bs_df = con.execute(
        "SELECT DISTINCT SM, BS FROM veri WHERE BS != '' ORDER BY SM, BS"
    ).fetchdf()
    filtreler['bs_map'] = bs_df.groupby('SM')['BS'].apply(list).to_dict()
    
    # Mağaza by BS
    mag_df = con.execute("""
        SELECT DISTINCT BS, Magaza_Kod, Magaza_Ad 
        FROM veri WHERE Magaza_Kod != '' 
        ORDER BY BS, Magaza_Kod
    """).fetchdf()
    filtreler['magaza_map'] = mag_df.groupby('BS').apply(
        lambda x: list(zip(x['Magaza_Kod'], x['Magaza_Ad']))
    ).to_dict()
    
    # Ürün Grubu
    filtreler['urun_grubu'] = con.execute(
        "SELECT DISTINCT Urun_Grubu FROM veri WHERE Urun_Grubu != '' ORDER BY Urun_Grubu"
    ).fetchdf()['Urun_Grubu'].tolist()
    
    # Üst Mal by Ürün Grubu
    ust_df = con.execute("""
        SELECT DISTINCT Urun_Grubu, Ust_Mal 
        FROM veri WHERE Ust_Mal != '' 
        ORDER BY Urun_Grubu, Ust_Mal
    """).fetchdf()
    filtreler['ust_mal_map'] = ust_df.groupby('Urun_Grubu')['Ust_Mal'].apply(list).to_dict()
    
    # Mal Grubu by Üst Mal
    mal_df = con.execute("""
        SELECT DISTINCT Ust_Mal, Mal_Grubu 
        FROM veri WHERE Mal_Grubu != '' 
        ORDER BY Ust_Mal, Mal_Grubu
    """).fetchdf()
    filtreler['mal_grubu_map'] = mal_df.groupby('Ust_Mal')['Mal_Grubu'].apply(list).to_dict()
    
    con.close()
    
    progress.progress(100, text="✅ Hazır!")
    progress.empty()
    
    return {
        'path_2024': path_2024,
        'path_2025': path_2025,
        'filtreler': filtreler,
        'sayilar': {'2024': sayi_2024, '2025': sayi_2025}
    }


# ============================================================================
# 2. DUCKDB SORGULARI
# ============================================================================

def get_duckdb_connection(path_2024: str, path_2025: str):
    """DuckDB bağlantısı oluştur"""
    con = duckdb.connect()
    con.execute(f"""
        CREATE VIEW veri AS
        SELECT * FROM parquet_scan('{path_2024}')
        UNION ALL
        SELECT * FROM parquet_scan('{path_2025}')
    """)
    return con


def build_where(filtreler: dict) -> str:
    """Filtre koşullarını SQL WHERE'e çevir"""
    
    kosullar = []
    
    if filtreler.get('sm') and filtreler['sm'] != 'Tümü':
        kosullar.append(f"SM = '{filtreler['sm']}'")
    
    if filtreler.get('bs') and filtreler['bs'] != 'Tümü':
        kosullar.append(f"BS = '{filtreler['bs']}'")
    
    if filtreler.get('magaza') and filtreler['magaza'] != 'Tümü':
        kosullar.append(f"Magaza_Kod = '{filtreler['magaza']}'")
    
    if filtreler.get('nitelik') and filtreler['nitelik'] != 'Tümü':
        kosullar.append(f"Nitelik = '{filtreler['nitelik']}'")
    
    if filtreler.get('urun_grubu') and filtreler['urun_grubu'] != 'Tümü':
        kosullar.append(f"Urun_Grubu = '{filtreler['urun_grubu']}'")
    
    if filtreler.get('ust_mal') and filtreler['ust_mal'] != 'Tümü':
        kosullar.append(f"Ust_Mal = '{filtreler['ust_mal']}'")
    
    if filtreler.get('mal_grubu') and filtreler['mal_grubu'] != 'Tümü':
        kosullar.append(f"Mal_Grubu = '{filtreler['mal_grubu']}'")
    
    return "WHERE " + " AND ".join(kosullar) if kosullar else ""


def get_ozet_kpiler(con, where: str) -> dict:
    """Özet KPI'ları getir"""
    
    sql = f"""
        SELECT 
            Yil,
            SUM(Adet) as Adet,
            SUM(Ciro) as Ciro,
            SUM(Marj) as Marj,
            SUM(ABS(Fire)) as Fire,
            SUM(ABS(Kampanya_Zarar)) as Kampanya
        FROM veri
        {where}
        GROUP BY Yil
    """
    
    df = con.execute(sql).fetchdf()
    
    sonuc = {}
    for _, row in df.iterrows():
        yil = int(row['Yil'])
        for col in ['Adet', 'Ciro', 'Marj', 'Fire', 'Kampanya']:
            sonuc[f'{col.lower()}_{yil}'] = row[col]
    
    # Değişim hesapla
    for m in ['adet', 'ciro', 'marj', 'fire', 'kampanya']:
        v24 = sonuc.get(f'{m}_2024', 0) or 0
        v25 = sonuc.get(f'{m}_2025', 0) or 0
        if v24 > 0:
            sonuc[f'{m}_degisim'] = ((v25 / v24) - 1) * 100
        else:
            sonuc[f'{m}_degisim'] = 0
    
    return sonuc


def get_mal_grubu_analiz(con, where: str, min_ciro: float, limit: int = 10) -> pd.DataFrame:
    """
    Mal Grubu bazında analiz
    5 soruya cevap verecek metrikler
    """
    
    # Ciro limiti koşulu
    ciro_kosul = f"AND Ciro_2025 >= {min_ciro}" if min_ciro > 0 else ""
    
    sql = f"""
        WITH yillik AS (
            SELECT 
                Mal_Grubu,
                MAX(Ust_Mal) as Ust_Mal,
                Yil,
                SUM(Adet) as Adet,
                SUM(Ciro) as Ciro,
                SUM(Marj) as Marj,
                SUM(ABS(Fire)) as Fire,
                SUM(ABS(Kampanya_Zarar)) as Kampanya
            FROM veri
            {where}
            GROUP BY Mal_Grubu, Yil
        ),
        pivot AS (
            SELECT 
                Mal_Grubu,
                MAX(Ust_Mal) as Ust_Mal,
                SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
                SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
                SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
                SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
                SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
                SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
                SUM(CASE WHEN Yil=2024 THEN Fire ELSE 0 END) as Fire_2024,
                SUM(CASE WHEN Yil=2025 THEN Fire ELSE 0 END) as Fire_2025,
                SUM(CASE WHEN Yil=2024 THEN Kampanya ELSE 0 END) as Kampanya_2024,
                SUM(CASE WHEN Yil=2025 THEN Kampanya ELSE 0 END) as Kampanya_2025
            FROM yillik
            GROUP BY Mal_Grubu
        ),
        toplam AS (
            SELECT 
                SUM(Ciro_2024) as T_Ciro_2024,
                SUM(Ciro_2025) as T_Ciro_2025
            FROM pivot
        )
        SELECT 
            p.*,
            -- Pay hesabı
            CASE WHEN t.T_Ciro_2024 > 0 THEN p.Ciro_2024 / t.T_Ciro_2024 * 100 ELSE 0 END as Pay_2024,
            CASE WHEN t.T_Ciro_2025 > 0 THEN p.Ciro_2025 / t.T_Ciro_2025 * 100 ELSE 0 END as Pay_2025,
            -- Değişim hesapları
            CASE WHEN p.Adet_2024 > 0 THEN ((p.Adet_2025 / p.Adet_2024) - 1) * 100 ELSE 0 END as Adet_Deg,
            CASE WHEN p.Ciro_2024 > 0 THEN ((p.Ciro_2025 / p.Ciro_2024) - 1) * 100 ELSE 0 END as Ciro_Deg,
            CASE WHEN p.Marj_2024 > 0 THEN ((p.Marj_2025 / p.Marj_2024) - 1) * 100 ELSE 0 END as Marj_Deg,
            CASE WHEN p.Fire_2024 > 0 THEN ((p.Fire_2025 / p.Fire_2024) - 1) * 100 ELSE 0 END as Fire_Deg,
            CASE WHEN p.Kampanya_2024 > 0 THEN ((p.Kampanya_2025 / p.Kampanya_2024) - 1) * 100 ELSE 0 END as Kampanya_Deg,
            -- Marj oranı
            CASE WHEN p.Ciro_2024 > 0 THEN p.Marj_2024 / p.Ciro_2024 * 100 ELSE 0 END as Marj_Oran_2024,
            CASE WHEN p.Ciro_2025 > 0 THEN p.Marj_2025 / p.Ciro_2025 * 100 ELSE 0 END as Marj_Oran_2025
        FROM pivot p, toplam t
        WHERE p.Mal_Grubu != ''
        {ciro_kosul}
        ORDER BY p.Ciro_2025 DESC
    """
    
    return con.execute(sql).fetchdf()


def get_urun_detay(con, mal_grubu: str, where: str) -> pd.DataFrame:
    """Mal grubu içindeki ürün detayları"""
    
    mal_kosul = f"Mal_Grubu = '{mal_grubu}'"
    if where:
        full_where = f"{where} AND {mal_kosul}"
    else:
        full_where = f"WHERE {mal_kosul}"
    
    sql = f"""
        WITH yillik AS (
            SELECT 
                Urun_Kod,
                MAX(Urun_Ad) as Urun_Ad,
                Yil,
                SUM(Adet) as Adet,
                SUM(Ciro) as Ciro,
                SUM(Marj) as Marj,
                SUM(ABS(Fire)) as Fire
            FROM veri
            {full_where}
            GROUP BY Urun_Kod, Yil
        )
        SELECT 
            Urun_Kod,
            MAX(Urun_Ad) as Urun_Ad,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2025 THEN Fire ELSE 0 END) as Fire_2025,
            CASE WHEN SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) > 0 
                 THEN ((SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) / 
                        SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END)) - 1) * 100 
                 ELSE 0 END as Adet_Deg
        FROM yillik
        GROUP BY Urun_Kod
        ORDER BY Adet_2025 DESC
    """
    
    return con.execute(sql).fetchdf()


# ============================================================================
# 3. OTOMATİK YORUM MOTORU
# ============================================================================

def neden_tespit(row: pd.Series) -> tuple:
    """
    Otomatik neden tespiti ve aksiyon önerisi
    Returns: (neden, aksiyon, renk)
    """
    
    marj_deg = row.get('Marj_Deg', 0) or 0
    adet_deg = row.get('Adet_Deg', 0) or 0
    fire_deg = row.get('Fire_Deg', 0) or 0
    kampanya_deg = row.get('Kampanya_Deg', 0) or 0
    pay_2024 = row.get('Pay_2024', 0) or 0
    pay_2025 = row.get('Pay_2025', 0) or 0
    pay_degisim = pay_2025 - pay_2024
    
    # Kural 1: Kampanya zararı artıyor + Marj düşüyor
    if kampanya_deg > 30 and marj_deg < -10:
        return (
            "🏷️ Kampanya kaynaklı marj erimesi",
            "Kampanya karlılığını analiz et, düşük marjlı promosyonları azalt",
            "yellow"
        )
    
    # Kural 2: Fire patlıyor
    if fire_deg > 50:
        return (
            "🔥 Fire artışı kritik",
            "SKT kontrolü yap, sipariş miktarlarını ve raf düzenini gözden geçir",
            "red"
        )
    
    # Kural 3: Satış düşüyor + Fire yok
    if adet_deg < -15 and fire_deg < 20:
        return (
            "📦 Bulunurluk/yerleşim problemi",
            "Raf yerleşimini kontrol et, ürün bulunurluğunu sorgula",
            "yellow"
        )
    
    # Kural 4: Satış düşüyor + Fire artıyor
    if adet_deg < -10 and fire_deg > 30:
        return (
            "⚠️ Stok/SKT problemi",
            "Sipariş miktarlarını düşür, fire takibini sıkılaştır",
            "red"
        )
    
    # Kural 5: Pay kaybı
    if pay_degisim < -1 and adet_deg < 0:
        return (
            "📉 Pazar payı kaybı",
            "Kategori yönetimini ve fiyatlamayı gözden geçir",
            "yellow"
        )
    
    # Kural 6: Marj erimesi (diğer nedenlerden bağımsız)
    if marj_deg < -20:
        return (
            "💰 Marj erimesi",
            "SMM değişikliği ve fiyatlama stratejisini kontrol et",
            "yellow"
        )
    
    # Kural 7: Pozitif - Çok satan & pay kazanan
    if adet_deg > 20 and pay_degisim > 0.5:
        return (
            "✅ Başarılı performans",
            "Bu ürün grubunun başarı faktörlerini diğer kategorilere uygula",
            "green"
        )
    
    # Kural 8: Az satan ama pay kazanan
    if adet_deg < 0 and pay_degisim > 0:
        return (
            "🎯 Nispi başarı",
            "Genel düşüşe rağmen pay artışı var, analiz et",
            "green"
        )
    
    # Default
    if adet_deg < 0:
        return ("📊 Performans düşüşü", "Detaylı analiz gerekli", "yellow")
    else:
        return ("📊 Normal performans", "-", "green")


# ============================================================================
# 4. EXCEL RAPOR
# ============================================================================

def excel_rapor_olustur(con, where: str, min_ciro: float, filtre_aciklama: str) -> BytesIO:
    """Filtreye göre Excel raporu oluştur"""
    
    output = BytesIO()
    
    # Analiz verisini al
    df_analiz = get_mal_grubu_analiz(con, where, min_ciro, limit=1000)
    
    # Neden ve aksiyon ekle
    if not df_analiz.empty:
        df_analiz['Neden'] = ''
        df_analiz['Aksiyon'] = ''
        for idx, row in df_analiz.iterrows():
            neden, aksiyon, _ = neden_tespit(row)
            df_analiz.at[idx, 'Neden'] = neden
            df_analiz.at[idx, 'Aksiyon'] = aksiyon
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Sayfa 1: Özet Bilgi
        ozet = pd.DataFrame([{
            'Filtre': filtre_aciklama,
            'Min Ciro Limiti': f"₺{min_ciro:,.0f}",
            'Rapor Tarihi': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M'),
            'Kayıt Sayısı': len(df_analiz)
        }])
        ozet.to_excel(writer, sheet_name='Bilgi', index=False)
        
        # Sayfa 2: En Kötü Performans (Adet düşüşüne göre)
        en_kotu = df_analiz.nsmallest(20, 'Adet_Deg')[
            ['Mal_Grubu', 'Ust_Mal', 'Adet_2024', 'Adet_2025', 'Adet_Deg', 
             'Ciro_Deg', 'Marj_Deg', 'Fire_Deg', 'Neden', 'Aksiyon']
        ]
        en_kotu.to_excel(writer, sheet_name='En Kötü 20', index=False)
        
        # Sayfa 3: En İyi Performans
        en_iyi = df_analiz.nlargest(20, 'Adet_Deg')[
            ['Mal_Grubu', 'Ust_Mal', 'Adet_2024', 'Adet_2025', 'Adet_Deg', 
             'Ciro_Deg', 'Marj_Deg', 'Fire_Deg', 'Neden', 'Aksiyon']
        ]
        en_iyi.to_excel(writer, sheet_name='En İyi 20', index=False)
        
        # Sayfa 4: Tüm Veriler
        df_analiz.to_excel(writer, sheet_name='Tüm Mal Grupları', index=False)
    
    output.seek(0)
    return output


# ============================================================================
# 5. UI BİLEŞENLERİ
# ============================================================================

def sidebar_filtreler(filtre_options: dict) -> dict:
    """Sol panel filtreleri"""
    
    st.sidebar.markdown("## 🎛️ FİLTRELER")
    
    # === Organizasyon ===
    st.sidebar.markdown("### 📍 Organizasyon")
    
    sm_list = ['Tümü'] + filtre_options.get('sm', [])
    secili_sm = st.sidebar.selectbox('SM', sm_list)
    
    # BS (SM'ye bağlı)
    if secili_sm != 'Tümü':
        bs_opts = filtre_options.get('bs_map', {}).get(secili_sm, [])
    else:
        bs_opts = []
        for v in filtre_options.get('bs_map', {}).values():
            bs_opts.extend(v)
        bs_opts = sorted(set(bs_opts))
    
    bs_list = ['Tümü'] + bs_opts
    secili_bs = st.sidebar.selectbox('BS', bs_list)
    
    # Mağaza (BS'ye bağlı)
    if secili_bs != 'Tümü':
        mag_opts = filtre_options.get('magaza_map', {}).get(secili_bs, [])
        mag_list = ['Tümü'] + [f"{k} - {a}" for k, a in mag_opts]
    else:
        mag_list = ['Tümü']
    
    secili_mag = st.sidebar.selectbox('Mağaza', mag_list)
    secili_mag_kod = secili_mag.split(' - ')[0] if secili_mag != 'Tümü' else 'Tümü'
    
    st.sidebar.markdown("---")
    
    # === Ürün Hiyerarşisi ===
    st.sidebar.markdown("### 📦 Ürün")
    
    nitelik_list = ['Tümü'] + filtre_options.get('nitelik', [])
    secili_nitelik = st.sidebar.selectbox('Nitelik', nitelik_list)
    
    urun_grubu_list = ['Tümü'] + filtre_options.get('urun_grubu', [])
    secili_urun_grubu = st.sidebar.selectbox('Ürün Grubu', urun_grubu_list)
    
    # Üst Mal (Ürün Grubuna bağlı)
    if secili_urun_grubu != 'Tümü':
        ust_opts = filtre_options.get('ust_mal_map', {}).get(secili_urun_grubu, [])
    else:
        ust_opts = []
        for v in filtre_options.get('ust_mal_map', {}).values():
            ust_opts.extend(v)
        ust_opts = sorted(set(ust_opts))
    
    ust_list = ['Tümü'] + ust_opts
    secili_ust = st.sidebar.selectbox('Üst Mal Grubu', ust_list)
    
    # Mal Grubu (Üst Mal'a bağlı)
    if secili_ust != 'Tümü':
        mal_opts = filtre_options.get('mal_grubu_map', {}).get(secili_ust, [])
    else:
        mal_opts = []
        for v in filtre_options.get('mal_grubu_map', {}).values():
            mal_opts.extend(v)
        mal_opts = sorted(set(mal_opts))
    
    mal_list = ['Tümü'] + mal_opts
    secili_mal = st.sidebar.selectbox('Mal Grubu', mal_list)
    
    st.sidebar.markdown("---")
    
    # === Alt Limit ===
    st.sidebar.markdown("### 📊 Alt Limit")
    min_ciro = st.sidebar.number_input(
        '2025 Min. Ciro (₺)', 
        min_value=0, 
        max_value=1000000, 
        value=10000, 
        step=5000,
        help="Bu tutarın altındaki mal grupları analize dahil edilmez"
    )
    
    return {
        'sm': secili_sm,
        'bs': secili_bs,
        'magaza': secili_mag_kod,
        'nitelik': secili_nitelik,
        'urun_grubu': secili_urun_grubu,
        'ust_mal': secili_ust,
        'mal_grubu': secili_mal,
        'min_ciro': min_ciro
    }


def filtre_aciklamasi(f: dict) -> str:
    """Filtre açıklaması oluştur"""
    
    parcalar = []
    if f['sm'] != 'Tümü': parcalar.append(f"SM: {f['sm']}")
    if f['bs'] != 'Tümü': parcalar.append(f"BS: {f['bs']}")
    if f['magaza'] != 'Tümü': parcalar.append(f"Mağaza: {f['magaza']}")
    if f['nitelik'] != 'Tümü': parcalar.append(f"Nitelik: {f['nitelik']}")
    if f['urun_grubu'] != 'Tümü': parcalar.append(f"Ürün Grubu: {f['urun_grubu']}")
    if f['ust_mal'] != 'Tümü': parcalar.append(f"Üst Mal: {f['ust_mal']}")
    if f['mal_grubu'] != 'Tümü': parcalar.append(f"Mal Grubu: {f['mal_grubu']}")
    
    return " | ".join(parcalar) if parcalar else "Tüm Veriler"


def kpi_goster(ozet: dict):
    """KPI kartları"""
    
    metrikler = [
        ('📦 Satış Adedi', 'adet', '{:,.0f}', False),
        ('💰 Ciro', 'ciro', '₺{:,.0f}', False),
        ('📈 Marj', 'marj', '₺{:,.0f}', False),
        ('🔥 Fire', 'fire', '₺{:,.0f}', True),  # Fire için ters mantık
    ]
    
    cols = st.columns(4)
    
    for col, (label, key, fmt, ters) in zip(cols, metrikler):
        with col:
            deger = ozet.get(f'{key}_2025', 0) or 0
            degisim = ozet.get(f'{key}_degisim', 0) or 0
            
            if ters:
                delta_class = 'delta-down' if degisim > 0 else 'delta-up'
            else:
                delta_class = 'delta-up' if degisim > 0 else 'delta-down'
            
            isaret = '+' if degisim > 0 else ''
            
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{fmt.format(deger)}</div>
                <div class="kpi-delta {delta_class}">{isaret}{degisim:.1f}%</div>
            </div>
            """, unsafe_allow_html=True)


def karar_kartlari_goster(df: pd.DataFrame, baslik: str, limit: int = 10, ters: bool = False):
    """Karar kartlarını göster"""
    
    st.markdown(f'<div class="section-title">{baslik}</div>', unsafe_allow_html=True)
    
    if df.empty:
        st.info("Gösterilecek veri yok")
        return
    
    # Sırala
    if ters:
        df_sorted = df.nlargest(limit, 'Adet_Deg')
    else:
        df_sorted = df.nsmallest(limit, 'Adet_Deg')
    
    for idx, row in df_sorted.iterrows():
        mal_grubu = row['Mal_Grubu']
        adet_deg = row.get('Adet_Deg', 0) or 0
        ciro_deg = row.get('Ciro_Deg', 0) or 0
        marj_deg = row.get('Marj_Deg', 0) or 0
        fire_deg = row.get('Fire_Deg', 0) or 0
        
        neden, aksiyon, renk = neden_tespit(row)
        
        card_class = f"karar-card karar-card-{renk}"
        badge_class = f"karar-badge badge-{renk}"
        
        with st.expander(f"**{mal_grubu}** → Adet: {adet_deg:+.1f}%", expanded=False):
            st.markdown(f"""
            <div class="{card_class}">
                <div class="karar-header">
                    <span class="karar-title">{row.get('Ust_Mal', '-')}</span>
                    <span class="{badge_class}">{neden.split(' ')[0]}</span>
                </div>
                <div class="karar-metrics">
                    📦 Adet: {row.get('Adet_2024', 0):,.0f} → {row.get('Adet_2025', 0):,.0f} ({adet_deg:+.1f}%)<br>
                    💰 Ciro: ₺{row.get('Ciro_2024', 0):,.0f} → ₺{row.get('Ciro_2025', 0):,.0f} ({ciro_deg:+.1f}%)<br>
                    📈 Marj: {marj_deg:+.1f}% | 🔥 Fire: {fire_deg:+.1f}%
                </div>
                <div class="karar-neden">
                    <strong>Neden:</strong> {neden}
                </div>
                <div class="karar-aksiyon">
                    💡 <strong>Aksiyon:</strong> {aksiyon}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Ürün detay butonu
            if st.button(f"📋 Ürünleri Göster", key=f"urun_{idx}_{mal_grubu}"):
                st.session_state[f'show_urun_{mal_grubu}'] = True
            
            if st.session_state.get(f'show_urun_{mal_grubu}'):
                # Bu fonksiyon main'de çağrılacak
                st.session_state['selected_mal_grubu'] = mal_grubu


# ============================================================================
# 6. MAIN
# ============================================================================

def main():
    """Ana uygulama"""
    
    st.markdown('<h1 class="main-title">🎯 Satış Karar Sistemi</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-title">Kasım 2024 → Kasım 2025 | 3 dakikada teşhis, neden, aksiyon</p>', unsafe_allow_html=True)
    
    # Dosya yükleme
    col1, col2 = st.columns(2)
    with col1:
        file_2024 = st.file_uploader("📁 2024 Kasım Verisi", type=['xlsx'], key='file_2024')
    with col2:
        file_2025 = st.file_uploader("📁 2025 Kasım Verisi", type=['xlsx'], key='file_2025')
    
    if not file_2024 or not file_2025:
        st.info("👆 Her iki Excel dosyasını da yükleyin")
        
        st.markdown("""
        ### 🎯 Bu Sistem Ne Yapar?
        
        **Veri göstermez, KARAR üretir.**
        
        **5 Soruya Cevap:**
        1. Çok satan & pay kazanan kim?
        2. Az satan ama pay kazanan kim?
        3. Çok satan ama pay kaybeden kim?
        4. Marjı bozanlar kim?
        5. Düşüşün muhtemel NEDENİ ne?
        
        **Otomatik Teşhis:**
        - 🏷️ Kampanya kaynaklı mı?
        - 🔥 Fire problemi mi?
        - 📦 Bulunurluk/yerleşim mi?
        - 💰 Fiyatlama mı?
        
        **Analiz Edilecek Nitelikler:**
        Spot, Grup Spot, Regule, Kasa Aktivitesi, Bölgesel
        """)
        return
    
    # Veri yükle
    cache_key = f"{file_2024.name}_{file_2025.name}_{file_2024.size}_{file_2025.size}"
    
    try:
        veri = veri_yukle(file_2024.getvalue(), file_2025.getvalue(), cache_key)
    except Exception as e:
        st.error(f"Veri yüklenirken hata: {str(e)}")
        return
    
    # Sidebar filtreleri
    secili = sidebar_filtreler(veri['filtreler'])
    filtre_text = filtre_aciklamasi(secili)
    where_clause = build_where(secili)
    
    # DuckDB bağlantısı
    con = get_duckdb_connection(veri['path_2024'], veri['path_2025'])
    
    # Filtre bilgisi
    st.markdown(f'<div class="filter-badge">📍 <strong>Filtre:</strong> {filtre_text} | <strong>Min Ciro:</strong> ₺{secili["min_ciro"]:,}</div>', unsafe_allow_html=True)
    
    # Excel rapor butonu
    excel = excel_rapor_olustur(con, where_clause, secili['min_ciro'], filtre_text)
    st.download_button(
        "📥 EXCEL RAPORU İNDİR",
        excel,
        f"karar_raporu_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    
    st.markdown("---")
    
    # KPI'lar
    ozet = get_ozet_kpiler(con, where_clause)
    kpi_goster(ozet)
    
    st.markdown("---")
    
    # Mal Grubu Analizi
    df_analiz = get_mal_grubu_analiz(con, where_clause, secili['min_ciro'])
    
    # İki sütun: En Kötü / En İyi
    col1, col2 = st.columns(2)
    
    with col1:
        karar_kartlari_goster(df_analiz, "🔴 EN KÖTÜ 10 (Adet Düşüşü)", limit=10, ters=False)
    
    with col2:
        karar_kartlari_goster(df_analiz, "🟢 EN İYİ 10 (Adet Artışı)", limit=10, ters=True)
    
    # Ürün detay gösterimi
    selected_mal = st.session_state.get('selected_mal_grubu')
    if selected_mal:
        st.markdown("---")
        st.markdown(f'<div class="section-title">📋 {selected_mal} - Ürün Detayları</div>', unsafe_allow_html=True)
        
        df_urunler = get_urun_detay(con, selected_mal, where_clause)
        
        if not df_urunler.empty:
            st.dataframe(
                df_urunler,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'Adet_Deg': st.column_config.NumberColumn('Adet %', format='%.1f%%'),
                    'Ciro_2024': st.column_config.NumberColumn('Ciro 2024', format='₺%.0f'),
                    'Ciro_2025': st.column_config.NumberColumn('Ciro 2025', format='₺%.0f'),
                }
            )
        
        if st.button("❌ Kapat"):
            st.session_state['selected_mal_grubu'] = None
            st.rerun()
    
    # Footer
    st.markdown("---")
    st.caption(f"📊 Kayıt: 2024={veri['sayilar']['2024']:,} | 2025={veri['sayilar']['2025']:,}")
    
    con.close()


if __name__ == "__main__":
    main()
