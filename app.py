"""
🎯 BÖLGESEL PERFORMANS ANALİZİ v4
Basit, Net, Kullanışlı

Ana Metrik: SATIŞ MİKTARI
Odak: Mal Grubu bazlı En İyi/Kötü 10
Nitelik Filtresi: Spot, Grup Spot, Regule, Kasa Aktivitesi, Bölgesel
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
from io import BytesIO
import warnings
import gc
warnings.filterwarnings('ignore')

# ============================================================================
# SAYFA AYARLARI
# ============================================================================
st.set_page_config(
    page_title="Performans Analizi v4",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# SABİTLER
# ============================================================================

# Sadece bu niteliklere bak
VALID_NITELIKLER = ['Spot', 'Grup Spot', 'Regule', 'Kasa Aktivitesi', 'Bölgesel']

# Minimum baz (2024'te bu adetten az satanları gösterme)
MIN_BASE_ADET = 100

# Gerekli kolonlar
REQUIRED_COLS = [
    'SM', 'BS', 'Mağaza - Anahtar', 'Mağaza - Orta uzunl.metin',
    'Malzeme Nitelik - Metin', 'Ürün Grubu - Orta uzunl.metin',
    'Üst Mal Grubu - Orta uzunl.metin', 'Mal Grubu - Orta uzunl.metin',
    'Malzeme Kodu', 'Malzeme Tanımı',
    'Satış Miktarı', 'Satış Hasılatı (VD)', 'Net Marj',
    'Fire Tutarı', 'Envanter Tutarı'
]

# ============================================================================
# CSS
# ============================================================================
st.markdown("""
<style>
    .main-header {font-size: 2rem; font-weight: 700; color: #1f2937; margin-bottom: 0.5rem;}
    .sub-header {font-size: 1rem; color: #6b7280; margin-bottom: 1rem;}
    
    .kpi-container {display: flex; gap: 1rem; margin: 1rem 0;}
    .kpi-box {
        flex: 1; background: white; border: 1px solid #e5e7eb; border-radius: 12px;
        padding: 1rem; text-align: center;
    }
    .kpi-label {font-size: 0.85rem; color: #6b7280; margin-bottom: 0.25rem;}
    .kpi-value {font-size: 1.8rem; font-weight: 700; color: #1f2937;}
    .kpi-delta {font-size: 1rem; font-weight: 600;}
    .kpi-delta-pos {color: #10b981;}
    .kpi-delta-neg {color: #ef4444;}
    
    .section-title {
        font-size: 1.1rem; font-weight: 600; color: #374151;
        border-bottom: 2px solid #e5e7eb; padding-bottom: 0.5rem;
        margin: 1.5rem 0 1rem 0;
    }
    .section-title-red {border-bottom-color: #ef4444; color: #dc2626;}
    .section-title-green {border-bottom-color: #10b981; color: #059669;}
    
    .filter-info {
        background: #f3f4f6; padding: 0.75rem; border-radius: 8px;
        font-size: 0.85rem; color: #4b5563; margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# VERİ YÜKLEME
# ============================================================================

def load_excel_to_df(file_bytes, year):
    """Excel'i DataFrame'e yükle ve temizle"""
    
    df = pd.read_excel(
        BytesIO(file_bytes),
        engine='openpyxl'
    )
    df.columns = df.columns.str.strip()
    df['YIL'] = year
    
    # Sadece geçerli nitelikleri filtrele
    if 'Malzeme Nitelik - Metin' in df.columns:
        df = df[df['Malzeme Nitelik - Metin'].isin(VALID_NITELIKLER)]
    
    # Numerik kolonları düzelt
    for col in ['Satış Miktarı', 'Satış Hasılatı (VD)', 'Net Marj', 'Fire Tutarı', 'Envanter Tutarı']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # String kolonları düzelt
    for col in ['SM', 'BS', 'Mağaza - Anahtar', 'Mağaza - Orta uzunl.metin',
                'Malzeme Nitelik - Metin', 'Ürün Grubu - Orta uzunl.metin',
                'Üst Mal Grubu - Orta uzunl.metin', 'Mal Grubu - Orta uzunl.metin',
                'Malzeme Kodu', 'Malzeme Tanımı']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['nan', 'None', 'NaN', ''], '')
    
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_data(_bytes_2024, _bytes_2025, cache_key):
    """Veriyi yükle ve DuckDB'ye hazırla"""
    
    progress = st.progress(0, text="2024 verisi yükleniyor...")
    df_2024 = load_excel_to_df(_bytes_2024, 2024)
    count_2024 = len(df_2024)
    
    progress.progress(40, text="2025 verisi yükleniyor...")
    df_2025 = load_excel_to_df(_bytes_2025, 2025)
    count_2025 = len(df_2025)
    
    progress.progress(60, text="Veriler birleştiriliyor...")
    df_all = pd.concat([df_2024, df_2025], ignore_index=True)
    
    del df_2024, df_2025
    gc.collect()
    
    progress.progress(70, text="Filtre seçenekleri hazırlanıyor...")
    
    # DuckDB bağlantısı - DataFrame'den direkt oku
    con = duckdb.connect()
    con.register('veri', df_all)
    
    # Filtre seçenekleri
    filters = {
        'sm': con.execute('SELECT DISTINCT SM FROM veri WHERE SM != "" ORDER BY SM').df()['SM'].tolist(),
        'nitelik': con.execute('SELECT DISTINCT "Malzeme Nitelik - Metin" as n FROM veri ORDER BY n').df()['n'].tolist(),
        'urun_grubu': con.execute('SELECT DISTINCT "Ürün Grubu - Orta uzunl.metin" as n FROM veri ORDER BY n').df()['n'].tolist(),
    }
    
    # BS by SM
    bs_df = con.execute('SELECT DISTINCT SM, BS FROM veri WHERE BS != "" ORDER BY SM, BS').df()
    filters['bs_by_sm'] = bs_df.groupby('SM')['BS'].apply(list).to_dict()
    
    # Mağaza by BS
    mag_df = con.execute('''
        SELECT DISTINCT BS, "Mağaza - Anahtar" as kod, "Mağaza - Orta uzunl.metin" as ad 
        FROM veri WHERE kod != "" ORDER BY BS, kod
    ''').df()
    filters['magaza_by_bs'] = mag_df.groupby('BS').apply(
        lambda x: list(zip(x['kod'], x['ad']))
    ).to_dict()
    
    # Üst Mal by Ürün Grubu
    ust_df = con.execute('''
        SELECT DISTINCT "Ürün Grubu - Orta uzunl.metin" as ug, "Üst Mal Grubu - Orta uzunl.metin" as umg
        FROM veri ORDER BY ug, umg
    ''').df()
    filters['ust_mal_by_urun'] = ust_df.groupby('ug')['umg'].apply(list).to_dict()
    
    # Mal by Üst Mal
    mal_df = con.execute('''
        SELECT DISTINCT "Üst Mal Grubu - Orta uzunl.metin" as umg, "Mal Grubu - Orta uzunl.metin" as mg
        FROM veri ORDER BY umg, mg
    ''').df()
    filters['mal_by_ust'] = mal_df.groupby('umg')['mg'].apply(list).to_dict()
    
    progress.progress(100, text="Hazır!")
    progress.empty()
    
    return {
        'df': df_all,
        'filters': filters,
        'counts': {'2024': count_2024, '2025': count_2025}
    }


# ============================================================================
# SORGULAR
# ============================================================================

def build_where_clause(filters):
    """Filtre koşullarını SQL WHERE clause'a çevir"""
    
    conditions = []
    
    if filters.get('sm') and filters['sm'] != 'Tümü':
        conditions.append(f"SM = '{filters['sm']}'")
    
    if filters.get('bs') and filters['bs'] != 'Tümü':
        conditions.append(f"BS = '{filters['bs']}'")
    
    if filters.get('magaza') and filters['magaza'] != 'Tümü':
        conditions.append(f"\"Mağaza - Anahtar\" = '{filters['magaza']}'")
    
    if filters.get('nitelik') and filters['nitelik'] != 'Tümü':
        conditions.append(f"\"Malzeme Nitelik - Metin\" = '{filters['nitelik']}'")
    
    if filters.get('urun_grubu') and filters['urun_grubu'] != 'Tümü':
        conditions.append(f"\"Ürün Grubu - Orta uzunl.metin\" = '{filters['urun_grubu']}'")
    
    if filters.get('ust_mal') and filters['ust_mal'] != 'Tümü':
        conditions.append(f"\"Üst Mal Grubu - Orta uzunl.metin\" = '{filters['ust_mal']}'")
    
    if filters.get('mal_grubu') and filters['mal_grubu'] != 'Tümü':
        conditions.append(f"\"Mal Grubu - Orta uzunl.metin\" = '{filters['mal_grubu']}'")
    
    if conditions:
        return "WHERE " + " AND ".join(conditions)
    return ""


def get_summary(con, where_clause):
    """Özet KPI'ları getir"""
    
    query = f"""
        SELECT 
            YIL,
            SUM("Satış Miktarı") as adet,
            SUM("Satış Hasılatı (VD)") as ciro,
            SUM("Net Marj") as marj,
            SUM(ABS("Fire Tutarı")) as fire
        FROM veri
        {where_clause}
        GROUP BY YIL
    """
    
    df = con.execute(query).df()
    
    result = {}
    for _, row in df.iterrows():
        year = int(row['YIL'])
        result[f'adet_{year}'] = row['adet']
        result[f'ciro_{year}'] = row['ciro']
        result[f'marj_{year}'] = row['marj']
        result[f'fire_{year}'] = row['fire']
    
    # Değişimler
    for metric in ['adet', 'ciro', 'marj', 'fire']:
        v2024 = result.get(f'{metric}_2024', 0)
        v2025 = result.get(f'{metric}_2025', 0)
        if v2024 and v2024 != 0:
            result[f'{metric}_change'] = ((v2025 / v2024) - 1) * 100
        else:
            result[f'{metric}_change'] = 0
    
    return result


def get_mal_grubu_performance(con, where_clause, order='ASC', limit=10):
    """Mal Grubu bazlı performans (en iyi veya en kötü)"""
    
    # WHERE clause'a yıl koşulu ekle
    base_where = where_clause if where_clause else "WHERE 1=1"
    
    query = f"""
        WITH yearly AS (
            SELECT 
                "Mal Grubu - Orta uzunl.metin" as mal_grubu,
                "Üst Mal Grubu - Orta uzunl.metin" as ust_mal,
                YIL,
                SUM("Satış Miktarı") as adet,
                SUM("Satış Hasılatı (VD)") as ciro,
                SUM("Net Marj") as marj,
                SUM(ABS("Fire Tutarı")) as fire
            FROM veri
            {where_clause}
            GROUP BY "Mal Grubu - Orta uzunl.metin", "Üst Mal Grubu - Orta uzunl.metin", YIL
        ),
        pivoted AS (
            SELECT 
                mal_grubu,
                MAX(ust_mal) as ust_mal,
                SUM(CASE WHEN YIL = 2024 THEN adet ELSE 0 END) as adet_2024,
                SUM(CASE WHEN YIL = 2025 THEN adet ELSE 0 END) as adet_2025,
                SUM(CASE WHEN YIL = 2024 THEN ciro ELSE 0 END) as ciro_2024,
                SUM(CASE WHEN YIL = 2025 THEN ciro ELSE 0 END) as ciro_2025,
                SUM(CASE WHEN YIL = 2024 THEN marj ELSE 0 END) as marj_2024,
                SUM(CASE WHEN YIL = 2025 THEN marj ELSE 0 END) as marj_2025,
                SUM(CASE WHEN YIL = 2024 THEN fire ELSE 0 END) as fire_2024,
                SUM(CASE WHEN YIL = 2025 THEN fire ELSE 0 END) as fire_2025
            FROM yearly
            GROUP BY mal_grubu
        )
        SELECT 
            *,
            CASE WHEN adet_2024 > 0 THEN ((adet_2025 / adet_2024) - 1) * 100 ELSE 0 END as adet_change,
            CASE WHEN ciro_2024 > 0 THEN ((ciro_2025 / ciro_2024) - 1) * 100 ELSE 0 END as ciro_change,
            CASE WHEN marj_2024 > 0 THEN ((marj_2025 / marj_2024) - 1) * 100 ELSE 0 END as marj_change,
            CASE WHEN fire_2024 > 0 THEN ((fire_2025 / fire_2024) - 1) * 100 ELSE 0 END as fire_change
        FROM pivoted
        WHERE adet_2024 >= {MIN_BASE_ADET}
        ORDER BY adet_change {order}
        LIMIT {limit}
    """
    
    return con.execute(query).df()


def get_product_details(con, mal_grubu, where_clause):
    """Mal grubu içindeki ürün detayları"""
    
    base_where = where_clause if where_clause else "WHERE 1=1"
    mal_condition = f"\"Mal Grubu - Orta uzunl.metin\" = '{mal_grubu}'"
    
    if where_clause:
        full_where = f"{where_clause} AND {mal_condition}"
    else:
        full_where = f"WHERE {mal_condition}"
    
    query = f"""
        WITH yearly AS (
            SELECT 
                "Malzeme Kodu" as kod,
                "Malzeme Tanımı" as urun,
                YIL,
                SUM("Satış Miktarı") as adet,
                SUM("Satış Hasılatı (VD)") as ciro,
                SUM("Net Marj") as marj,
                SUM(ABS("Fire Tutarı")) as fire
            FROM veri
            {full_where}
            GROUP BY "Malzeme Kodu", "Malzeme Tanımı", YIL
        ),
        pivoted AS (
            SELECT 
                kod,
                MAX(urun) as urun,
                SUM(CASE WHEN YIL = 2024 THEN adet ELSE 0 END) as adet_2024,
                SUM(CASE WHEN YIL = 2025 THEN adet ELSE 0 END) as adet_2025,
                SUM(CASE WHEN YIL = 2024 THEN ciro ELSE 0 END) as ciro_2024,
                SUM(CASE WHEN YIL = 2025 THEN ciro ELSE 0 END) as ciro_2025,
                SUM(CASE WHEN YIL = 2024 THEN marj ELSE 0 END) as marj_2024,
                SUM(CASE WHEN YIL = 2025 THEN marj ELSE 0 END) as marj_2025,
                SUM(CASE WHEN YIL = 2024 THEN fire ELSE 0 END) as fire_2024,
                SUM(CASE WHEN YIL = 2025 THEN fire ELSE 0 END) as fire_2025
            FROM yearly
            GROUP BY kod
        )
        SELECT 
            *,
            CASE WHEN adet_2024 > 0 THEN ((adet_2025 / adet_2024) - 1) * 100 ELSE 0 END as adet_change,
            CASE WHEN ciro_2024 > 0 THEN ((ciro_2025 / ciro_2024) - 1) * 100 ELSE 0 END as ciro_change,
            CASE WHEN marj_2024 > 0 THEN ((marj_2025 / marj_2024) - 1) * 100 ELSE 0 END as marj_change,
            CASE WHEN fire_2024 > 0 THEN ((fire_2025 / fire_2024) - 1) * 100 ELSE 0 END as fire_change
        FROM pivoted
        ORDER BY adet_2025 DESC
    """
    
    return con.execute(query).df()


def get_filtered_data_for_excel(con, where_clause):
    """Excel için filtrelenmiş veri"""
    
    # Mal Grubu özet
    mal_grubu = con.execute(f"""
        WITH yearly AS (
            SELECT 
                "Mal Grubu - Orta uzunl.metin" as mal_grubu,
                "Üst Mal Grubu - Orta uzunl.metin" as ust_mal,
                YIL,
                SUM("Satış Miktarı") as adet,
                SUM("Satış Hasılatı (VD)") as ciro,
                SUM("Net Marj") as marj,
                SUM(ABS("Fire Tutarı")) as fire
            FROM veri
            {where_clause}
            GROUP BY "Mal Grubu - Orta uzunl.metin", "Üst Mal Grubu - Orta uzunl.metin", YIL
        )
        SELECT 
            mal_grubu as "Mal Grubu",
            MAX(ust_mal) as "Üst Mal Grubu",
            SUM(CASE WHEN YIL = 2024 THEN adet ELSE 0 END) as "Adet 2024",
            SUM(CASE WHEN YIL = 2025 THEN adet ELSE 0 END) as "Adet 2025",
            ROUND(CASE WHEN SUM(CASE WHEN YIL = 2024 THEN adet ELSE 0 END) > 0 
                  THEN ((SUM(CASE WHEN YIL = 2025 THEN adet ELSE 0 END) / SUM(CASE WHEN YIL = 2024 THEN adet ELSE 0 END)) - 1) * 100 
                  ELSE 0 END, 1) as "Adet Değişim %",
            SUM(CASE WHEN YIL = 2024 THEN ciro ELSE 0 END) as "Ciro 2024",
            SUM(CASE WHEN YIL = 2025 THEN ciro ELSE 0 END) as "Ciro 2025",
            ROUND(CASE WHEN SUM(CASE WHEN YIL = 2024 THEN ciro ELSE 0 END) > 0 
                  THEN ((SUM(CASE WHEN YIL = 2025 THEN ciro ELSE 0 END) / SUM(CASE WHEN YIL = 2024 THEN ciro ELSE 0 END)) - 1) * 100 
                  ELSE 0 END, 1) as "Ciro Değişim %",
            SUM(CASE WHEN YIL = 2024 THEN marj ELSE 0 END) as "Marj 2024",
            SUM(CASE WHEN YIL = 2025 THEN marj ELSE 0 END) as "Marj 2025",
            SUM(CASE WHEN YIL = 2025 THEN fire ELSE 0 END) as "Fire 2025"
        FROM yearly
        GROUP BY mal_grubu
        ORDER BY "Adet Değişim %" ASC
    """).df()
    
    # Ürün detay
    urun_detay = con.execute(f"""
        WITH yearly AS (
            SELECT 
                "Malzeme Kodu" as kod,
                "Malzeme Tanımı" as urun,
                "Mal Grubu - Orta uzunl.metin" as mal_grubu,
                YIL,
                SUM("Satış Miktarı") as adet,
                SUM("Satış Hasılatı (VD)") as ciro,
                SUM("Net Marj") as marj,
                SUM(ABS("Fire Tutarı")) as fire
            FROM veri
            {where_clause}
            GROUP BY "Malzeme Kodu", "Malzeme Tanımı", "Mal Grubu - Orta uzunl.metin", YIL
        )
        SELECT 
            kod as "Malzeme Kodu",
            MAX(urun) as "Ürün Adı",
            MAX(mal_grubu) as "Mal Grubu",
            SUM(CASE WHEN YIL = 2024 THEN adet ELSE 0 END) as "Adet 2024",
            SUM(CASE WHEN YIL = 2025 THEN adet ELSE 0 END) as "Adet 2025",
            ROUND(CASE WHEN SUM(CASE WHEN YIL = 2024 THEN adet ELSE 0 END) > 0 
                  THEN ((SUM(CASE WHEN YIL = 2025 THEN adet ELSE 0 END) / SUM(CASE WHEN YIL = 2024 THEN adet ELSE 0 END)) - 1) * 100 
                  ELSE 0 END, 1) as "Adet Değişim %",
            SUM(CASE WHEN YIL = 2024 THEN ciro ELSE 0 END) as "Ciro 2024",
            SUM(CASE WHEN YIL = 2025 THEN ciro ELSE 0 END) as "Ciro 2025",
            SUM(CASE WHEN YIL = 2025 THEN fire ELSE 0 END) as "Fire 2025"
        FROM yearly
        GROUP BY kod
        ORDER BY "Adet 2025" DESC
    """).df()
    
    return {'mal_grubu': mal_grubu, 'urun_detay': urun_detay}


# ============================================================================
# EXCEL RAPOR
# ============================================================================

def create_excel_report(con, where_clause, filter_desc):
    """Filtreye göre Excel raporu"""
    
    output = BytesIO()
    
    data = get_filtered_data_for_excel(con, where_clause)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Filtre bilgisi
        info_df = pd.DataFrame([{'Filtre': filter_desc, 'Tarih': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}])
        info_df.to_excel(writer, sheet_name='Bilgi', index=False)
        
        # Mal Grubu özet
        data['mal_grubu'].to_excel(writer, sheet_name='Mal Grubu Özet', index=False)
        
        # Ürün detay
        data['urun_detay'].to_excel(writer, sheet_name='Ürün Detay', index=False)
    
    output.seek(0)
    return output


# ============================================================================
# UI
# ============================================================================

def render_sidebar(filter_options):
    """Sol panel filtreleri"""
    
    st.sidebar.markdown("## 🎛️ FİLTRELER")
    
    # Organizasyon filtreleri
    st.sidebar.markdown("### 📍 Organizasyon")
    
    sm_list = ['Tümü'] + filter_options.get('sm', [])
    selected_sm = st.sidebar.selectbox('SM', sm_list, key='sm')
    
    # BS (SM'ye bağlı)
    if selected_sm != 'Tümü':
        bs_opts = filter_options.get('bs_by_sm', {}).get(selected_sm, [])
    else:
        bs_opts = []
        for bs_list in filter_options.get('bs_by_sm', {}).values():
            bs_opts.extend(bs_list)
        bs_opts = sorted(set(bs_opts))
    
    bs_list = ['Tümü'] + bs_opts
    selected_bs = st.sidebar.selectbox('BS', bs_list, key='bs')
    
    # Mağaza (BS'ye bağlı)
    if selected_bs != 'Tümü':
        mag_opts = filter_options.get('magaza_by_bs', {}).get(selected_bs, [])
        mag_list = ['Tümü'] + [f"{kod} - {ad}" for kod, ad in mag_opts]
    else:
        mag_list = ['Tümü']
    
    selected_mag = st.sidebar.selectbox('Mağaza', mag_list, key='mag')
    selected_mag_kod = selected_mag.split(' - ')[0] if selected_mag != 'Tümü' else 'Tümü'
    
    st.sidebar.markdown("---")
    
    # Ürün filtreleri
    st.sidebar.markdown("### 📦 Ürün")
    
    nitelik_list = ['Tümü'] + filter_options.get('nitelik', [])
    selected_nitelik = st.sidebar.selectbox('Nitelik', nitelik_list, key='nitelik')
    
    urun_list = ['Tümü'] + filter_options.get('urun_grubu', [])
    selected_urun = st.sidebar.selectbox('Ürün Grubu', urun_list, key='urun')
    
    # Üst Mal (Ürün Grubuna bağlı)
    if selected_urun != 'Tümü':
        ust_opts = filter_options.get('ust_mal_by_urun', {}).get(selected_urun, [])
    else:
        ust_opts = []
        for ust_list in filter_options.get('ust_mal_by_urun', {}).values():
            ust_opts.extend(ust_list)
        ust_opts = sorted(set(ust_opts))
    
    ust_list = ['Tümü'] + ust_opts
    selected_ust = st.sidebar.selectbox('Üst Mal Grubu', ust_list, key='ust')
    
    # Mal Grubu (Üst Mal'a bağlı)
    if selected_ust != 'Tümü':
        mal_opts = filter_options.get('mal_by_ust', {}).get(selected_ust, [])
    else:
        mal_opts = []
        for mal_list in filter_options.get('mal_by_ust', {}).values():
            mal_opts.extend(mal_list)
        mal_opts = sorted(set(mal_opts))
    
    mal_list = ['Tümü'] + mal_opts
    selected_mal = st.sidebar.selectbox('Mal Grubu', mal_list, key='mal')
    
    return {
        'sm': selected_sm,
        'bs': selected_bs,
        'magaza': selected_mag_kod,
        'nitelik': selected_nitelik,
        'urun_grubu': selected_urun,
        'ust_mal': selected_ust,
        'mal_grubu': selected_mal
    }


def get_filter_description(filters):
    """Filtre açıklaması"""
    
    parts = []
    if filters['sm'] != 'Tümü':
        parts.append(f"SM: {filters['sm']}")
    if filters['bs'] != 'Tümü':
        parts.append(f"BS: {filters['bs']}")
    if filters['magaza'] != 'Tümü':
        parts.append(f"Mağaza: {filters['magaza']}")
    if filters['nitelik'] != 'Tümü':
        parts.append(f"Nitelik: {filters['nitelik']}")
    if filters['urun_grubu'] != 'Tümü':
        parts.append(f"Ürün Grubu: {filters['urun_grubu']}")
    if filters['ust_mal'] != 'Tümü':
        parts.append(f"Üst Mal: {filters['ust_mal']}")
    if filters['mal_grubu'] != 'Tümü':
        parts.append(f"Mal Grubu: {filters['mal_grubu']}")
    
    return " | ".join(parts) if parts else "Tüm Veriler"


def render_kpis(summary):
    """KPI kartları"""
    
    col1, col2, col3, col4 = st.columns(4)
    
    metrics = [
        ('📦 Satış Adedi', 'adet', '{:,.0f}'),
        ('💰 Ciro', 'ciro', '₺{:,.0f}'),
        ('📈 Marj', 'marj', '₺{:,.0f}'),
        ('🔥 Fire', 'fire', '₺{:,.0f}')
    ]
    
    for col, (label, key, fmt) in zip([col1, col2, col3, col4], metrics):
        with col:
            val_2025 = summary.get(f'{key}_2025', 0)
            change = summary.get(f'{key}_change', 0)
            
            delta_class = 'kpi-delta-pos' if change > 0 else 'kpi-delta-neg'
            if key == 'fire':  # Fire için ters mantık
                delta_class = 'kpi-delta-neg' if change > 0 else 'kpi-delta-pos'
            
            st.markdown(f"""
            <div class="kpi-box">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{fmt.format(val_2025)}</div>
                <div class="kpi-delta {delta_class}">{'+' if change > 0 else ''}{change:.1f}%</div>
            </div>
            """, unsafe_allow_html=True)


def render_worst_best(con, where_clause):
    """En kötü ve en iyi 10 mal grubu"""
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<p class="section-title section-title-red">🔴 EN KÖTÜ 10 MAL GRUBU (Adet Değişimi)</p>', unsafe_allow_html=True)
        worst = get_mal_grubu_performance(con, where_clause, 'ASC', 10)
        
        if worst.empty:
            st.info("Veri bulunamadı")
        else:
            for idx, row in worst.iterrows():
                mal = row['mal_grubu']
                adet_ch = row['adet_change']
                ciro_ch = row['ciro_change']
                marj_ch = row['marj_change']
                
                with st.expander(f"**{mal}** → Adet: {adet_ch:+.1f}%"):
                    st.markdown(f"""
                    - **Adet**: {row['adet_2024']:,.0f} → {row['adet_2025']:,.0f} ({adet_ch:+.1f}%)
                    - **Ciro**: ₺{row['ciro_2024']:,.0f} → ₺{row['ciro_2025']:,.0f} ({ciro_ch:+.1f}%)
                    - **Marj**: ₺{row['marj_2024']:,.0f} → ₺{row['marj_2025']:,.0f} ({marj_ch:+.1f}%)
                    - **Fire 2025**: ₺{row['fire_2025']:,.0f}
                    """)
                    
                    if st.button(f"📋 Ürünleri Göster", key=f"worst_{idx}"):
                        products = get_product_details(con, mal, where_clause)
                        st.dataframe(products, use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown('<p class="section-title section-title-green">🟢 EN İYİ 10 MAL GRUBU (Adet Değişimi)</p>', unsafe_allow_html=True)
        best = get_mal_grubu_performance(con, where_clause, 'DESC', 10)
        
        if best.empty:
            st.info("Veri bulunamadı")
        else:
            for idx, row in best.iterrows():
                mal = row['mal_grubu']
                adet_ch = row['adet_change']
                ciro_ch = row['ciro_change']
                marj_ch = row['marj_change']
                
                with st.expander(f"**{mal}** → Adet: {adet_ch:+.1f}%"):
                    st.markdown(f"""
                    - **Adet**: {row['adet_2024']:,.0f} → {row['adet_2025']:,.0f} ({adet_ch:+.1f}%)
                    - **Ciro**: ₺{row['ciro_2024']:,.0f} → ₺{row['ciro_2025']:,.0f} ({ciro_ch:+.1f}%)
                    - **Marj**: ₺{row['marj_2024']:,.0f} → ₺{row['marj_2025']:,.0f} ({marj_ch:+.1f}%)
                    - **Fire 2025**: ₺{row['fire_2025']:,.0f}
                    """)
                    
                    if st.button(f"📋 Ürünleri Göster", key=f"best_{idx}"):
                        products = get_product_details(con, mal, where_clause)
                        st.dataframe(products, use_container_width=True, hide_index=True)


# ============================================================================
# MAIN
# ============================================================================

def main():
    st.markdown('<h1 class="main-header">📊 Performans Analizi</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Kasım 2024 → Kasım 2025 | Ana Metrik: Satış Miktarı</p>', unsafe_allow_html=True)
    
    # Dosya yükleme
    col1, col2 = st.columns(2)
    with col1:
        file_2024 = st.file_uploader("📁 2024 Kasım", type=['xlsx'], key='f2024')
    with col2:
        file_2025 = st.file_uploader("📁 2025 Kasım", type=['xlsx'], key='f2025')
    
    if not file_2024 or not file_2025:
        st.info("👆 Her iki dosyayı da yükleyin")
        
        st.markdown("""
        ### ℹ️ Bu Dashboard Ne Yapar?
        
        **Sadece şu nitelikleri analiz eder:**
        - Spot
        - Grup Spot
        - Regule
        - Kasa Aktivitesi
        - Bölgesel
        
        **Gösterir:**
        - 📦 **Satış Miktarı** ana metrik
        - 🔴 **En Kötü 10 Mal Grubu** (adet düşüşüne göre)
        - 🟢 **En İyi 10 Mal Grubu** (adet artışına göre)
        - 📋 **Ürün detayları** (her mal grubunun ürünleri)
        - 📥 **Excel rapor** (seçilen filtreye göre)
        """)
        return
    
    # Veri yükle
    cache_key = f"{file_2024.name}_{file_2025.name}_{file_2024.size}"
    data = load_data(file_2024.getvalue(), file_2025.getvalue(), cache_key)
    
    # Sidebar filtreleri
    filters = render_sidebar(data['filters'])
    filter_desc = get_filter_description(filters)
    where_clause = build_where_clause(filters)
    
    # DuckDB bağlantısı - DataFrame'den direkt
    con = duckdb.connect()
    con.register('veri', data['df'])
    
    # Filtre bilgisi
    st.markdown(f'<div class="filter-info">📍 <strong>Filtre:</strong> {filter_desc}</div>', unsafe_allow_html=True)
    
    # Excel rapor butonu
    excel = create_excel_report(con, where_clause, filter_desc)
    st.download_button(
        "📥 EXCEL RAPORU İNDİR",
        excel,
        f"performans_raporu_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    
    st.markdown("---")
    
    # KPI'lar
    summary = get_summary(con, where_clause)
    render_kpis(summary)
    
    st.markdown("---")
    
    # En kötü / en iyi
    render_worst_best(con, where_clause)
    
    # Footer
    st.markdown("---")
    st.caption(f"Kayıt: 2024={data['counts']['2024']:,} | 2025={data['counts']['2025']:,} | Min. Baz: {MIN_BASE_ADET} adet")
    
    con.close()


if __name__ == "__main__":
    main()
