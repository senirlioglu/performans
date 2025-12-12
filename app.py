"""
🎯 SATIŞ ANALİZ KARAR SİSTEMİ v5.1
━━━━━━━━━━━━━━━━━━━━━━━━━━
Bu bir dashboard değil, KARAR sistemi.
3 dakikada teşhis, neden, aksiyon.

Mimari: Excel → DataFrame → DuckDB → Karar
(Parquet kaldırıldı - Cloud uyumluluğu için)
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
    page_title="Satış Karar Sistemi",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# SABİTLER
# ============================================================================

GECERLI_NITELIKLER = ['Spot', 'Grup Spot', 'Regule', 'Kasa Aktivitesi', 'Bölgesel']

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

NUMERIK_KOLONLAR = ['Adet', 'Ciro', 'Marj', 'Fire', 'Envanter', 'Kampanya_Zarar']

# ============================================================================
# CSS
# ============================================================================
st.markdown("""
<style>
    .main-title {font-size: 2rem; font-weight: 700; color: #1e293b; margin-bottom: 0;}
    .sub-title {font-size: 1rem; color: #64748b; margin-bottom: 1.5rem;}
    
    .kpi-card {
        background: white; border: 1px solid #e2e8f0; border-radius: 12px;
        padding: 1.25rem; text-align: center;
    }
    .kpi-label {font-size: 0.8rem; color: #64748b; text-transform: uppercase;}
    .kpi-value {font-size: 1.75rem; font-weight: 700; color: #1e293b; margin: 0.25rem 0;}
    .kpi-delta {font-size: 0.9rem; font-weight: 600;}
    .delta-up {color: #10b981;}
    .delta-down {color: #ef4444;}
    
    .section-title {
        font-size: 1.1rem; font-weight: 600; color: #334155;
        padding-bottom: 0.5rem; border-bottom: 2px solid #e2e8f0;
        margin: 1.5rem 0 1rem 0;
    }
    
    .filter-badge {
        display: inline-block; background: #f1f5f9; padding: 0.5rem 1rem;
        border-radius: 8px; font-size: 0.85rem; color: #475569; margin-bottom: 1rem;
    }
    
    .neden-box {
        background: #fef3c7; padding: 0.5rem; border-radius: 6px;
        font-size: 0.85rem; margin-top: 0.5rem;
    }
    .aksiyon-box {
        color: #0369a1; font-size: 0.85rem; margin-top: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# VERİ OKUMA
# ============================================================================

def excel_oku(file_bytes: bytes, yil: int) -> pd.DataFrame:
    """Excel'i oku ve temizle"""
    
    df = pd.read_excel(BytesIO(file_bytes), engine='openpyxl')
    df.columns = df.columns.str.strip()
    
    # Kolon isimlerini kısalt
    df = df.rename(columns=KOLON_MAP)
    df['Yil'] = yil
    
    # Nitelik filtresi
    if 'Nitelik' in df.columns:
        df = df[df['Nitelik'].isin(GECERLI_NITELIKLER)]
    
    # Numerik kolonlar
    for col in NUMERIK_KOLONLAR:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # String kolonlar
    str_cols = ['SM', 'BS', 'Magaza_Kod', 'Magaza_Ad', 'Nitelik', 
                'Urun_Grubu', 'Ust_Mal', 'Mal_Grubu', 'Urun_Kod', 'Urun_Ad']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['nan', 'None', 'NaN', '<NA>'], '')
    
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def veri_yukle(_bytes_2024: bytes, _bytes_2025: bytes, cache_key: str) -> dict:
    """Ana veri yükleme - DataFrame tabanlı"""
    
    progress = st.progress(0, text="📂 2024 verisi okunuyor...")
    df_2024 = excel_oku(_bytes_2024, 2024)
    
    progress.progress(40, text="📂 2025 verisi okunuyor...")
    df_2025 = excel_oku(_bytes_2025, 2025)
    
    progress.progress(70, text="🔧 Veriler birleştiriliyor...")
    df_all = pd.concat([df_2024, df_2025], ignore_index=True)
    
    # Filtre seçenekleri
    filtreler = {
        'sm': sorted(df_all[df_all['SM'] != '']['SM'].unique().tolist()),
        'nitelik': sorted(df_all[df_all['Nitelik'] != '']['Nitelik'].unique().tolist()),
        'urun_grubu': sorted(df_all[df_all['Urun_Grubu'] != '']['Urun_Grubu'].unique().tolist()),
    }
    
    # BS by SM
    bs_df = df_all[df_all['BS'] != ''][['SM', 'BS']].drop_duplicates()
    filtreler['bs_map'] = bs_df.groupby('SM')['BS'].apply(list).to_dict()
    
    # Mağaza by BS
    mag_df = df_all[df_all['Magaza_Kod'] != ''][['BS', 'Magaza_Kod', 'Magaza_Ad']].drop_duplicates()
    filtreler['magaza_map'] = mag_df.groupby('BS').apply(
        lambda x: list(zip(x['Magaza_Kod'], x['Magaza_Ad']))
    ).to_dict()
    
    # Üst Mal by Ürün Grubu
    ust_df = df_all[df_all['Ust_Mal'] != ''][['Urun_Grubu', 'Ust_Mal']].drop_duplicates()
    filtreler['ust_mal_map'] = ust_df.groupby('Urun_Grubu')['Ust_Mal'].apply(list).to_dict()
    
    # Mal Grubu by Üst Mal
    mal_df = df_all[df_all['Mal_Grubu'] != ''][['Ust_Mal', 'Mal_Grubu']].drop_duplicates()
    filtreler['mal_grubu_map'] = mal_df.groupby('Ust_Mal')['Mal_Grubu'].apply(list).to_dict()
    
    sayilar = {'2024': len(df_2024), '2025': len(df_2025)}
    
    del df_2024, df_2025
    gc.collect()
    
    progress.progress(100, text="✅ Hazır!")
    progress.empty()
    
    return {
        'df': df_all,
        'filtreler': filtreler,
        'sayilar': sayilar
    }


# ============================================================================
# DUCKDB SORGULARI
# ============================================================================

def build_where(f: dict) -> str:
    """Filtre → SQL WHERE"""
    
    kosullar = []
    
    if f.get('sm') and f['sm'] != 'Tümü':
        kosullar.append(f"SM = '{f['sm']}'")
    if f.get('bs') and f['bs'] != 'Tümü':
        kosullar.append(f"BS = '{f['bs']}'")
    if f.get('magaza') and f['magaza'] != 'Tümü':
        kosullar.append(f"Magaza_Kod = '{f['magaza']}'")
    if f.get('nitelik') and f['nitelik'] != 'Tümü':
        kosullar.append(f"Nitelik = '{f['nitelik']}'")
    if f.get('urun_grubu') and f['urun_grubu'] != 'Tümü':
        kosullar.append(f"Urun_Grubu = '{f['urun_grubu']}'")
    if f.get('ust_mal') and f['ust_mal'] != 'Tümü':
        kosullar.append(f"Ust_Mal = '{f['ust_mal']}'")
    if f.get('mal_grubu') and f['mal_grubu'] != 'Tümü':
        kosullar.append(f"Mal_Grubu = '{f['mal_grubu']}'")
    
    return "WHERE " + " AND ".join(kosullar) if kosullar else ""


def get_ozet(con, where: str) -> dict:
    """Özet KPI'lar"""
    
    sql = f"""
        SELECT 
            Yil,
            SUM(Adet) as Adet,
            SUM(Ciro) as Ciro,
            SUM(Marj) as Marj,
            SUM(ABS(Fire)) as Fire
        FROM veri
        {where}
        GROUP BY Yil
    """
    
    df = con.execute(sql).fetchdf()
    
    sonuc = {}
    for _, row in df.iterrows():
        yil = int(row['Yil'])
        for col in ['Adet', 'Ciro', 'Marj', 'Fire']:
            sonuc[f'{col.lower()}_{yil}'] = float(row[col]) if pd.notna(row[col]) else 0
    
    # Değişim
    for m in ['adet', 'ciro', 'marj', 'fire']:
        v24 = sonuc.get(f'{m}_2024', 0) or 0
        v25 = sonuc.get(f'{m}_2025', 0) or 0
        sonuc[f'{m}_degisim'] = ((v25 / v24) - 1) * 100 if v24 > 0 else 0
    
    return sonuc


def get_mal_grubu_analiz(con, where: str, min_ciro: float) -> pd.DataFrame:
    """Mal Grubu bazında analiz"""
    
    ciro_filtre = f"HAVING SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) >= {min_ciro}" if min_ciro > 0 else ""
    
    sql = f"""
        SELECT 
            Mal_Grubu,
            MAX(Ust_Mal) as Ust_Mal,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2024 THEN ABS(Fire) ELSE 0 END) as Fire_2024,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {where}
        {"WHERE" if not where else "AND"} Mal_Grubu != ''
        GROUP BY Mal_Grubu
        {ciro_filtre}
    """.replace("WHERE WHERE", "WHERE").replace("WHERE AND", "WHERE")
    
    df = con.execute(sql).fetchdf()
    
    if df.empty:
        return df
    
    # Değişim hesapla
    df['Adet_Deg'] = df.apply(lambda r: ((r['Adet_2025']/r['Adet_2024'])-1)*100 if r['Adet_2024']>0 else 0, axis=1)
    df['Ciro_Deg'] = df.apply(lambda r: ((r['Ciro_2025']/r['Ciro_2024'])-1)*100 if r['Ciro_2024']>0 else 0, axis=1)
    df['Marj_Deg'] = df.apply(lambda r: ((r['Marj_2025']/r['Marj_2024'])-1)*100 if r['Marj_2024']>0 else 0, axis=1)
    df['Fire_Deg'] = df.apply(lambda r: ((r['Fire_2025']/r['Fire_2024'])-1)*100 if r['Fire_2024']>0 else 0, axis=1)
    
    return df


def get_urun_detay(con, mal_grubu: str, where: str) -> pd.DataFrame:
    """Ürün detayları"""
    
    mal_kosul = f"Mal_Grubu = '{mal_grubu}'"
    full_where = f"{where} AND {mal_kosul}" if where else f"WHERE {mal_kosul}"
    
    sql = f"""
        SELECT 
            Urun_Kod,
            MAX(Urun_Ad) as Urun_Ad,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {full_where}
        GROUP BY Urun_Kod
        ORDER BY Adet_2025 DESC
    """
    
    df = con.execute(sql).fetchdf()
    
    if not df.empty:
        df['Adet_Deg'] = df.apply(lambda r: ((r['Adet_2025']/r['Adet_2024'])-1)*100 if r['Adet_2024']>0 else 0, axis=1)
    
    return df


# ============================================================================
# OTOMATİK YORUM
# ============================================================================

def neden_tespit(row: pd.Series) -> tuple:
    """Neden ve aksiyon"""
    
    marj_deg = row.get('Marj_Deg', 0) or 0
    adet_deg = row.get('Adet_Deg', 0) or 0
    fire_deg = row.get('Fire_Deg', 0) or 0
    
    if fire_deg > 50:
        return ("🔥 Fire artışı kritik", "SKT kontrolü, sipariş azalt", "red")
    
    if adet_deg < -15 and fire_deg < 20:
        return ("📦 Bulunurluk problemi", "Raf yerleşimi ve stok kontrol", "yellow")
    
    if adet_deg < -10 and fire_deg > 30:
        return ("⚠️ Stok/SKT sorunu", "Sipariş miktarını düşür", "red")
    
    if marj_deg < -20:
        return ("💰 Marj erimesi", "Fiyatlama ve SMM kontrol", "yellow")
    
    if adet_deg > 20:
        return ("✅ Başarılı performans", "Başarı faktörlerini analiz et", "green")
    
    if adet_deg < 0:
        return ("📉 Performans düşüşü", "Detaylı analiz gerekli", "yellow")
    
    return ("📊 Normal", "-", "green")


# ============================================================================
# EXCEL RAPOR
# ============================================================================

def excel_rapor(con, where: str, min_ciro: float, filtre_text: str) -> BytesIO:
    """Excel raporu"""
    
    output = BytesIO()
    
    df = get_mal_grubu_analiz(con, where, min_ciro)
    
    if not df.empty:
        df['Neden'] = df.apply(lambda r: neden_tespit(r)[0], axis=1)
        df['Aksiyon'] = df.apply(lambda r: neden_tespit(r)[1], axis=1)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Bilgi
        pd.DataFrame([{
            'Filtre': filtre_text,
            'Min Ciro': f"₺{min_ciro:,.0f}",
            'Tarih': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
        }]).to_excel(writer, sheet_name='Bilgi', index=False)
        
        # En Kötü
        if not df.empty:
            df.nsmallest(20, 'Adet_Deg').to_excel(writer, sheet_name='En Kötü 20', index=False)
            df.nlargest(20, 'Adet_Deg').to_excel(writer, sheet_name='En İyi 20', index=False)
            df.to_excel(writer, sheet_name='Tüm Veriler', index=False)
    
    output.seek(0)
    return output


# ============================================================================
# UI
# ============================================================================

def sidebar_filtreler(opts: dict) -> dict:
    """Filtreler"""
    
    st.sidebar.markdown("## 🎛️ FİLTRELER")
    
    st.sidebar.markdown("### 📍 Organizasyon")
    
    sm_list = ['Tümü'] + opts.get('sm', [])
    secili_sm = st.sidebar.selectbox('SM', sm_list)
    
    if secili_sm != 'Tümü':
        bs_opts = opts.get('bs_map', {}).get(secili_sm, [])
    else:
        bs_opts = []
        for v in opts.get('bs_map', {}).values():
            bs_opts.extend(v)
        bs_opts = sorted(set(bs_opts))
    
    secili_bs = st.sidebar.selectbox('BS', ['Tümü'] + bs_opts)
    
    if secili_bs != 'Tümü':
        mag_opts = opts.get('magaza_map', {}).get(secili_bs, [])
        mag_list = ['Tümü'] + [f"{k} - {a}" for k, a in mag_opts]
    else:
        mag_list = ['Tümü']
    
    secili_mag = st.sidebar.selectbox('Mağaza', mag_list)
    secili_mag_kod = secili_mag.split(' - ')[0] if secili_mag != 'Tümü' else 'Tümü'
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📦 Ürün")
    
    secili_nitelik = st.sidebar.selectbox('Nitelik', ['Tümü'] + opts.get('nitelik', []))
    secili_urun = st.sidebar.selectbox('Ürün Grubu', ['Tümü'] + opts.get('urun_grubu', []))
    
    if secili_urun != 'Tümü':
        ust_opts = opts.get('ust_mal_map', {}).get(secili_urun, [])
    else:
        ust_opts = []
        for v in opts.get('ust_mal_map', {}).values():
            ust_opts.extend(v)
        ust_opts = sorted(set(ust_opts))
    
    secili_ust = st.sidebar.selectbox('Üst Mal Grubu', ['Tümü'] + ust_opts)
    
    if secili_ust != 'Tümü':
        mal_opts = opts.get('mal_grubu_map', {}).get(secili_ust, [])
    else:
        mal_opts = []
        for v in opts.get('mal_grubu_map', {}).values():
            mal_opts.extend(v)
        mal_opts = sorted(set(mal_opts))
    
    secili_mal = st.sidebar.selectbox('Mal Grubu', ['Tümü'] + mal_opts)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Alt Limit")
    min_ciro = st.sidebar.number_input('2025 Min. Ciro (₺)', min_value=0, value=10000, step=5000)
    
    return {
        'sm': secili_sm, 'bs': secili_bs, 'magaza': secili_mag_kod,
        'nitelik': secili_nitelik, 'urun_grubu': secili_urun,
        'ust_mal': secili_ust, 'mal_grubu': secili_mal,
        'min_ciro': min_ciro
    }


def filtre_text(f: dict) -> str:
    """Filtre açıklaması"""
    p = []
    if f['sm'] != 'Tümü': p.append(f"SM: {f['sm']}")
    if f['bs'] != 'Tümü': p.append(f"BS: {f['bs']}")
    if f['magaza'] != 'Tümü': p.append(f"Mağaza: {f['magaza']}")
    if f['nitelik'] != 'Tümü': p.append(f"Nitelik: {f['nitelik']}")
    if f['urun_grubu'] != 'Tümü': p.append(f"Ürün Grubu: {f['urun_grubu']}")
    if f['ust_mal'] != 'Tümü': p.append(f"Üst Mal: {f['ust_mal']}")
    if f['mal_grubu'] != 'Tümü': p.append(f"Mal Grubu: {f['mal_grubu']}")
    return " | ".join(p) if p else "Tüm Veriler"


def kpi_goster(ozet: dict):
    """KPI kartları"""
    
    cols = st.columns(4)
    metrikler = [
        ('📦 Satış Adedi', 'adet', '{:,.0f}', False),
        ('💰 Ciro', 'ciro', '₺{:,.0f}', False),
        ('📈 Marj', 'marj', '₺{:,.0f}', False),
        ('🔥 Fire', 'fire', '₺{:,.0f}', True),
    ]
    
    for col, (label, key, fmt, ters) in zip(cols, metrikler):
        with col:
            deger = ozet.get(f'{key}_2025', 0)
            degisim = ozet.get(f'{key}_degisim', 0)
            delta_class = 'delta-down' if (degisim > 0) == ters else 'delta-up'
            isaret = '+' if degisim > 0 else ''
            
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{fmt.format(deger)}</div>
                <div class="kpi-delta {delta_class}">{isaret}{degisim:.1f}%</div>
            </div>
            """, unsafe_allow_html=True)


def karar_goster(df: pd.DataFrame, baslik: str, limit: int = 10, ters: bool = False):
    """Karar kartları"""
    
    st.markdown(f'<div class="section-title">{baslik}</div>', unsafe_allow_html=True)
    
    if df.empty:
        st.info("Gösterilecek veri yok")
        return None
    
    df_sorted = df.nlargest(limit, 'Adet_Deg') if ters else df.nsmallest(limit, 'Adet_Deg')
    
    selected = None
    
    for idx, row in df_sorted.iterrows():
        mal = row['Mal_Grubu']
        adet_deg = row.get('Adet_Deg', 0)
        neden, aksiyon, renk = neden_tespit(row)
        
        with st.expander(f"**{mal}** → Adet: {adet_deg:+.1f}%"):
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Adet 2024", f"{row['Adet_2024']:,.0f}")
                st.metric("Ciro 2024", f"₺{row['Ciro_2024']:,.0f}")
            with col2:
                st.metric("Adet 2025", f"{row['Adet_2025']:,.0f}", f"{adet_deg:+.1f}%")
                st.metric("Ciro 2025", f"₺{row['Ciro_2025']:,.0f}", f"{row.get('Ciro_Deg',0):+.1f}%")
            
            st.markdown(f'<div class="neden-box"><strong>Neden:</strong> {neden}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="aksiyon-box">💡 <strong>Aksiyon:</strong> {aksiyon}</div>', unsafe_allow_html=True)
            
            if st.button("📋 Ürünleri Göster", key=f"btn_{idx}_{mal}"):
                selected = mal
    
    return selected


# ============================================================================
# MAIN
# ============================================================================

def main():
    st.markdown('<h1 class="main-title">🎯 Satış Karar Sistemi</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-title">Kasım 2024 → 2025 | 3 dakikada teşhis, neden, aksiyon</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        file_2024 = st.file_uploader("📁 2024 Kasım", type=['xlsx'])
    with col2:
        file_2025 = st.file_uploader("📁 2025 Kasım", type=['xlsx'])
    
    if not file_2024 or not file_2025:
        st.info("👆 Her iki dosyayı da yükleyin")
        st.markdown("""
        ### 🎯 Bu Sistem Ne Yapar?
        - Veri göstermez, **KARAR** üretir
        - Otomatik **neden** tespiti
        - Her sorun için **aksiyon** önerisi
        
        **Analiz:** Spot, Grup Spot, Regule, Kasa Aktivitesi, Bölgesel
        """)
        return
    
    # Veri yükle
    cache_key = f"{file_2024.name}_{file_2025.name}_{file_2024.size}"
    
    try:
        veri = veri_yukle(file_2024.getvalue(), file_2025.getvalue(), cache_key)
    except Exception as e:
        st.error(f"Hata: {e}")
        return
    
    # Filtreler
    secili = sidebar_filtreler(veri['filtreler'])
    where = build_where(secili)
    filtre = filtre_text(secili)
    
    # DuckDB
    con = duckdb.connect()
    con.register('veri', veri['df'])
    
    # Filtre bilgisi
    st.markdown(f'<div class="filter-badge">📍 {filtre} | Min: ₺{secili["min_ciro"]:,}</div>', unsafe_allow_html=True)
    
    # Excel rapor
    excel = excel_rapor(con, where, secili['min_ciro'], filtre)
    st.download_button("📥 EXCEL RAPORU", excel, f"rapor_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx")
    
    st.markdown("---")
    
    # KPI'lar
    ozet = get_ozet(con, where)
    kpi_goster(ozet)
    
    st.markdown("---")
    
    # Analiz
    df_analiz = get_mal_grubu_analiz(con, where, secili['min_ciro'])
    
    col1, col2 = st.columns(2)
    
    with col1:
        selected1 = karar_goster(df_analiz, "🔴 EN KÖTÜ 10", limit=10, ters=False)
    
    with col2:
        selected2 = karar_goster(df_analiz, "🟢 EN İYİ 10", limit=10, ters=True)
    
    # Ürün detay
    selected = selected1 or selected2
    if selected:
        st.markdown("---")
        st.markdown(f"### 📋 {selected} - Ürün Detayları")
        df_urun = get_urun_detay(con, selected, where)
        if not df_urun.empty:
            st.dataframe(df_urun, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    st.caption(f"📊 2024: {veri['sayilar']['2024']:,} | 2025: {veri['sayilar']['2025']:,}")
    
    con.close()


if __name__ == "__main__":
    main()
