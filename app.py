"""
🎯 SATIŞ KARAR SİSTEMİ v6
━━━━━━━━━━━━━━━━━━━━━━━━
Parquet'ten okur - Süper Hızlı
Dosya yükleme YOK - Direkt açılır

Güncelleme: Ayda 1 kez parquet dosyalarını değiştir
"""

import streamlit as st
import pandas as pd
import duckdb
from io import BytesIO
import warnings

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

# GitHub'daki parquet dosyaları (raw URL)
# Bu URL'leri kendi repo'na göre güncelle!
PARQUET_2024 = "veri_2024.parquet"
PARQUET_2025 = "veri_2025.parquet"

NUMERIK_KOLONLAR = ['Adet', 'Ciro', 'Marj', 'Fire', 'Envanter', 'Kampanya_Zarar']

# ============================================================================
# CSS
# ============================================================================
st.markdown("""
<style>
    /* Genel padding azaltma */
    .block-container {padding-top: 1rem !important; padding-bottom: 0 !important;}
    
    .main-title {font-size: 1.3rem; font-weight: 700; color: #1e293b; margin-bottom: 0; margin-top: 0;}
    .sub-title {font-size: 0.85rem; color: #64748b; margin-bottom: 0.5rem;}
    
    .kpi-card {
        background: white; border: 1px solid #e2e8f0; border-radius: 8px;
        padding: 0.75rem; text-align: center;
    }
    .kpi-label {font-size: 0.7rem; color: #64748b; text-transform: uppercase;}
    .kpi-value {font-size: 1.4rem; font-weight: 700; color: #1e293b; margin: 0.15rem 0;}
    .kpi-delta {font-size: 0.8rem; font-weight: 600;}
    .delta-up {color: #10b981;}
    .delta-down {color: #ef4444;}
    
    .section-title {
        font-size: 1rem; font-weight: 600; color: #334155;
        padding-bottom: 0.3rem; border-bottom: 2px solid #e2e8f0;
        margin: 0.75rem 0 0.5rem 0;
    }
    
    .filter-badge {
        display: inline-block; background: #f1f5f9; padding: 0.3rem 0.75rem;
        border-radius: 6px; font-size: 0.8rem; color: #475569; margin-bottom: 0.5rem;
    }
    
    .neden-box {
        background: #fef3c7; padding: 0.4rem; border-radius: 6px;
        font-size: 0.8rem; margin-top: 0.4rem;
    }
    .aksiyon-box {
        color: #0369a1; font-size: 0.8rem; margin-top: 0.4rem;
    }
    
    .success-box {
        background: #d1fae5; border: 1px solid #10b981; border-radius: 6px;
        padding: 0.5rem; margin: 0.5rem 0; font-size: 0.85rem;
    }
    
    /* Streamlit varsayılanlarını sıkıştır */
    .stTabs [data-baseweb="tab-list"] {gap: 8px;}
    .stTabs [data-baseweb="tab"] {padding: 8px 16px; font-size: 0.9rem;}
    div[data-testid="stExpander"] {margin-bottom: 0.3rem;}
    
    /* Detay bölümü için highlight */
    .detay-baslik {
        background: linear-gradient(90deg, #3b82f6 0%, #1d4ed8 100%);
        color: white;
        padding: 0.75rem 1rem;
        border-radius: 8px;
        margin: 1rem 0 0.5rem 0;
        font-weight: 600;
        font-size: 1rem;
        scroll-margin-top: 80px;
    }
</style>
""", unsafe_allow_html=True)


def scroll_to(element_id: str):
    """JavaScript ile scroll"""
    js = f"""
    <script>
        var element = window.parent.document.getElementById("{element_id}");
        if (element) {{
            element.scrollIntoView({{behavior: 'smooth', block: 'start'}});
        }}
    </script>
    """
    st.components.v1.html(js, height=0)


# ============================================================================
# VERİ OKUMA (PARQUET - SÜPER HIZLI)
# ============================================================================

@st.cache_resource
def get_db_connection():
    """DuckDB bağlantısı - tek sefer oluştur"""
    return duckdb.connect()


@st.cache_data(ttl=86400)  # 24 saat cache
def veri_yukle():
    """Parquet dosyalarını oku - ÇOK HIZLI"""
    
    try:
        df_2024 = pd.read_parquet(PARQUET_2024)
        df_2025 = pd.read_parquet(PARQUET_2025)
        
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
        
        return {
            'df': df_all,
            'filtreler': filtreler,
            'sayilar': sayilar,
            'loaded': True
        }
        
    except FileNotFoundError:
        return {'loaded': False, 'error': 'Parquet dosyaları bulunamadı'}
    except Exception as e:
        return {'loaded': False, 'error': str(e)}


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
    df.columns = df.columns.str.lower()
    
    sonuc = {}
    for _, row in df.iterrows():
        yil = int(row['yil'])
        for col in ['adet', 'ciro', 'marj', 'fire']:
            sonuc[f'{col}_{yil}'] = float(row[col]) if pd.notna(row[col]) else 0
    
    # Değişim
    for m in ['adet', 'ciro', 'marj', 'fire']:
        v24 = sonuc.get(f'{m}_2024', 0) or 0
        v25 = sonuc.get(f'{m}_2025', 0) or 0
        sonuc[f'{m}_degisim'] = ((v25 / v24) - 1) * 100 if v24 > 0 else 0
    
    return sonuc


def get_mal_grubu_analiz(con, where: str, min_ciro: float) -> pd.DataFrame:
    """Mal Grubu bazında analiz"""
    
    ciro_filtre = f"HAVING SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) >= {min_ciro}" if min_ciro > 0 else ""
    
    if where:
        where_mal = f"{where} AND Mal_Grubu != ''"
    else:
        where_mal = "WHERE Mal_Grubu != ''"
    
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
        {where_mal}
        GROUP BY Mal_Grubu
        {ciro_filtre}
    """
    
    df = con.execute(sql).fetchdf()
    
    if df.empty:
        return df
    
    df.columns = [c.lower() for c in df.columns]
    
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 0, axis=1)
    df['marj_deg'] = df.apply(lambda r: ((r['marj_2025']/r['marj_2024'])-1)*100 if r['marj_2024']>0 else 0, axis=1)
    df['fire_deg'] = df.apply(lambda r: ((r['fire_2025']/r['fire_2024'])-1)*100 if r['fire_2024']>0 else 0, axis=1)
    
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
    df.columns = [c.lower() for c in df.columns]
    
    if not df.empty:
        df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    
    return df


# ============================================================================
# ÜRÜN GRUBU ANALİZİ FONKSİYONLARI
# ============================================================================

def get_urun_grubu_analiz(con, where: str, min_ciro: float) -> pd.DataFrame:
    """Ürün Grubu bazında analiz"""
    
    ciro_filtre = f"HAVING SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) >= {min_ciro}" if min_ciro > 0 else ""
    
    if where:
        where_ug = f"{where} AND Urun_Grubu != ''"
    else:
        where_ug = "WHERE Urun_Grubu != ''"
    
    sql = f"""
        SELECT 
            Urun_Grubu,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2024 THEN ABS(Fire) ELSE 0 END) as Fire_2024,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {where_ug}
        GROUP BY Urun_Grubu
        {ciro_filtre}
    """
    
    df = con.execute(sql).fetchdf()
    
    if df.empty:
        return df
    
    df.columns = [c.lower() for c in df.columns]
    
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 0, axis=1)
    df['marj_deg'] = df.apply(lambda r: ((r['marj_2025']/r['marj_2024'])-1)*100 if r['marj_2024']>0 else 0, axis=1)
    df['fire_deg'] = df.apply(lambda r: ((r['fire_2025']/r['fire_2024'])-1)*100 if r['fire_2024']>0 else 0, axis=1)
    
    return df


def get_mal_grubu_by_urun_grubu(con, urun_grubu: str, where: str) -> pd.DataFrame:
    """Ürün Grubu için mal grupları detayı"""
    
    ug_kosul = f"Urun_Grubu = '{urun_grubu}'"
    full_where = f"{where} AND {ug_kosul}" if where else f"WHERE {ug_kosul}"
    
    sql = f"""
        SELECT 
            Mal_Grubu,
            MAX(Ust_Mal) as Ust_Mal,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {full_where}
        AND Mal_Grubu != ''
        GROUP BY Mal_Grubu
        ORDER BY Adet_2025 DESC
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if not df.empty:
        df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    
    return df


def get_magaza_dusus_ug(con, urun_grubu: str, where: str, limit: int = 5) -> pd.DataFrame:
    """Ürün Grubu için en çok düşen mağazalar"""
    
    ug_kosul = f"Urun_Grubu = '{urun_grubu}'"
    full_where = f"{where} AND {ug_kosul}" if where else f"WHERE {ug_kosul}"
    
    sql = f"""
        SELECT 
            Magaza_Kod,
            MAX(Magaza_Ad) as Magaza_Ad,
            MAX(BS) as BS,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {full_where}
        GROUP BY Magaza_Kod
        HAVING Adet_2024 > 0
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if df.empty:
        return df
    
    df['adet_fark'] = df['adet_2025'] - df['adet_2024']
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 0, axis=1)
    
    return df.nsmallest(limit, 'adet_fark')


def get_magaza_artis_ug(con, urun_grubu: str, where: str, limit: int = 5) -> pd.DataFrame:
    """Ürün Grubu için en çok yükselen mağazalar"""
    
    ug_kosul = f"Urun_Grubu = '{urun_grubu}'"
    full_where = f"{where} AND {ug_kosul}" if where else f"WHERE {ug_kosul}"
    
    sql = f"""
        SELECT 
            Magaza_Kod,
            MAX(Magaza_Ad) as Magaza_Ad,
            MAX(BS) as BS,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {full_where}
        GROUP BY Magaza_Kod
        HAVING Adet_2024 > 0 OR Adet_2025 > 0
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if df.empty:
        return df
    
    df['adet_fark'] = df['adet_2025'] - df['adet_2024']
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 100, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 100, axis=1)
    
    return df.nlargest(limit, 'adet_fark')


def karar_goster_ug(df: pd.DataFrame, baslik: str, limit: int = 10, ters: bool = False):
    """Ürün Grubu karar kartları"""
    
    st.markdown(f'<div class="section-title">{baslik}</div>', unsafe_allow_html=True)
    
    if df.empty:
        st.info("Gösterilecek veri yok")
        return None, None, None, None
    
    df_sorted = df.nlargest(limit, 'adet_deg') if ters else df.nsmallest(limit, 'adet_deg')
    
    prefix = "ug_iyi" if ters else "ug_kotu"
    selected_mal = None
    selected_mag_dusus = None
    selected_mag_artis = None
    selected_ug = None
    
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        ug = row['urun_grubu']
        adet_deg = row.get('adet_deg', 0)
        neden, aksiyon, renk = neden_tespit(row)
        
        with st.expander(f"**{ug}** → Adet: {adet_deg:+.1f}%"):
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
            with col2:
                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{adet_deg:+.1f}%")
                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row.get('ciro_deg',0):+.1f}%")
            
            st.markdown(f'<div class="neden-box"><strong>Neden:</strong> {neden}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="aksiyon-box">💡 <strong>Aksiyon:</strong> {aksiyon}</div>', unsafe_allow_html=True)
            
            # 4 buton
            btn_col1, btn_col2, btn_col3, btn_col4 = st.columns(4)
            with btn_col1:
                if st.button("📂 Mal Grp", key=f"btn_mal_{prefix}_{i}"):
                    selected_mal = ug
            with btn_col2:
                if st.button("🟢 Yükselen", key=f"btn_artis_{prefix}_{i}"):
                    selected_mag_artis = ug
            with btn_col3:
                if st.button("🔴 Düşen", key=f"btn_dusus_{prefix}_{i}"):
                    selected_mag_dusus = ug
    
    return selected_mal, selected_mag_dusus, selected_mag_artis


def excel_rapor_ug(con, where: str, min_ciro: float, filtre_text: str) -> BytesIO:
    """Ürün Grubu Excel raporu"""
    
    output = BytesIO()
    
    df = get_urun_grubu_analiz(con, where, min_ciro)
    
    if not df.empty:
        df['neden'] = df.apply(lambda r: neden_tespit(r)[0], axis=1)
        df['aksiyon'] = df.apply(lambda r: neden_tespit(r)[1], axis=1)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        pd.DataFrame([{
            'Filtre': filtre_text,
            'Min Ciro': f"₺{min_ciro:,.0f}",
            'Tarih': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M'),
            'Rapor': 'Ürün Grubu Analizi'
        }]).to_excel(writer, sheet_name='Bilgi', index=False)
        
        if not df.empty:
            df.nsmallest(20, 'adet_deg').to_excel(writer, sheet_name='En Kötü 20', index=False)
            df.nlargest(20, 'adet_deg').to_excel(writer, sheet_name='En İyi 20', index=False)
            df.to_excel(writer, sheet_name='Tüm Veriler', index=False)
    
    output.seek(0)
    return output


# ============================================================================
# ÜRÜN (MALZEME) ANALİZİ FONKSİYONLARI
# ============================================================================

def get_urun_analiz(con, where: str, min_ciro: float) -> pd.DataFrame:
    """Ürün (Malzeme) bazında analiz"""
    
    ciro_filtre = f"HAVING SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) >= {min_ciro/5}" if min_ciro > 0 else ""
    
    if where:
        where_u = f"{where} AND Urun_Kod != ''"
    else:
        where_u = "WHERE Urun_Kod != ''"
    
    sql = f"""
        SELECT 
            Urun_Kod,
            MAX(Urun_Ad) as Urun_Ad,
            MAX(Mal_Grubu) as Mal_Grubu,
            MAX(Ust_Mal) as Ust_Mal,
            MAX(Urun_Grubu) as Urun_Grubu,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2024 THEN ABS(Fire) ELSE 0 END) as Fire_2024,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {where_u}
        GROUP BY Urun_Kod
        {ciro_filtre}
    """
    
    df = con.execute(sql).fetchdf()
    
    if df.empty:
        return df
    
    df.columns = [c.lower() for c in df.columns]
    
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 0, axis=1)
    df['marj_deg'] = df.apply(lambda r: ((r['marj_2025']/r['marj_2024'])-1)*100 if r['marj_2024']>0 else 0, axis=1)
    df['fire_deg'] = df.apply(lambda r: ((r['fire_2025']/r['fire_2024'])-1)*100 if r['fire_2024']>0 else 0, axis=1)
    
    return df


def get_magaza_dusus_urun(con, urun_kod: str, where: str, limit: int = 5) -> pd.DataFrame:
    """Ürün için en çok düşen mağazalar"""
    
    u_kosul = f"Urun_Kod = '{urun_kod}'"
    full_where = f"{where} AND {u_kosul}" if where else f"WHERE {u_kosul}"
    
    sql = f"""
        SELECT 
            Magaza_Kod,
            MAX(Magaza_Ad) as Magaza_Ad,
            MAX(BS) as BS,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {full_where}
        GROUP BY Magaza_Kod
        HAVING Adet_2024 > 0
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if df.empty:
        return df
    
    df['adet_fark'] = df['adet_2025'] - df['adet_2024']
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 0, axis=1)
    
    return df.nsmallest(limit, 'adet_fark')


def get_magaza_artis_urun(con, urun_kod: str, where: str, limit: int = 5) -> pd.DataFrame:
    """Ürün için en çok yükselen mağazalar"""
    
    u_kosul = f"Urun_Kod = '{urun_kod}'"
    full_where = f"{where} AND {u_kosul}" if where else f"WHERE {u_kosul}"
    
    sql = f"""
        SELECT 
            Magaza_Kod,
            MAX(Magaza_Ad) as Magaza_Ad,
            MAX(BS) as BS,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {full_where}
        GROUP BY Magaza_Kod
        HAVING Adet_2024 > 0 OR Adet_2025 > 0
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if df.empty:
        return df
    
    df['adet_fark'] = df['adet_2025'] - df['adet_2024']
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 100, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 100, axis=1)
    
    return df.nlargest(limit, 'adet_fark')


def karar_goster_urun(df: pd.DataFrame, baslik: str, limit: int = 20, ters: bool = False):
    """Ürün karar kartları"""
    
    st.markdown(f'<div class="section-title">{baslik}</div>', unsafe_allow_html=True)
    
    if df.empty:
        st.info("Gösterilecek veri yok")
        return None, None
    
    df_sorted = df.nlargest(limit, 'adet_deg') if ters else df.nsmallest(limit, 'adet_deg')
    
    prefix = "urun_iyi" if ters else "urun_kotu"
    selected_mag_dusus = None
    selected_mag_artis = None
    
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        urun_ad = row['urun_ad'][:40] + "..." if len(str(row['urun_ad'])) > 40 else row['urun_ad']
        urun_kod = row['urun_kod']
        adet_deg = row.get('adet_deg', 0)
        neden, aksiyon, renk = neden_tespit(row)
        
        with st.expander(f"**{urun_ad}** → Adet: {adet_deg:+.1f}%"):
            st.caption(f"Kod: {urun_kod} | Mal Grubu: {row['mal_grubu']} | Ürün Grubu: {row['urun_grubu']}")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                st.metric("Fire 2024", f"₺{row['fire_2024']:,.0f}")
            with col2:
                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{adet_deg:+.1f}%")
                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row.get('ciro_deg',0):+.1f}%")
                st.metric("Fire 2025", f"₺{row['fire_2025']:,.0f}", f"{row.get('fire_deg',0):+.1f}%")
            
            st.markdown(f'<div class="neden-box"><strong>Neden:</strong> {neden}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="aksiyon-box">💡 <strong>Aksiyon:</strong> {aksiyon}</div>', unsafe_allow_html=True)
            
            # 2 buton
            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                if st.button("🟢 Yükselen Mağ.", key=f"btn_artis_{prefix}_{i}"):
                    selected_mag_artis = urun_kod
            with btn_col2:
                if st.button("🔴 Düşen Mağ.", key=f"btn_dusus_{prefix}_{i}"):
                    selected_mag_dusus = urun_kod
    
    return selected_mag_dusus, selected_mag_artis


def excel_rapor_urun(con, where: str, min_ciro: float, filtre_text: str) -> BytesIO:
    """Ürün Excel raporu"""
    
    output = BytesIO()
    
    df = get_urun_analiz(con, where, min_ciro)
    
    if not df.empty:
        df['neden'] = df.apply(lambda r: neden_tespit(r)[0], axis=1)
        df['aksiyon'] = df.apply(lambda r: neden_tespit(r)[1], axis=1)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        pd.DataFrame([{
            'Filtre': filtre_text,
            'Min Ciro': f"₺{min_ciro:,.0f}",
            'Tarih': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M'),
            'Rapor': 'Ürün Analizi'
        }]).to_excel(writer, sheet_name='Bilgi', index=False)
        
        if not df.empty:
            df.nsmallest(50, 'adet_deg').to_excel(writer, sheet_name='En Kötü 50', index=False)
            df.nlargest(50, 'adet_deg').to_excel(writer, sheet_name='En İyi 50', index=False)
            df.to_excel(writer, sheet_name='Tüm Veriler', index=False)
    
    output.seek(0)
    return output


# ============================================================================
# EN ÇOK / EN AZ SATAN FONKSİYONLARI
# ============================================================================

def get_urun_adet_sirali(con, where: str, min_adet: int = 0) -> pd.DataFrame:
    """Ürünleri 2025 adet bazında sırala"""
    
    adet_filtre = f"HAVING SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) >= {min_adet}" if min_adet > 0 else ""
    
    if where:
        where_u = f"{where} AND Urun_Kod != ''"
    else:
        where_u = "WHERE Urun_Kod != ''"
    
    sql = f"""
        SELECT 
            Urun_Kod,
            MAX(Urun_Ad) as Urun_Ad,
            MAX(Mal_Grubu) as Mal_Grubu,
            MAX(Urun_Grubu) as Urun_Grubu,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025
        FROM veri
        {where_u}
        GROUP BY Urun_Kod
        {adet_filtre}
    """
    
    df = con.execute(sql).fetchdf()
    
    if df.empty:
        return df
    
    df.columns = [c.lower() for c in df.columns]
    
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 0, axis=1)
    
    return df


def get_magaza_adet_sirali(con, urun_kod: str, where: str) -> pd.DataFrame:
    """Ürün için mağazaları adet bazında sırala"""
    
    u_kosul = f"Urun_Kod = '{urun_kod}'"
    full_where = f"{where} AND {u_kosul}" if where else f"WHERE {u_kosul}"
    
    sql = f"""
        SELECT 
            Magaza_Kod,
            MAX(Magaza_Ad) as Magaza_Ad,
            MAX(BS) as BS,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025
        FROM veri
        {full_where}
        GROUP BY Magaza_Kod
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if df.empty:
        return df
    
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    
    return df


def karar_goster_adet(df: pd.DataFrame, baslik: str, limit: int = 20, en_cok: bool = True):
    """Adet bazlı ürün kartları"""
    
    st.markdown(f'<div class="section-title">{baslik}</div>', unsafe_allow_html=True)
    
    if df.empty:
        st.info("Gösterilecek veri yok")
        return None, None
    
    df_sorted = df.nlargest(limit, 'adet_2025') if en_cok else df.nsmallest(limit, 'adet_2025')
    # En az satan için 0'dan büyük olanları filtrele
    if not en_cok:
        df_sorted = df[df['adet_2025'] > 0].nsmallest(limit, 'adet_2025')
    
    prefix = "adet_cok" if en_cok else "adet_az"
    selected_mag_cok = None
    selected_mag_az = None
    
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        urun_ad = row['urun_ad'][:35] + "..." if len(str(row['urun_ad'])) > 35 else row['urun_ad']
        urun_kod = row['urun_kod']
        adet_2025 = row['adet_2025']
        adet_deg = row.get('adet_deg', 0)
        
        # Renk belirle
        deg_renk = "🟢" if adet_deg > 0 else "🔴" if adet_deg < 0 else "⚪"
        
        with st.expander(f"**{urun_ad}** → {adet_2025:,.0f} adet ({deg_renk} {adet_deg:+.1f}%)"):
            st.caption(f"Kod: {urun_kod} | Mal Grubu: {row['mal_grubu']}")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
            with col2:
                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{adet_deg:+.1f}%")
                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row.get('ciro_deg',0):+.1f}%")
            
            # 2 buton
            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                if st.button("🏆 En Çok Satan 10 Mağaza", key=f"btn_cok_{prefix}_{i}"):
                    selected_mag_cok = urun_kod
            with btn_col2:
                if st.button("📉 En Az Satan 10 Mağaza", key=f"btn_az_{prefix}_{i}"):
                    selected_mag_az = urun_kod
    
    return selected_mag_cok, selected_mag_az


def excel_rapor_adet(con, where: str, filtre_text: str) -> BytesIO:
    """En Çok/Az Satan Excel raporu"""
    
    output = BytesIO()
    
    df = get_urun_adet_sirali(con, where, 0)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        pd.DataFrame([{
            'Filtre': filtre_text,
            'Tarih': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M'),
            'Rapor': 'En Çok / En Az Satan Ürünler'
        }]).to_excel(writer, sheet_name='Bilgi', index=False)
        
        if not df.empty:
            df.nlargest(50, 'adet_2025').to_excel(writer, sheet_name='En Çok Satan 50', index=False)
            df[df['adet_2025'] > 0].nsmallest(50, 'adet_2025').to_excel(writer, sheet_name='En Az Satan 50', index=False)
            df.to_excel(writer, sheet_name='Tüm Veriler', index=False)
    
    output.seek(0)
    return output


def get_magaza_dusus(con, mal_grubu: str, where: str, limit: int = 5) -> pd.DataFrame:
    """Mal grubu için en çok düşen mağazalar"""
    
    mal_kosul = f"Mal_Grubu = '{mal_grubu}'"
    full_where = f"{where} AND {mal_kosul}" if where else f"WHERE {mal_kosul}"
    
    sql = f"""
        SELECT 
            Magaza_Kod,
            MAX(Magaza_Ad) as Magaza_Ad,
            MAX(BS) as BS,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {full_where}
        GROUP BY Magaza_Kod
        HAVING Adet_2024 > 0
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if df.empty:
        return df
    
    # Değişim hesapla
    df['adet_fark'] = df['adet_2025'] - df['adet_2024']
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 0, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 0, axis=1)
    
    # En çok düşene göre sırala
    df = df.nsmallest(limit, 'adet_fark')
    
    return df


def get_magaza_artis(con, mal_grubu: str, where: str, limit: int = 5) -> pd.DataFrame:
    """Mal grubu için en çok yükselen mağazalar"""
    
    mal_kosul = f"Mal_Grubu = '{mal_grubu}'"
    full_where = f"{where} AND {mal_kosul}" if where else f"WHERE {mal_kosul}"
    
    sql = f"""
        SELECT 
            Magaza_Kod,
            MAX(Magaza_Ad) as Magaza_Ad,
            MAX(BS) as BS,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025
        FROM veri
        {full_where}
        GROUP BY Magaza_Kod
        HAVING Adet_2024 > 0 OR Adet_2025 > 0
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if df.empty:
        return df
    
    # Değişim hesapla
    df['adet_fark'] = df['adet_2025'] - df['adet_2024']
    df['adet_deg'] = df.apply(lambda r: ((r['adet_2025']/r['adet_2024'])-1)*100 if r['adet_2024']>0 else 100, axis=1)
    df['ciro_deg'] = df.apply(lambda r: ((r['ciro_2025']/r['ciro_2024'])-1)*100 if r['ciro_2024']>0 else 100, axis=1)
    
    # En çok artana göre sırala
    df = df.nlargest(limit, 'adet_fark')
    
    return df


# ============================================================================
# OTOMATİK YORUM
# ============================================================================

def neden_tespit(row: pd.Series) -> tuple:
    """Neden ve aksiyon"""
    
    marj_deg = row.get('marj_deg', 0) or 0
    adet_deg = row.get('adet_deg', 0) or 0
    fire_deg = row.get('fire_deg', 0) or 0
    
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
        df['neden'] = df.apply(lambda r: neden_tespit(r)[0], axis=1)
        df['aksiyon'] = df.apply(lambda r: neden_tespit(r)[1], axis=1)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        pd.DataFrame([{
            'Filtre': filtre_text,
            'Min Ciro': f"₺{min_ciro:,.0f}",
            'Tarih': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
        }]).to_excel(writer, sheet_name='Bilgi', index=False)
        
        if not df.empty:
            df.nsmallest(20, 'adet_deg').to_excel(writer, sheet_name='En Kötü 20', index=False)
            df.nlargest(20, 'adet_deg').to_excel(writer, sheet_name='En İyi 20', index=False)
            df.to_excel(writer, sheet_name='Tüm Veriler', index=False)
    
    output.seek(0)
    return output


# ============================================================================
# MARJ ANALİZİ FONKSİYONLARI (YENİ)
# ============================================================================

def marj_neden_tespit(row: pd.Series) -> list:
    """
    Marj kaybının nedenlerini tespit et
    Mantık:
    1. Satış miktarı düştü mü? → Düştüyse kampanya/indirim kontrolü
    2. Envanter arttı mı?
    3. Fire arttı mı?
    En yüksek 2 nedeni döndür
    """
    
    nedenler = []
    
    # Değerleri al
    adet_2024 = row.get('adet_2024', 0) or 0
    adet_2025 = row.get('adet_2025', 0) or 0
    adet_fark = adet_2025 - adet_2024
    adet_deg = ((adet_2025/adet_2024)-1)*100 if adet_2024 > 0 else 0
    
    envanter_2024 = row.get('envanter_2024', 0) or 0
    envanter_2025 = row.get('envanter_2025', 0) or 0
    envanter_fark = envanter_2025 - envanter_2024
    
    fire_2024 = abs(row.get('fire_2024', 0) or 0)
    fire_2025 = abs(row.get('fire_2025', 0) or 0)
    fire_fark = fire_2025 - fire_2024
    
    kampanya_2024 = abs(row.get('kampanya_2024', 0) or 0)
    kampanya_2025 = abs(row.get('kampanya_2025', 0) or 0)
    kampanya_fark = kampanya_2025 - kampanya_2024
    
    # 1. Satış düşüşü kontrolü
    if adet_fark < 0:
        # Satış düştü - kampanya/indirim kontrolü
        if kampanya_fark > 0:
            nedenler.append({
                'neden': '📉 Satış Düşüşü + Kampanya Artışı',
                'aciklama': f"Satış: {adet_2024:,.0f} → {adet_2025:,.0f} ({adet_deg:+.1f}%)\nKampanya Zararı: ₺{kampanya_2024:,.0f} → ₺{kampanya_2025:,.0f} (+₺{kampanya_fark:,.0f})",
                'oncelik': abs(adet_fark) + kampanya_fark
            })
        else:
            nedenler.append({
                'neden': '📉 Satış Düşüşü',
                'aciklama': f"Satış: {adet_2024:,.0f} → {adet_2025:,.0f} ({adet_deg:+.1f}%)",
                'oncelik': abs(adet_fark)
            })
    
    # 2. Envanter artışı kontrolü
    if envanter_fark > 0:
        envanter_deg = ((envanter_2025/envanter_2024)-1)*100 if envanter_2024 > 0 else 100
        nedenler.append({
            'neden': '📦 Envanter Artışı',
            'aciklama': f"Envanter: ₺{envanter_2024:,.0f} → ₺{envanter_2025:,.0f} (+₺{envanter_fark:,.0f}, {envanter_deg:+.1f}%)",
            'oncelik': envanter_fark
        })
    
    # 3. Fire artışı kontrolü
    if fire_fark > 0:
        fire_deg = ((fire_2025/fire_2024)-1)*100 if fire_2024 > 0 else 100
        nedenler.append({
            'neden': '🔥 Fire Artışı',
            'aciklama': f"Fire: ₺{fire_2024:,.0f} → ₺{fire_2025:,.0f} (+₺{fire_fark:,.0f}, {fire_deg:+.1f}%)",
            'oncelik': fire_fark
        })
    
    # 4. Kampanya zararı (satış düşmese bile)
    if kampanya_fark > 0 and adet_fark >= 0:
        nedenler.append({
            'neden': '🏷️ Kampanya Zararı Artışı',
            'aciklama': f"Kampanya: ₺{kampanya_2024:,.0f} → ₺{kampanya_2025:,.0f} (+₺{kampanya_fark:,.0f})",
            'oncelik': kampanya_fark
        })
    
    # Önceliğe göre sırala ve en yüksek 2'yi döndür
    nedenler.sort(key=lambda x: x['oncelik'], reverse=True)
    
    if not nedenler:
        return [{'neden': '📊 Belirgin neden yok', 'aciklama': 'Detaylı analiz gerekli'}]
    
    return nedenler[:2]


def get_marj_magaza_by_mal_grubu(con, mal_grubu: str, where: str, limit: int = 10) -> pd.DataFrame:
    """Mal grubu için marj bazında en iyi mağazalar"""
    
    mal_kosul = f"Mal_Grubu = '{mal_grubu}'"
    full_where = f"{where} AND {mal_kosul}" if where else f"WHERE {mal_kosul}"
    
    sql = f"""
        SELECT 
            Magaza_Kod,
            MAX(Magaza_Ad) as Magaza_Ad,
            MAX(BS) as BS,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025
        FROM veri
        {full_where}
        GROUP BY Magaza_Kod
        HAVING Marj_2025 > 0
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if df.empty:
        return df
    
    df['marj_fark'] = df['marj_2025'] - df['marj_2024']
    df['marj_deg'] = df.apply(lambda r: ((r['marj_2025']/r['marj_2024'])-1)*100 if r['marj_2024']>0 else 0, axis=1)
    
    return df.nlargest(limit, 'marj_2025')


def get_marj_urun_by_mal_grubu(con, mal_grubu: str, where: str, limit: int = 10) -> pd.DataFrame:
    """Mal grubu için marj bazında en iyi ürünler"""
    
    mal_kosul = f"Mal_Grubu = '{mal_grubu}'"
    full_where = f"{where} AND {mal_kosul}" if where else f"WHERE {mal_kosul}"
    
    sql = f"""
        SELECT 
            Urun_Kod,
            MAX(Urun_Ad) as Urun_Ad,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025
        FROM veri
        {full_where}
        GROUP BY Urun_Kod
        HAVING Marj_2025 > 0
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = [c.lower() for c in df.columns]
    
    if df.empty:
        return df
    
    df['marj_fark'] = df['marj_2025'] - df['marj_2024']
    df['marj_deg'] = df.apply(lambda r: ((r['marj_2025']/r['marj_2024'])-1)*100 if r['marj_2024']>0 else 0, axis=1)
    
    return df.nlargest(limit, 'marj_2025')


def get_marj_mal_grubu(con, where: str, min_ciro: float) -> pd.DataFrame:
    """Mal Grubu bazında marj analizi - genişletilmiş"""
    
    ciro_filtre = f"HAVING SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) >= {min_ciro}" if min_ciro > 0 else ""
    
    if where:
        where_mal = f"{where} AND Mal_Grubu != ''"
    else:
        where_mal = "WHERE Mal_Grubu != ''"
    
    sql = f"""
        SELECT 
            Mal_Grubu,
            MAX(Ust_Mal) as Ust_Mal,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN ABS(Fire) ELSE 0 END) as Fire_2024,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025,
            SUM(CASE WHEN Yil=2024 THEN ABS(Envanter) ELSE 0 END) as Envanter_2024,
            SUM(CASE WHEN Yil=2025 THEN ABS(Envanter) ELSE 0 END) as Envanter_2025,
            SUM(CASE WHEN Yil=2024 THEN ABS(Kampanya_Zarar) ELSE 0 END) as Kampanya_2024,
            SUM(CASE WHEN Yil=2025 THEN ABS(Kampanya_Zarar) ELSE 0 END) as Kampanya_2025
        FROM veri
        {where_mal}
        GROUP BY Mal_Grubu
        {ciro_filtre}
    """
    
    df = con.execute(sql).fetchdf()
    
    if df.empty:
        return df
    
    df.columns = [c.lower() for c in df.columns]
    
    # Marj farkı (tutar olarak)
    df['marj_fark'] = df['marj_2025'] - df['marj_2024']
    
    # Marj değişim %
    df['marj_deg'] = df.apply(lambda r: ((r['marj_2025']/r['marj_2024'])-1)*100 if r['marj_2024']>0 else 0, axis=1)
    
    # Marj oranı
    df['marj_oran_2024'] = df.apply(lambda r: (r['marj_2024']/r['ciro_2024'])*100 if r['ciro_2024']>0 else 0, axis=1)
    df['marj_oran_2025'] = df.apply(lambda r: (r['marj_2025']/r['ciro_2025'])*100 if r['ciro_2025']>0 else 0, axis=1)
    df['marj_oran_fark'] = df['marj_oran_2025'] - df['marj_oran_2024']
    
    return df


def get_marj_malzeme(con, where: str, min_ciro: float) -> pd.DataFrame:
    """Malzeme bazında marj analizi - genişletilmiş"""
    
    ciro_filtre = f"HAVING SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) >= {min_ciro/10}" if min_ciro > 0 else ""
    
    if where:
        where_mal = f"{where} AND Urun_Kod != ''"
    else:
        where_mal = "WHERE Urun_Kod != ''"
    
    sql = f"""
        SELECT 
            Urun_Kod,
            MAX(Urun_Ad) as Urun_Ad,
            MAX(Mal_Grubu) as Mal_Grubu,
            SUM(CASE WHEN Yil=2024 THEN Marj ELSE 0 END) as Marj_2024,
            SUM(CASE WHEN Yil=2025 THEN Marj ELSE 0 END) as Marj_2025,
            SUM(CASE WHEN Yil=2024 THEN Ciro ELSE 0 END) as Ciro_2024,
            SUM(CASE WHEN Yil=2025 THEN Ciro ELSE 0 END) as Ciro_2025,
            SUM(CASE WHEN Yil=2024 THEN Adet ELSE 0 END) as Adet_2024,
            SUM(CASE WHEN Yil=2025 THEN Adet ELSE 0 END) as Adet_2025,
            SUM(CASE WHEN Yil=2024 THEN ABS(Fire) ELSE 0 END) as Fire_2024,
            SUM(CASE WHEN Yil=2025 THEN ABS(Fire) ELSE 0 END) as Fire_2025,
            SUM(CASE WHEN Yil=2024 THEN ABS(Envanter) ELSE 0 END) as Envanter_2024,
            SUM(CASE WHEN Yil=2025 THEN ABS(Envanter) ELSE 0 END) as Envanter_2025,
            SUM(CASE WHEN Yil=2024 THEN ABS(Kampanya_Zarar) ELSE 0 END) as Kampanya_2024,
            SUM(CASE WHEN Yil=2025 THEN ABS(Kampanya_Zarar) ELSE 0 END) as Kampanya_2025
        FROM veri
        {where_mal}
        GROUP BY Urun_Kod
        {ciro_filtre}
    """
    
    df = con.execute(sql).fetchdf()
    
    if df.empty:
        return df
    
    df.columns = [c.lower() for c in df.columns]
    
    # Marj farkı (tutar olarak)
    df['marj_fark'] = df['marj_2025'] - df['marj_2024']
    
    # Marj değişim %
    df['marj_deg'] = df.apply(lambda r: ((r['marj_2025']/r['marj_2024'])-1)*100 if r['marj_2024']>0 else 0, axis=1)
    
    # Marj oranı
    df['marj_oran_2024'] = df.apply(lambda r: (r['marj_2024']/r['ciro_2024'])*100 if r['ciro_2024']>0 else 0, axis=1)
    df['marj_oran_2025'] = df.apply(lambda r: (r['marj_2025']/r['ciro_2025'])*100 if r['ciro_2025']>0 else 0, axis=1)
    
    return df


def marj_kpi_goster(con, where: str):
    """Marj KPI kartları"""
    
    sql = f"""
        SELECT 
            Yil,
            SUM(Marj) as Marj,
            SUM(Ciro) as Ciro
        FROM veri
        {where}
        GROUP BY Yil
    """
    
    df = con.execute(sql).fetchdf()
    df.columns = df.columns.str.lower()
    
    marj_2024 = df[df['yil']==2024]['marj'].sum() if len(df[df['yil']==2024]) > 0 else 0
    marj_2025 = df[df['yil']==2025]['marj'].sum() if len(df[df['yil']==2025]) > 0 else 0
    ciro_2024 = df[df['yil']==2024]['ciro'].sum() if len(df[df['yil']==2024]) > 0 else 0
    ciro_2025 = df[df['yil']==2025]['ciro'].sum() if len(df[df['yil']==2025]) > 0 else 0
    
    marj_fark = marj_2025 - marj_2024
    marj_deg = ((marj_2025/marj_2024)-1)*100 if marj_2024 > 0 else 0
    
    marj_oran_2024 = (marj_2024/ciro_2024)*100 if ciro_2024 > 0 else 0
    marj_oran_2025 = (marj_2025/ciro_2025)*100 if ciro_2025 > 0 else 0
    oran_fark = marj_oran_2025 - marj_oran_2024
    
    cols = st.columns(4)
    
    with cols[0]:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">💰 Marj 2024</div>
            <div class="kpi-value">₺{marj_2024:,.0f}</div>
            <div class="kpi-delta">%{marj_oran_2024:.1f} oran</div>
        </div>
        """, unsafe_allow_html=True)
    
    with cols[1]:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">💰 Marj 2025</div>
            <div class="kpi-value">₺{marj_2025:,.0f}</div>
            <div class="kpi-delta">%{marj_oran_2025:.1f} oran</div>
        </div>
        """, unsafe_allow_html=True)
    
    with cols[2]:
        delta_class = 'delta-up' if marj_fark > 0 else 'delta-down'
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">📊 Marj Farkı</div>
            <div class="kpi-value {delta_class}">₺{marj_fark:+,.0f}</div>
            <div class="kpi-delta {delta_class}">{marj_deg:+.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with cols[3]:
        delta_class = 'delta-up' if oran_fark > 0 else 'delta-down'
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">📈 Oran Değişimi</div>
            <div class="kpi-value {delta_class}">{oran_fark:+.2f}%</div>
            <div class="kpi-delta">{marj_oran_2024:.1f}% → {marj_oran_2025:.1f}%</div>
        </div>
        """, unsafe_allow_html=True)


def marj_liste_goster(df: pd.DataFrame, baslik: str, limit: int = 10, ters: bool = False, prefix: str = ""):
    """Mal grubu marj listesi - neden tespiti ile"""
    
    st.markdown(f'<div class="section-title">{baslik}</div>', unsafe_allow_html=True)
    
    selected_mag = None
    selected_urun = None
    
    if df.empty:
        st.info("Gösterilecek veri yok")
        return None, None
    
    df_sorted = df.nlargest(limit, 'marj_fark') if ters else df.nsmallest(limit, 'marj_fark')
    
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        mal = row['mal_grubu']
        marj_fark = row.get('marj_fark', 0)
        marj_deg = row.get('marj_deg', 0)
        
        renk = "🟢" if marj_fark > 0 else "🔴"
        
        with st.expander(f"{renk} **{mal}** → ₺{marj_fark:+,.0f}"):
            # Marj bilgileri
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Marj 2024", f"₺{row['marj_2024']:,.0f}")
                st.metric("Marj Oranı 2024", f"%{row['marj_oran_2024']:.1f}")
            with col2:
                st.metric("Marj 2025", f"₺{row['marj_2025']:,.0f}", f"{marj_deg:+.1f}%")
                st.metric("Marj Oranı 2025", f"%{row['marj_oran_2025']:.1f}", f"{row['marj_oran_fark']:+.2f}%")
            
            # NEDEN TESPİTİ - sadece kayıp varsa göster
            if marj_fark < 0:
                st.markdown("---")
                st.markdown("**🔍 KAYIP NEDENLERİ:**")
                
                nedenler = marj_neden_tespit(row)
                
                for neden in nedenler:
                    st.markdown(f"""
                    <div class="neden-box">
                        <strong>{neden['neden']}</strong><br>
                        {neden['aciklama'].replace(chr(10), '<br>')}
                    </div>
                    """, unsafe_allow_html=True)
            
            # Butonlar
            st.markdown("---")
            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                if st.button("🏆 En Çok Satan 10 Mağaza", key=f"btn_mag_{prefix}_{i}"):
                    selected_mag = mal
            with btn_col2:
                if st.button("📦 En Çok Satan 10 Ürün", key=f"btn_urun_{prefix}_{i}"):
                    selected_urun = mal
    
    return selected_mag, selected_urun


def marj_malzeme_goster(df: pd.DataFrame, baslik: str, limit: int = 10, ters: bool = False, prefix: str = ""):
    """Malzeme marj listesi - neden tespiti ile"""
    
    st.markdown(f'<div class="section-title">{baslik}</div>', unsafe_allow_html=True)
    
    if df.empty:
        st.info("Gösterilecek veri yok")
        return
    
    df_sorted = df.nlargest(limit, 'marj_fark') if ters else df.nsmallest(limit, 'marj_fark')
    
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        urun = row['urun_ad'][:50] + "..." if len(str(row['urun_ad'])) > 50 else row['urun_ad']
        marj_fark = row.get('marj_fark', 0)
        marj_deg = row.get('marj_deg', 0)
        
        renk = "🟢" if marj_fark > 0 else "🔴"
        
        with st.expander(f"{renk} **{urun}** → ₺{marj_fark:+,.0f}"):
            st.caption(f"Kod: {row['urun_kod']} | Mal Grubu: {row['mal_grubu']}")
            
            # Marj bilgileri
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Marj 2024", f"₺{row['marj_2024']:,.0f}")
                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
            with col2:
                st.metric("Marj 2025", f"₺{row['marj_2025']:,.0f}", f"{marj_deg:+.1f}%")
                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}")
            
            # NEDEN TESPİTİ - sadece kayıp varsa göster
            if marj_fark < 0:
                st.markdown("---")
                st.markdown("**🔍 KAYIP NEDENLERİ:**")
                
                nedenler = marj_neden_tespit(row)
                
                for neden in nedenler:
                    st.markdown(f"""
                    <div class="neden-box">
                        <strong>{neden['neden']}</strong><br>
                        {neden['aciklama'].replace(chr(10), '<br>')}
                    </div>
                    """, unsafe_allow_html=True)


def excel_rapor_marj(con, where: str, min_ciro: float, filtre_text: str) -> BytesIO:
    """Marj Excel raporu"""
    
    output = BytesIO()
    
    df_mal = get_marj_mal_grubu(con, where, min_ciro)
    df_urun = get_marj_malzeme(con, where, min_ciro)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        pd.DataFrame([{
            'Filtre': filtre_text,
            'Min Ciro': f"₺{min_ciro:,.0f}",
            'Tarih': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M'),
            'Rapor': 'Net Marj Analizi'
        }]).to_excel(writer, sheet_name='Bilgi', index=False)
        
        if not df_mal.empty:
            df_mal.nsmallest(20, 'marj_fark').to_excel(writer, sheet_name='Mal Grubu - Kayıp', index=False)
            df_mal.nlargest(20, 'marj_fark').to_excel(writer, sheet_name='Mal Grubu - Kazanç', index=False)
            df_mal.to_excel(writer, sheet_name='Mal Grubu - Tümü', index=False)
        
        if not df_urun.empty:
            df_urun.nsmallest(20, 'marj_fark').to_excel(writer, sheet_name='Malzeme - Kayıp', index=False)
            df_urun.nlargest(20, 'marj_fark').to_excel(writer, sheet_name='Malzeme - Kazanç', index=False)
    
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
        return None, None, None
    
    df_sorted = df.nlargest(limit, 'adet_deg') if ters else df.nsmallest(limit, 'adet_deg')
    
    prefix = "iyi" if ters else "kotu"
    selected_urun = None
    selected_mag_dusus = None
    selected_mag_artis = None
    
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        mal = row['mal_grubu']
        adet_deg = row.get('adet_deg', 0)
        neden, aksiyon, renk = neden_tespit(row)
        
        with st.expander(f"**{mal}** → Adet: {adet_deg:+.1f}%"):
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
            with col2:
                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{adet_deg:+.1f}%")
                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row.get('ciro_deg',0):+.1f}%")
            
            st.markdown(f'<div class="neden-box"><strong>Neden:</strong> {neden}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="aksiyon-box">💡 <strong>Aksiyon:</strong> {aksiyon}</div>', unsafe_allow_html=True)
            
            # Her iki liste için de 3 buton
            btn_col1, btn_col2, btn_col3 = st.columns(3)
            with btn_col1:
                if st.button("📋 Ürünler", key=f"btn_urun_{prefix}_{i}"):
                    selected_urun = mal
            with btn_col2:
                if st.button("🟢 Yükselen", key=f"btn_artis_{prefix}_{i}"):
                    selected_mag_artis = mal
            with btn_col3:
                if st.button("🔴 Düşen", key=f"btn_dusus_{prefix}_{i}"):
                    selected_mag_dusus = mal
    
    return selected_urun, selected_mag_dusus, selected_mag_artis


# ============================================================================
# MAIN
# ============================================================================

def main():
    st.markdown('<h1 class="main-title">🎯 Satış Karar Sistemi</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-title">Kasım 2024 → 2025 | 3 dakikada teşhis, neden, aksiyon</p>', unsafe_allow_html=True)
    
    # Veri yükle (Parquet'ten - çok hızlı)
    veri = veri_yukle()
    
    if not veri.get('loaded'):
        st.error(f"❌ Veri yüklenemedi: {veri.get('error', 'Bilinmeyen hata')}")
        st.markdown("""
        ### 📁 Parquet Dosyaları Bulunamadı
        
        Bu uygulama `veri_2024.parquet` ve `veri_2025.parquet` dosyalarını okur.
        
        **Çözüm:**
        1. `donusturucu.py` scriptini çalıştır
        2. Oluşan `.parquet` dosyalarını bu repo'ya yükle
        3. Sayfayı yenile
        """)
        return
    
    st.markdown('<div class="success-box">✅ Veri yüklendi: {:,} satır (2024: {:,} | 2025: {:,})</div>'.format(
        veri['sayilar']['2024'] + veri['sayilar']['2025'],
        veri['sayilar']['2024'],
        veri['sayilar']['2025']
    ), unsafe_allow_html=True)
    
    # Filtreler
    secili = sidebar_filtreler(veri['filtreler'])
    where = build_where(secili)
    filtre = filtre_text(secili)
    
    # DuckDB
    con = duckdb.connect()
    con.register('veri', veri['df'])
    
    # Filtre bilgisi
    st.markdown(f'<div class="filter-badge">📍 {filtre} | Min: ₺{secili["min_ciro"]:,}</div>', unsafe_allow_html=True)
    
    # SEKMELER
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📦 Satış Analizi", "📂 Ürün Grubu Analizi", "🏷️ Ürün Analizi", "🔢 En Çok/Az Satan", "💰 Net Marj Analizi"])
    
    # =========================================================================
    # TAB 1: SATIŞ ANALİZİ (mevcut)
    # =========================================================================
    with tab1:
        # Excel rapor
        excel = excel_rapor(con, where, secili['min_ciro'], filtre)
        st.download_button("📥 EXCEL RAPORU", excel, f"rapor_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx", key="excel_satis")
        
        st.markdown("---")
        
        # KPI'lar
        ozet = get_ozet(con, where)
        kpi_goster(ozet)
        
        st.markdown("---")
        
        # DETAY PLACEHOLDER - Üstte gösterilecek
        detay_placeholder = st.container()
        
        st.markdown("---")
        
        # Analiz
        df_analiz = get_mal_grubu_analiz(con, where, secili['min_ciro'])
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_urun1, selected_mag_dusus1, selected_mag_artis1 = karar_goster(df_analiz, "🔴 EN KÖTÜ 10", limit=10, ters=False)
        
        with col2:
            selected_urun2, selected_mag_dusus2, selected_mag_artis2 = karar_goster(df_analiz, "🟢 EN İYİ 10", limit=10, ters=True)
        
        # DETAYLARI ÜSTTE GÖSTER
        with detay_placeholder:
            # Ürün detay
            selected_urun = selected_urun1 or selected_urun2
            if selected_urun:
                st.markdown(f'<div class="detay-baslik">📋 {selected_urun} - Ürün Detayları</div>', unsafe_allow_html=True)
                df_urun = get_urun_detay(con, selected_urun, where)
                if not df_urun.empty:
                    st.dataframe(df_urun, use_container_width=True, hide_index=True)
            
            # Düşen mağazalar
            selected_mag_dusus = selected_mag_dusus1 or selected_mag_dusus2
            if selected_mag_dusus:
                st.markdown(f'<div class="detay-baslik">🔴 {selected_mag_dusus} - En Çok Düşen 5 Mağaza</div>', unsafe_allow_html=True)
                df_mag = get_magaza_dusus(con, selected_mag_dusus, where, limit=5)
                if not df_mag.empty:
                    for i, (idx, row) in enumerate(df_mag.iterrows()):
                        mag_ad = row['magaza_ad']
                        adet_fark = row['adet_fark']
                        adet_deg = row['adet_deg']
                        
                        with st.expander(f"🔴 **{row['magaza_kod']}** - {mag_ad} → {adet_fark:+,.0f} adet ({adet_deg:+.1f}%)"):
                            st.caption(f"BS: {row['bs']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                            with c2:
                                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{adet_deg:+.1f}%")
                                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row['ciro_deg']:+.1f}%")
                            st.metric("Fire 2025", f"₺{row['fire_2025']:,.0f}")
                else:
                    st.info("Bu mal grubu için mağaza verisi bulunamadı")
            
            # Yükselen mağazalar
            selected_mag_artis = selected_mag_artis1 or selected_mag_artis2
            if selected_mag_artis:
                st.markdown(f'<div class="detay-baslik">🟢 {selected_mag_artis} - En Çok Yükselen 5 Mağaza</div>', unsafe_allow_html=True)
                df_mag_artis = get_magaza_artis(con, selected_mag_artis, where, limit=5)
                if not df_mag_artis.empty:
                    for i, (idx, row) in enumerate(df_mag_artis.iterrows()):
                        mag_ad = row['magaza_ad']
                        adet_fark = row['adet_fark']
                        adet_deg = row['adet_deg']
                        
                        with st.expander(f"🟢 **{row['magaza_kod']}** - {mag_ad} → {adet_fark:+,.0f} adet ({adet_deg:+.1f}%)"):
                            st.caption(f"BS: {row['bs']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                            with c2:
                                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{adet_deg:+.1f}%")
                                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row['ciro_deg']:+.1f}%")
                else:
                    st.info("Bu mal grubu için mağaza verisi bulunamadı")
    
    # =========================================================================
    # TAB 2: ÜRÜN GRUBU ANALİZİ (yeni)
    # =========================================================================
    with tab2:
        # Excel rapor
        excel_ug = excel_rapor_ug(con, where, secili['min_ciro'], filtre)
        st.download_button("📥 ÜRÜN GRUBU RAPORU", excel_ug, f"urun_grubu_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx", key="excel_ug")
        
        st.markdown("---")
        
        # KPI'lar
        ozet_ug = get_ozet(con, where)
        kpi_goster(ozet_ug)
        
        st.markdown("---")
        
        # DETAY PLACEHOLDER
        detay_ug_placeholder = st.container()
        
        st.markdown("---")
        
        # Ürün Grubu Analizi
        df_ug_analiz = get_urun_grubu_analiz(con, where, secili['min_ciro'])
        
        col1, col2 = st.columns(2)
        
        with col1:
            ug_mal1, ug_dusus1, ug_artis1 = karar_goster_ug(df_ug_analiz, "🔴 EN KÖTÜ 10 ÜRÜN GRUBU", limit=10, ters=False)
        
        with col2:
            ug_mal2, ug_dusus2, ug_artis2 = karar_goster_ug(df_ug_analiz, "🟢 EN İYİ 10 ÜRÜN GRUBU", limit=10, ters=True)
        
        # DETAYLARI ÜSTTE GÖSTER
        with detay_ug_placeholder:
            selected_mal = ug_mal1 or ug_mal2
            if selected_mal:
                st.markdown(f'<div class="detay-baslik">📂 {selected_mal} - Mal Grupları</div>', unsafe_allow_html=True)
                df_mal = get_mal_grubu_by_urun_grubu(con, selected_mal, where)
                if not df_mal.empty:
                    st.dataframe(df_mal, use_container_width=True, hide_index=True)
            
            ug_mag_dusus = ug_dusus1 or ug_dusus2
            if ug_mag_dusus:
                st.markdown(f'<div class="detay-baslik">🔴 {ug_mag_dusus} - En Çok Düşen 5 Mağaza</div>', unsafe_allow_html=True)
                df_mag = get_magaza_dusus_ug(con, ug_mag_dusus, where, limit=5)
                if not df_mag.empty:
                    for i, (idx, row) in enumerate(df_mag.iterrows()):
                        with st.expander(f"🔴 **{row['magaza_kod']}** - {row['magaza_ad']} → {row['adet_fark']:+,.0f} adet ({row['adet_deg']:+.1f}%)"):
                            st.caption(f"BS: {row['bs']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                            with c2:
                                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{row['adet_deg']:+.1f}%")
                                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row['ciro_deg']:+.1f}%")
                            st.metric("Fire 2025", f"₺{row['fire_2025']:,.0f}")
            
            ug_mag_artis = ug_artis1 or ug_artis2
            if ug_mag_artis:
                st.markdown(f'<div class="detay-baslik">🟢 {ug_mag_artis} - En Çok Yükselen 5 Mağaza</div>', unsafe_allow_html=True)
                df_mag_artis = get_magaza_artis_ug(con, ug_mag_artis, where, limit=5)
                if not df_mag_artis.empty:
                    for i, (idx, row) in enumerate(df_mag_artis.iterrows()):
                        with st.expander(f"🟢 **{row['magaza_kod']}** - {row['magaza_ad']} → {row['adet_fark']:+,.0f} adet ({row['adet_deg']:+.1f}%)"):
                            st.caption(f"BS: {row['bs']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                            with c2:
                                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{row['adet_deg']:+.1f}%")
                                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row['ciro_deg']:+.1f}%")
    
    # =========================================================================
    # TAB 3: ÜRÜN ANALİZİ
    # =========================================================================
    with tab3:
        excel_urun = excel_rapor_urun(con, where, secili['min_ciro'], filtre)
        st.download_button("📥 ÜRÜN RAPORU", excel_urun, f"urun_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx", key="excel_urun")
        
        st.markdown("---")
        ozet_urun = get_ozet(con, where)
        kpi_goster(ozet_urun)
        st.markdown("---")
        
        # DETAY PLACEHOLDER
        detay_urun_placeholder = st.container()
        st.markdown("---")
        
        df_urun_analiz = get_urun_analiz(con, where, secili['min_ciro'])
        
        col1, col2 = st.columns(2)
        with col1:
            urun_dusus1, urun_artis1 = karar_goster_urun(df_urun_analiz, "🔴 EN KÖTÜ 20 ÜRÜN", limit=20, ters=False)
        with col2:
            urun_dusus2, urun_artis2 = karar_goster_urun(df_urun_analiz, "🟢 EN İYİ 20 ÜRÜN", limit=20, ters=True)
        
        # DETAYLARI ÜSTTE GÖSTER
        with detay_urun_placeholder:
            urun_mag_dusus = urun_dusus1 or urun_dusus2
            if urun_mag_dusus:
                urun_row = df_urun_analiz[df_urun_analiz['urun_kod'] == urun_mag_dusus]
                urun_ad = urun_row['urun_ad'].values[0] if not urun_row.empty else urun_mag_dusus
                st.markdown(f'<div class="detay-baslik">🔴 {urun_ad[:30]}... - En Çok Düşen 5 Mağaza</div>', unsafe_allow_html=True)
                df_mag = get_magaza_dusus_urun(con, urun_mag_dusus, where, limit=5)
                if not df_mag.empty:
                    for i, (idx, row) in enumerate(df_mag.iterrows()):
                        with st.expander(f"🔴 **{row['magaza_kod']}** - {row['magaza_ad']} → {row['adet_fark']:+,.0f} adet ({row['adet_deg']:+.1f}%)"):
                            st.caption(f"BS: {row['bs']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                            with c2:
                                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{row['adet_deg']:+.1f}%")
                                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row['ciro_deg']:+.1f}%")
                            st.metric("Fire 2025", f"₺{row['fire_2025']:,.0f}")
            
            urun_mag_artis = urun_artis1 or urun_artis2
            if urun_mag_artis:
                urun_row = df_urun_analiz[df_urun_analiz['urun_kod'] == urun_mag_artis]
                urun_ad = urun_row['urun_ad'].values[0] if not urun_row.empty else urun_mag_artis
                st.markdown(f'<div class="detay-baslik">🟢 {urun_ad[:30]}... - En Çok Yükselen 5 Mağaza</div>', unsafe_allow_html=True)
                df_mag_artis = get_magaza_artis_urun(con, urun_mag_artis, where, limit=5)
                if not df_mag_artis.empty:
                    for i, (idx, row) in enumerate(df_mag_artis.iterrows()):
                        with st.expander(f"🟢 **{row['magaza_kod']}** - {row['magaza_ad']} → {row['adet_fark']:+,.0f} adet ({row['adet_deg']:+.1f}%)"):
                            st.caption(f"BS: {row['bs']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                            with c2:
                                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{row['adet_deg']:+.1f}%")
                                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}", f"{row['ciro_deg']:+.1f}%")
    
    # =========================================================================
    # TAB 4: EN ÇOK / EN AZ SATAN
    # =========================================================================
    with tab4:
        excel_adet = excel_rapor_adet(con, where, filtre)
        st.download_button("📥 EN ÇOK/AZ SATAN RAPORU", excel_adet, f"en_cok_az_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx", key="excel_adet")
        st.markdown("---")
        ozet_adet = get_ozet(con, where)
        kpi_goster(ozet_adet)
        st.markdown("---")
        
        # DETAY PLACEHOLDER
        detay_adet_placeholder = st.container()
        st.markdown("---")
        
        df_adet_analiz = get_urun_adet_sirali(con, where, 0)
        
        col1, col2 = st.columns(2)
        with col1:
            adet_cok1, adet_az1 = karar_goster_adet(df_adet_analiz, "🏆 EN ÇOK SATAN 20 ÜRÜN (2025)", limit=20, en_cok=True)
        with col2:
            adet_cok2, adet_az2 = karar_goster_adet(df_adet_analiz, "📉 EN AZ SATAN 20 ÜRÜN (2025)", limit=20, en_cok=False)
        
        # DETAYLARI ÜSTTE GÖSTER
        with detay_adet_placeholder:
            adet_mag_cok = adet_cok1 or adet_cok2
            if adet_mag_cok:
                urun_row = df_adet_analiz[df_adet_analiz['urun_kod'] == adet_mag_cok]
                urun_ad = urun_row['urun_ad'].values[0][:30] if not urun_row.empty else adet_mag_cok
                st.markdown(f'<div class="detay-baslik">🏆 {urun_ad}... - En Çok Satan 10 Mağaza</div>', unsafe_allow_html=True)
                df_mag = get_magaza_adet_sirali(con, adet_mag_cok, where)
                if not df_mag.empty:
                    df_mag_top = df_mag.nlargest(10, 'adet_2025')
                    for i, (idx, row) in enumerate(df_mag_top.iterrows()):
                        deg_renk = "🟢" if row['adet_deg'] > 0 else "🔴" if row['adet_deg'] < 0 else "⚪"
                        with st.expander(f"🏆 **{row['magaza_kod']}** - {row['magaza_ad']} → {row['adet_2025']:,.0f} adet ({deg_renk} {row['adet_deg']:+.1f}%)"):
                            st.caption(f"BS: {row['bs']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                            with c2:
                                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{row['adet_deg']:+.1f}%")
                                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}")
            
            adet_mag_az = adet_az1 or adet_az2
            if adet_mag_az:
                urun_row = df_adet_analiz[df_adet_analiz['urun_kod'] == adet_mag_az]
                urun_ad = urun_row['urun_ad'].values[0][:30] if not urun_row.empty else adet_mag_az
                st.markdown(f'<div class="detay-baslik">📉 {urun_ad}... - En Az Satan 10 Mağaza</div>', unsafe_allow_html=True)
                df_mag = get_magaza_adet_sirali(con, adet_mag_az, where)
                if not df_mag.empty:
                    df_mag_bottom = df_mag[df_mag['adet_2025'] > 0].nsmallest(10, 'adet_2025')
                    for i, (idx, row) in enumerate(df_mag_bottom.iterrows()):
                        deg_renk = "🟢" if row['adet_deg'] > 0 else "🔴" if row['adet_deg'] < 0 else "⚪"
                        with st.expander(f"📉 **{row['magaza_kod']}** - {row['magaza_ad']} → {row['adet_2025']:,.0f} adet ({deg_renk} {row['adet_deg']:+.1f}%)"):
                            st.caption(f"BS: {row['bs']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                            with c2:
                                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}", f"{row['adet_deg']:+.1f}%")
                                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}")
    
    # =========================================================================
    # TAB 5: NET MARJ ANALİZİ
    # =========================================================================
    with tab5:
        excel_marj = excel_rapor_marj(con, where, secili['min_ciro'], filtre)
        st.download_button("📥 MARJ RAPORU", excel_marj, f"marj_rapor_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx", key="excel_marj")
        st.markdown("---")
        marj_kpi_goster(con, where)
        st.markdown("---")
        
        # DETAY PLACEHOLDER
        detay_marj_placeholder = st.container()
        st.markdown("---")
        
        # Mal Grubu bazında marj analizi
        st.markdown('<div class="section-title">📊 MAL GRUBU BAZINDA MARJ ANALİZİ</div>', unsafe_allow_html=True)
        
        df_marj_mal = get_marj_mal_grubu(con, where, secili['min_ciro'])
        
        col1, col2 = st.columns(2)
        with col1:
            marj_mag1, marj_urun1 = marj_liste_goster(df_marj_mal, "🔴 EN ÇOK MARJ KAYBI (Mal Grubu)", limit=10, ters=False, prefix="mal_kotu")
        with col2:
            marj_mag2, marj_urun2 = marj_liste_goster(df_marj_mal, "🟢 EN ÇOK MARJ ARTIŞI (Mal Grubu)", limit=10, ters=True, prefix="mal_iyi")
        
        st.markdown("---")
        
        # Malzeme bazında marj analizi
        st.markdown('<div class="section-title">📦 MALZEME BAZINDA MARJ ANALİZİ</div>', unsafe_allow_html=True)
        
        df_marj_urun = get_marj_malzeme(con, where, secili['min_ciro'])
        
        col1, col2 = st.columns(2)
        with col1:
            marj_malzeme_goster(df_marj_urun, "🔴 EN ÇOK MARJ KAYBI (Malzeme)", limit=10, ters=False, prefix="urun_kotu")
        with col2:
            marj_malzeme_goster(df_marj_urun, "🟢 EN ÇOK MARJ ARTIŞI (Malzeme)", limit=10, ters=True, prefix="urun_iyi")
        
        # DETAYLARI ÜSTTE GÖSTER
        with detay_marj_placeholder:
            # En çok satan mağazalar
            selected_marj_mag = marj_mag1 or marj_mag2
            if selected_marj_mag:
                st.markdown(f'<div class="detay-baslik">🏆 {selected_marj_mag} - En Çok Marj Yapan 10 Mağaza</div>', unsafe_allow_html=True)
                df_mag = get_marj_magaza_by_mal_grubu(con, selected_marj_mag, where, limit=10)
                if not df_mag.empty:
                    for i, (idx, row) in enumerate(df_mag.iterrows()):
                        deg_renk = "🟢" if row['marj_deg'] > 0 else "🔴" if row['marj_deg'] < 0 else "⚪"
                        with st.expander(f"🏆 **{row['magaza_kod']}** - {row['magaza_ad']} → ₺{row['marj_2025']:,.0f} ({deg_renk} {row['marj_deg']:+.1f}%)"):
                            st.caption(f"BS: {row['bs']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Marj 2024", f"₺{row['marj_2024']:,.0f}")
                                st.metric("Ciro 2024", f"₺{row['ciro_2024']:,.0f}")
                            with c2:
                                st.metric("Marj 2025", f"₺{row['marj_2025']:,.0f}", f"{row['marj_deg']:+.1f}%")
                                st.metric("Ciro 2025", f"₺{row['ciro_2025']:,.0f}")
            
            # En çok satan ürünler
            selected_marj_urun = marj_urun1 or marj_urun2
            if selected_marj_urun:
                st.markdown(f'<div class="detay-baslik">📦 {selected_marj_urun} - En Çok Marj Yapan 10 Ürün</div>', unsafe_allow_html=True)
                df_urun = get_marj_urun_by_mal_grubu(con, selected_marj_urun, where, limit=10)
                if not df_urun.empty:
                    for i, (idx, row) in enumerate(df_urun.iterrows()):
                        urun_ad = row['urun_ad'][:35] + "..." if len(str(row['urun_ad'])) > 35 else row['urun_ad']
                        deg_renk = "🟢" if row['marj_deg'] > 0 else "🔴" if row['marj_deg'] < 0 else "⚪"
                        with st.expander(f"📦 **{urun_ad}** → ₺{row['marj_2025']:,.0f} ({deg_renk} {row['marj_deg']:+.1f}%)"):
                            st.caption(f"Kod: {row['urun_kod']}")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.metric("Marj 2024", f"₺{row['marj_2024']:,.0f}")
                                st.metric("Adet 2024", f"{row['adet_2024']:,.0f}")
                            with c2:
                                st.metric("Marj 2025", f"₺{row['marj_2025']:,.0f}", f"{row['marj_deg']:+.1f}%")
                                st.metric("Adet 2025", f"{row['adet_2025']:,.0f}")
    
    # Footer
    st.markdown("---")
    st.caption(f"📊 2024: {veri['sayilar']['2024']:,} | 2025: {veri['sayilar']['2025']:,} | ⚡ Parquet ile süper hızlı")
    
    con.close()


if __name__ == "__main__":
    main()
