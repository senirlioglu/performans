# 📊 Performans Analizi v4

**Basit, Net, Kullanışlı**

## Ana Özellikler

- **Ana Metrik**: Satış Miktarı
- **Nitelik Filtresi**: Spot, Grup Spot, Regule, Kasa Aktivitesi, Bölgesel
- **En Kötü/İyi 10**: Mal Grubu bazlı
- **Ürün Detay**: Her mal grubunun ürünleri
- **Excel Rapor**: Seçilen filtreye göre

## Kurulum

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Filtre Hiyerarşisi

**Organizasyon:**
SM → BS → Mağaza

**Ürün:**
Nitelik → Ürün Grubu → Üst Mal Grubu → Mal Grubu
