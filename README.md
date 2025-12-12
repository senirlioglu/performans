# 🩺 Bölgesel Performans Röntgeni v2

**Sorun Bulucu / Müdahale Haritası**

Rapor değil, TEŞHİS aracı. Tıpkı doktorun MR'a bakıp "sorun burada" demesi gibi.

## 🎯 Özellikler

### Incident Scoring (0-100 Puan)
Her sorunlu alan skorlanır:
- Ciro Payı Düşüşü: %35 ağırlık
- Marj Erimesi: %25 ağırlık
- Fire Artışı: %20 ağırlık
- Envanter Artışı: %20 ağırlık

### Otomatik Neden Tespiti
Sistem sorunun kaynağını tespit eder:
- 🔴 Kampanya Zararı
- 🔴 Fire Patlaması
- 🔴 Stok/Envanter Problemi
- 🔴 Trafik/Talep Düşüşü
- 🔴 Marj Erimesi

### Aksiyon Önerileri
Her sorun için operasyonel öneri verir.

### Minimum Baz Filtresi
2024'te 10.000 TL altı satışı olan alanları incident listesine almaz.
Böylece %500 artan 500 TL'lik mağaza seni yanıltmaz.

### 6 Sekmeli Excel Rapor
1. Müdahale Haritası (skorlu)
2. Marj Sızıntısı
3. Gelişen Alanlar
4. Tüm Incidents
5. Top 50 Ürün
6. SM/BS Özet

## 🚀 Kullanım

```bash
pip install -r requirements.txt
streamlit run app.py
```

## ☁️ Streamlit Cloud

1. GitHub'a yükle
2. share.streamlit.io'dan deploy et
3. URL al

## 📊 Veri Formatı

Excel dosyasında olması gereken kolonlar:
- SM, BS, YIL
- Mağaza - Anahtar, Mağaza - Orta uzunl.metin
- Ürün Grubu, Malzeme Nitelik, Üst Mal Grubu, Mal Grubu
- Malzeme Kodu, Malzeme Tanımı
- Satış Miktarı, Satış Hasılatı (VD), Net Marj
- Fire Tutarı, Envanter Tutarı, Toplam Kampanya Zararı

---
**A101 Bölge Yönetimi için geliştirildi**
