# 🩺 Performans Röntgeni v3

**DuckDB + Parquet ile Ultra Hızlı Versiyon**

120MB+ Excel dosyaları için optimize edildi.

## 🚀 v3 Farkı

- **DuckDB**: SQL-tabanlı analiz, RAM kullanmadan
- **Parquet**: Excel'den 10x hızlı okuma
- **Streaming**: Büyük dosyalar için optimize

## Kurulum

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Nasıl Çalışır?

1. Excel yüklenir
2. Otomatik Parquet'e dönüştürülür (temp)
3. DuckDB ile SQL sorguları çalışır
4. Sonuçlar gösterilir

## Gereksinimler

- streamlit
- pandas
- duckdb
- pyarrow
- openpyxl
