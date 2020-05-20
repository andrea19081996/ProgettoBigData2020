DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS historical_stocks;

DROP TABLE IF EXISTS historical_join_stock;

DROP TABLE IF EXISTS result;

CREATE TABLE historical_stock_prices (ticker STRING, open DOUBLE, close DOUBLE, ads_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE historical_stocks(ticker STRING, `exchange` STRING, name STRING, sector STRING, industry STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/home/andrea/Scaricati/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;

LOAD DATA LOCAL INPATH '/home/andrea/Scaricati/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;

CREATE TEMPORARY TABLE historical_join_stock AS
SELECT st.sector, pr.ticker, pr.close, pr.volume, pr.data
FROM (SELECT ticker,close,volume,data
FROM historical_stock_prices
WHERE YEAR(data)<2019 AND YEAR(data)>2007) pr JOIN historical_stocks st ON pr.ticker=st.ticker;

CREATE TABLE result AS
SELECT st2.sector, tmp2.anno, st2.ticker, ROUND(AVG(tmp2.somma_volume),2) AS volume_medio, ROUND(SUM(tmp2.somma_close)/SUM(tmp2.contatore),2) AS quotazione_giornaliera_media, ROUND(AVG((tmp2.close_max-st2.close)/st2.close*100),2) AS variazione_annuale_media
FROM historical_join_stock st2 JOIN
(SELECT st.sector, tmp.anno, st.ticker, st.close AS close_max, tmp.data_minima, tmp.somma_volume, tmp.somma_close, tmp.contatore
FROM historical_join_stock st JOIN(SELECT sector, YEAR(data) AS anno, ticker, MAX(data) AS data_massima, MIN(data) AS data_minima, SUM(volume) AS somma_volume, SUM(close) AS somma_close, COUNT(*) AS contatore
FROM historical_join_stock
GROUP BY sector, YEAR(data), ticker)tmp ON st.sector=tmp.sector AND YEAR(st.data)=tmp.anno AND st.data=tmp.data_massima) tmp2 ON st2.sector=tmp2.sector AND YEAR(st2.data)=tmp2.anno AND st2.data=tmp2.data_minima
GROUP BY st2.sector, tmp2.anno, st2.ticker;
