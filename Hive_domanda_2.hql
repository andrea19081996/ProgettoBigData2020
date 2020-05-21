DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS historical_stocks;

DROP TABLE IF EXISTS historical_join_stock;
DROP TABLE IF EXISTS result;

CREATE TABLE historical_stock_prices (ticker STRING, open DOUBLE, close DOUBLE, ads_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE historical_stocks(ticker STRING, `exchange` STRING, name STRING, sector STRING, industry STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/home/davide/Documenti/ProgettoBigData2020/dataset/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;

LOAD DATA LOCAL INPATH '/home/davide/Documenti/ProgettoBigData2020/dataset/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;

CREATE TEMPORARY TABLE historical_join_stock AS
SELECT st.sector, year(from_unixtime(unix_timestamp(pr.data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS anno, pr.ticker, pr.close, pr.volume, pr.data
FROM historical_stock_prices pr JOIN historical_stocks st ON pr.ticker=st.ticker
WHERE year(from_unixtime(unix_timestamp(pr.data,'yyyy-MM-dd'),'yyyy-MM-dd'))<2019 AND year(from_unixtime(unix_timestamp(pr.data,'yyyy-MM-dd'),'yyyy-MM-dd'))>2007;

CREATE TABLE result AS
SELECT c.sector, c.anno, ROUND(AVG(c.somma_volume),2) AS volume_medio, ROUND(AVG((c.close_max-c.close_min)/c.close_min*100),2) AS variazione_annuale_media, ROUND(SUM(c.somma_close)/SUM(c.contatore),2) AS quotazione_giornaliera_media
FROM (SELECT st2.sector, st2.anno, st2.ticker, tmp2.close_max, st2.close AS close_min, tmp2.somma_volume, tmp2.somma_close, tmp2.contatore
FROM historical_join_stock st2 JOIN
(SELECT st.sector, st.anno, st.ticker, st.close AS close_max, tmp.data_minima, tmp.somma_volume, tmp.somma_close, tmp.contatore
FROM historical_join_stock st JOIN(SELECT sector, anno, ticker, MAX(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS data_massima, MIN(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS data_minima, SUM(volume) AS somma_volume, SUM(close) AS somma_close, COUNT(*) AS contatore
FROM historical_join_stock
GROUP BY sector, anno, ticker)tmp ON st.sector=tmp.sector AND st.anno=tmp.anno AND st.data=tmp.data_massima) tmp2 ON st2.sector=tmp2.sector AND st2.anno=tmp2.anno AND st2.data=tmp2.data_minima) c
GROUP BY c.sector, c.anno;


