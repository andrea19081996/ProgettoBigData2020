DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS historical_stocks;
DROP TABLE IF EXISTS historical_join;
DROP TABLE IF EXISTS result;

CREATE TABLE historical_stock_prices
(ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, day DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
LOAD DATA LOCAL INPATH '/home/andrea/Scaricati/daily-historical-stock-prices-1970-2018/historical_stock_pricesX3.csv' OVERWRITE INTO TABLE historical_stock_prices;

CREATE TABLE historical_stocks
(ticker STRING, ex STRING, name STRING, sector STRING, industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
LOAD DATA LOCAL INPATH '/home/andrea/Scaricati/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;

CREATE TEMPORARY TABLE historical_join AS
SELECT st.ticker, st.sector, pri.close, pri.volume, pri.day
FROM historical_stocks st JOIN (SELECT ticker, close, volume, day
FROM historical_stock_prices
WHERE YEAR(day)>2007 AND YEAR(day)<2019) pri ON st.ticker = pri.ticker;

CREATE TABLE result AS
SELECT fj.sector, fj.year, ROUND(AVG(fj.volume),2) AS volume_medio, ROUND(AVG(((hj3.close - fj.close) / fj.close) * 100), 2) AS variazione_annuale_media, ROUND(SUM(fj.close)/SUM(fj.cont), 2) AS quotazione_giornaliera_media
FROM historical_join hj3 JOIN (SELECT fa.ticker, fa.sector, fa.year, fa.volume, hj2.close, fa.max_day, fa.close, fa.cont FROM
(SELECT hj.ticker AS ticker, hj.sector AS sector, YEAR(hj.day) AS year,  MIN(hj.day) AS min_day, MAX(hj.day) AS max_day, SUM(hj.volume) as volume, SUM(hj.close) AS close, COUNT(*) AS cont
FROM historical_join hj
GROUP BY hj.ticker, hj.sector, YEAR(hj.day) ) fa
JOIN historical_join hj2 ON hj2.ticker=fa.ticker AND hj2.sector=fa.sector AND YEAR(hj2.day)=fa.year AND fa.min_day=hj2.day) fj ON hj3.ticker=fj.ticker AND hj3.sector=fj.sector AND YEAR(hj3.day)=fj.year AND hj3.day=fj.max_day
GROUP BY fj.sector, fj.year;
