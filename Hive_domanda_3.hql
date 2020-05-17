DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS historical_stocks;
DROP TABLE IF EXISTS historical_join_stock;
DROP TABLE IF EXISTS historical_aggregate;

DROP TABLE IF EXISTS all_in_one;
DROP TABLE IF EXISTS result;

CREATE TABLE historical_stock_prices (ticker STRING, open DOUBLE, close DOUBLE, ads_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE historical_stocks(ticker STRING, `exchange` STRING, name STRING, sector STRING, industry STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/home/andrea/Scaricati/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;

LOAD DATA LOCAL INPATH '/home/andrea/Scaricati/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;

CREATE TEMPORARY TABLE historical_join_stock AS
SELECT st.name, year(from_unixtime(unix_timestamp(pr.data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS anno, pr.ticker, pr.close, pr.data
FROM historical_stock_prices pr JOIN historical_stocks st ON pr.ticker=st.ticker
WHERE year(from_unixtime(unix_timestamp(pr.data,'yyyy-MM-dd'),'yyyy-MM-dd'))<2019 AND year(from_unixtime(unix_timestamp(pr.data,'yyyy-MM-dd'),'yyyy-MM-dd'))>2015;

CREATE TEMPORARY TABLE historical_aggregate AS
SELECT h.name, h.anno, ((t_max.close-t_min.close)/t_min.close*100) AS variazione_annuale
FROM ((SELECT distinct name, anno, ticker
FROM historical_join_stock) h JOIN (SELECT st.name, st.anno, st.ticker, st.close
FROM historical_join_stock st JOIN( SELECT name, anno, ticker, MIN(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS data_minima
FROM historical_join_stock
GROUP BY name, anno, ticker)tmp ON st.name=tmp.name AND st.anno=tmp.anno AND st.data=tmp.data_minima) t_min ON h.name=t_min.name AND h.anno=t_min.anno AND h.ticker=t_min.ticker) JOIN (SELECT st.name, st.anno, st.ticker, st.close
FROM historical_join_stock st JOIN( SELECT name, anno, ticker, MAX(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS data_massima
FROM historical_join_stock
GROUP BY name, anno, ticker)tmp ON st.name=tmp.name AND st.anno=tmp.anno AND st.data=tmp.data_massima) t_max ON h.name=t_max.name AND h.anno=t_max.anno AND h.ticker=t_max.ticker;

CREATE TEMPORARY TABLE all_in_one AS
SELECT t.name, concat_ws('//', collect_set(t.variazione_annuale)) AS variazioni_annuali
FROM (SELECT name, anno, CAST((ROUND((AVG(variazione_annuale)),0)) AS VARCHAR(40)) AS variazione_annuale
FROM historical_aggregate
GROUP BY name, anno) t
GROUP BY t.name;

CREATE TABLE result AS
SELECT temp.nomi_aziende, temp.variazioni_annuali
FROM(SELECT concat_ws('//', collect_set(t.name)) AS nomi_aziende, t.variazioni_annuali
FROM all_in_one t
WHERE SPLIT(t.variazioni_annuali,'//')[2] IS NOT NULL
GROUP BY t.variazioni_annuali) temp
WHERE SPLIT(temp.nomi_aziende,'//')[1] IS NOT NULL;

