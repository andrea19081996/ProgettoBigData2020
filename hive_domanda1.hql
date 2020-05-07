DROP TABLE IF EXISTS historical_stock_prices_temp;
DROP VIEW IF EXISTS historical_stock_prices;
DROP VIEW IF EXISTS app;
DROP VIEW IF EXISTS with_chiusura_iniziale;
DROP VIEW IF EXISTS with_chiusura_finale;
DROP TABLE IF EXISTS result;



CREATE TABLE historical_stock_prices_temp (ticker STRING, open DOUBLE, close DOUBLE, ads_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


LOAD DATA LOCAL INPATH '/home/davide/Documenti/ProgettoBigData2020/dataset/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices_temp;

CREATE TEMPORARY TABLE historical_stock_prices AS
SELECT * FROM historical_stock_prices_temp WHERE year(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd'))<2019 AND year(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd'))>2007;

CREATE TEMPORARY TABLE app AS
SELECT ticker, MIN(close) AS prezzo_minimo, MAX(close) AS prezzo_massimo, AVG(volume) AS volume_medio, MIN(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS prima, MAX(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS ultima
FROM historical_stock_prices
GROUP BY ticker;


CREATE TEMPORARY TABLE with_chiusura_iniziale AS
SELECT t1.*, close AS prezzo_iniziale
FROM app t1 JOIN historical_stock_prices t2 ON t1.ticker=t2.ticker
where t1.prima = from_unixtime(unix_timestamp(t2.data,'yyyy-MM-dd'),'yyyy-MM-dd');

CREATE TEMPORARY TABLE with_chiusura_finale AS
SELECT t1.*, close AS prezzo_finale
FROM with_chiusura_iniziale t1 JOIN historical_stock_prices t2 ON t1.ticker=t2.ticker
where t1.ultima = from_unixtime(unix_timestamp(t2.data,'yyyy-MM-dd'),'yyyy-MM-dd');

CREATE TABLE result AS
SELECT ticker, round(((prezzo_finale-prezzo_iniziale)/prezzo_iniziale)*100, 2) AS variazione, prezzo_minimo, prezzo_massimo, volume_medio
FROM with_chiusura_finale
ORDER BY variazione desc;

