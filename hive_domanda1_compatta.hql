DROP TABLE IF EXISTS historical_stock_prices_temp;
DROP VIEW IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS result;



CREATE TABLE historical_stock_prices_temp (ticker STRING, open DOUBLE, close DOUBLE, ads_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


LOAD DATA LOCAL INPATH '/home/davide/Documenti/ProgettoBigData2020/dataset/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices_temp;

CREATE TEMPORARY TABLE historical_stock_prices AS
SELECT * FROM historical_stock_prices_temp WHERE year(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd'))<2019 AND year(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd'))>2007;

CREATE TABLE result AS
SELECT t.ticker, round(((prezzo_finale-prezzo_iniziale)/prezzo_iniziale)*100, 2) AS variazione, prezzo_minimo, prezzo_massimo, volume_medio
FROM
	(SELECT second.*, hsp2.close as prezzo_finale 
	FROM
		(SELECT first.*, hsp.close as prezzo_iniziale
		FROM
			(SELECT ticker, MIN(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS prima, MAX(from_unixtime(unix_timestamp(data,'yyyy-MM-dd'),'yyyy-MM-dd')) AS ultima, MIN(close) AS prezzo_minimo, MAX(close) AS prezzo_massimo, AVG(volume) AS volume_medio
			FROM historical_stock_prices
			GROUP BY ticker
			) first JOIN historical_stock_prices hsp ON first.ticker=hsp.ticker AND first.prima=from_unixtime(unix_timestamp(hsp.data,'yyyy-MM-dd'),'yyyy-MM-dd')
		) second JOIN historical_stock_prices hsp2 ON second.ticker=hsp2.ticker AND second.ultima=from_unixtime(unix_timestamp(hsp2.data,'yyyy-MM-dd'),'yyyy-MM-dd')
	) t
ORDER BY variazione desc;
