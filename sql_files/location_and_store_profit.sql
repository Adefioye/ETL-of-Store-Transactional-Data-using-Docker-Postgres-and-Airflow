COPY (
    SELECT date, store_location, ROUND(CAST((SUM(sp) - SUM(cp)) AS NUMERIC), 2) AS location_profit 
    FROM clean_store_transactions 
    GROUP BY date, store_location 
    ORDER BY location_profit DESC
    ) 
TO '/var/lib/postgresql/store_files/location-based-profit.csv' With CSV DELIMITER ',' HEADER;

COPY (
    SELECT date, store_id, ROUND(CAST((SUM(sp) - SUM(cp)) AS NUMERIC), 2) AS store_profit 
    FROM clean_store_transactions 
    GROUP BY date, store_id 
    ORDER BY store_profit DESC
    ) 
TO '/var/lib/postgresql/store_files/store-based-profit.csv' With CSV DELIMITER ',' HEADER;