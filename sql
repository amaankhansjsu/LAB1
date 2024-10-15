#CREATING VIEW FOR STOCK DATA.

CREATE OR REPLACE VIEW stock.adhoc.stock_data_view AS
SELECT 
    CAST(DATE AS TIMESTAMP_NTZ) AS DATE, 
    CLOSE, 
    SYMBOL
FROM stock.stock_data.market_data;

#CREATING ML FORECAST MODEL.
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST stock.analytics.predict_stock_price (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'stock.adhoc.stock_data_view'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'CLOSE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

#EVALUATING MATRICS OF THE MODEL.
CALL stock.analytics.predict_stock_price!SHOW_EVALUATION_METRICS();

#MAKING PREDICTIONS USING THE MODEL
BEGIN
    CALL stock.analytics.predict_stock_price!FORECAST(
        FORECASTING_PERIODS => 7,
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    LET x := SQLID;
    CREATE OR REPLACE TABLE stock.adhoc.stock_data_forecast AS 
    SELECT * FROM TABLE(RESULT_SCAN(:x));
END;


#CREATING FINAL TABLE THAT CONTAINS ACTUAL AND FORECAST DATA
CREATE TABLE IF NOT EXISTS stock.analytics.final_data AS
SELECT 
    SYMBOL, 
    DATE, 
    CLOSE AS actual, 
    NULL AS forecast, 
    NULL AS lower_bound, 
    NULL AS upper_bound
FROM stock.stock_data.market_data
UNION ALL
SELECT 
    REPLACE(series, '"', '') AS SYMBOL, 
    ts AS DATE, 
    NULL AS actual, 
    forecast, 
    lower_bound, 
    upper_bound
FROM stock.adhoc.stock_data_forecast;
