-- Create the table
CREATE TABLE IF NOT EXISTS candlestick_data (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    ticker VARCHAR(10),
    open_time TIMESTAMP,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume FLOAT,
    close_time TIMESTAMP,
    quote_asset_volume FLOAT,
    trades_amount INT
);

-- Insert the JSON data
INSERT INTO candlestick_data (
    ticker, open_time, open_price, high_price, low_price, close_price,
    volume, close_time, quote_asset_volume, trades_amount
) VALUES (
    'btcusdt','2023-12-06T21:00:00', 43755.78, 43879.57, 43744.09, 43832.45,
    816.99276, '2023-12-06T21:59:59.999000', 35790842.2402955, 23025
);
