INSERT INTO stock_price (ticker, dt, open, high, low, close, volume, dividends, stock_splits)
SELECT
    sps.ticker,
    sps.dt,
    sps.open,
    sps.high,
    sps.low,
    sps.close,
    sps.volume,
    sps.dividends,
    sps.stock_splits
FROM stock_price_stage sps
ON CONFLICT (ticker, dt) DO UPDATE
SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    dividends = EXCLUDED.dividends,
    stock_splits = EXCLUDED.stock_splits;

TRUNCATE stock_price_stage;