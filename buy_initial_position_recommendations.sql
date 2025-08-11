CREATE OR REPLACE VIEW public.buy_initial_position_recommendations AS
SELECT a.account_name AS account, i.ticker, c.current_price, i.target_price, i.target_change,
       ROUND((c.current_price - i.price) / i.price, 4) AS current_change
FROM public.accounts a
CROSS JOIN (
    SELECT ticker, target_change, target_price, price,
           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
    FROM public.ticker_inflections
    WHERE type = 'high'
) i
INNER JOIN public.current_prices c ON i.ticker = c.ticker
WHERE i.rn = 1 AND i.target_price > c.current_price
AND NOT EXISTS (
    SELECT 1 FROM public.account_ticker_exclusions ex
    WHERE ex.account = a.account_name AND ex.ticker = i.ticker
)
ORDER BY a.account_name, i.ticker;