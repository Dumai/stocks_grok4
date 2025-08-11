CREATE OR REPLACE VIEW public.buy_more_recommendations AS
SELECT p.account,
    p.ticker,
    p.buy_date,
    p.invested_amount,
    p.shares,
    round(p.buy_price, 2) AS buy_price,
    round(((c.current_price / p.buy_price) - (1)::numeric), 4) AS current_change,
    i.target_change,
    c.current_price,
    round((p.buy_price * ((1)::numeric + i.target_change)), 4) AS target_buy_price,
        CASE
            WHEN ((round(((c.current_price / p.buy_price) - (1)::numeric), 4) < i.target_change) AND (p.profit_position = false)) THEN 'Buy More'::text
            ELSE 'HOLD'::text
        END AS recommendation
   FROM (
       SELECT op.*,
              row_number() OVER (PARTITION BY op.account, op.ticker ORDER BY op.buy_price ASC) AS rn
       FROM public.open_positions op
   ) p
   JOIN ( 
       SELECT ticker_inflections.ticker,
              ticker_inflections.target_change,
              row_number() OVER (PARTITION BY ticker_inflections.ticker ORDER BY ticker_inflections.date DESC) AS rn
       FROM public.ticker_inflections
       WHERE ((ticker_inflections.type)::text = 'low'::text)
   ) i ON ((((p.ticker)::text = (i.ticker)::text) AND (i.rn = 1)))
   JOIN public.current_prices c ON (((p.ticker)::text = (c.ticker)::text))
   WHERE p.rn = 1 AND (
        CASE
            WHEN ((round(((c.current_price / p.buy_price) - (1)::numeric), 4) < i.target_change) AND (p.profit_position = false)) THEN 'Buy More'::text
            ELSE 'HOLD'::text
        END = 'Buy More'::text)
   ORDER BY p.account, p.ticker, p.buy_date;