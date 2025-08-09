-- stocks.public.interested_tickers definition

-- Drop table

-- DROP TABLE stocks.public.interested_tickers;

CREATE TABLE stocks.public.interested_tickers (
	id serial DEFAULT nextval('interested_tickers_id_seq'::regclass) NOT NULL,
	ticker varchar(20) NOT NULL,
	golden bool DEFAULT false NOT NULL,
	notes text,
	created_at timestamptz DEFAULT now(),
	updated_at timestamptz DEFAULT now(),
	CONSTRAINT interested_tickers_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_interested_tickers_golden ON stocks.public.interested_tickers (golden);
CREATE INDEX idx_interested_tickers_ticker ON stocks.public.interested_tickers (ticker);
CREATE UNIQUE INDEX interested_tickers_ticker_key ON stocks.public.interested_tickers (ticker);