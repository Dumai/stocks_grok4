-- stocks.public.ticker_inflections definition

-- Drop table

-- DROP TABLE stocks.public.ticker_inflections;

CREATE TABLE stocks.public.ticker_inflections (
	id serial DEFAULT nextval('ticker_inflections_id_seq'::regclass) NOT NULL,
	ticker varchar(20) NOT NULL,
	"date" date NOT NULL,
	"type" varchar(20) NOT NULL,
	price numeric(10,4) NOT NULL,
	"close" numeric(10,4) NOT NULL,
	target_change numeric(8,4) NOT NULL,
	target_price numeric(10,4),
	days_to_target int4,
	last_updated timestamptz NOT NULL,
	CONSTRAINT ticker_inflections_pkey PRIMARY KEY (ticker,"date","type")
);
CREATE INDEX idx_ticker_inflections_date ON stocks.public.ticker_inflections ("date" DESC);
CREATE INDEX idx_ticker_inflections_ticker ON stocks.public.ticker_inflections (ticker);
CREATE INDEX idx_ticker_inflections_type ON stocks.public.ticker_inflections ("type");
CREATE INDEX idx_ticker_inflections_updated ON stocks.public.ticker_inflections (last_updated DESC);