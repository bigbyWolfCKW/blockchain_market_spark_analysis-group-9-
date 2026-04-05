-- DROP SCHEMA public;

CREATE SCHEMA public AUTHORIZATION pg_database_owner;

-- DROP SEQUENCE public.predictions_prediction_id_seq;

CREATE SEQUENCE public.predictions_prediction_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- public.crypto_group_summary definition

-- Drop table

-- DROP TABLE public.crypto_group_summary;

CREATE TABLE public.crypto_group_summary (
	summary_time timestamp NULL,
	group_key text NULL,
	metric_type text NULL,
	metric_value float8 NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL
);


-- public.crypto_summary definition

-- Drop table

-- DROP TABLE public.crypto_summary;

CREATE TABLE public.crypto_summary (
	summary_time timestamp NULL,
	symbol text NULL,
	metric_type text NULL,
	metric_value float8 NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL
);
CREATE INDEX idx_crypto_time_symbol ON public.crypto_summary USING btree (summary_time, symbol, metric_type);


-- public.live_blockchain_metrics definition

-- Drop table

-- DROP TABLE public.live_blockchain_metrics;

CREATE TABLE public.live_blockchain_metrics (
	time_bucket timestamptz NULL,
	tx_count int8 NULL,
	tx_count_7d_avg float8 NULL,
	tx_count_daily_change_pct float8 NULL,
	is_anomaly bool NULL
);


-- public.ml_btc_predictions definition

-- Drop table

-- DROP TABLE public.ml_btc_predictions;

CREATE TABLE public.ml_btc_predictions (
	"time" timestamptz NULL,
	actual_price float8 NULL,
	predicted_price float8 NOT NULL,
	error_usd float8 NULL,
	accuracy_pct float8 NULL
);


-- public.predictions definition

-- Drop table

-- DROP TABLE public.predictions;

CREATE TABLE public.predictions (
	prediction_id serial4 NOT NULL,
	target_symbol text NOT NULL,
	prediction_time timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	forecast_time timestamptz NULL,
	predicted_price float8 NULL,
	is_up_predicted bool NULL,
	confidence_score float8 NULL,
	model_name text NULL,
	actual_price_at_forecast float8 NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT predictions_pkey PRIMARY KEY (prediction_id)
);


-- public.realtime_prices definition

-- Drop table

-- DROP TABLE public.realtime_prices;

CREATE TABLE public.realtime_prices (
	symbol text NULL,
	"type" text NULL,
	avg_price float8 NULL,
	window_start timestamp NULL,
	window_end timestamp NULL,
	CONSTRAINT uq_realtime_prices UNIQUE (symbol, type, window_start)
);
CREATE INDEX idx_realtime_fast ON public.realtime_prices USING btree (type, symbol, window_start DESC);


-- public.us_sector_summary definition

-- Drop table

-- DROP TABLE public.us_sector_summary;

CREATE TABLE public.us_sector_summary (
	summary_time timestamp NULL,
	group_key text NULL,
	metric_type text NULL,
	metric_value float8 NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL
);


-- public.us_stock_summary definition

-- Drop table

-- DROP TABLE public.us_stock_summary;

CREATE TABLE public.us_stock_summary (
	summary_time timestamp NULL,
	symbol text NULL,
	metric_type text NULL,
	metric_value float8 NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL
);


-- public.real_time_fusion_dashboard source

CREATE OR REPLACE VIEW public.real_time_fusion_dashboard
AS WITH chain_features AS (
         SELECT live_blockchain_metrics.time_bucket,
            live_blockchain_metrics.tx_count,
            round(avg(live_blockchain_metrics.tx_count) OVER (ORDER BY live_blockchain_metrics.time_bucket ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS tx_count_7d_avg,
            lag(live_blockchain_metrics.tx_count, 1) OVER (ORDER BY live_blockchain_metrics.time_bucket) AS tx_count_lag_1
           FROM live_blockchain_metrics
        ), market_features AS (
         SELECT date(crypto_summary.summary_time) AS market_date,
            max(
                CASE
                    WHEN crypto_summary.metric_type = 'avg_price'::text THEN crypto_summary.metric_value
                    ELSE NULL::double precision
                END) AS btc_avg_price
           FROM crypto_summary
          WHERE crypto_summary.symbol = 'BTCUSDT'::text
          GROUP BY (date(crypto_summary.summary_time))
        )
 SELECT c.time_bucket AS "time",
    c.tx_count,
    c.tx_count_7d_avg,
    c.tx_count_lag_1,
    round(((c.tx_count - c.tx_count_lag_1) / NULLIF(c.tx_count_lag_1, 0) * 100)::numeric, 2) AS daily_change_pct,
    m.btc_avg_price
   FROM chain_features c
     LEFT JOIN market_features m ON date(c.time_bucket) = m.market_date;