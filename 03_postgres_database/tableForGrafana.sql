-- DROP SCHEMA public;

CREATE SCHEMA public AUTHORIZATION pg_database_owner;

COMMENT ON SCHEMA public IS 'standard public schema';

-- DROP SEQUENCE public.predictions_prediction_id_seq;

CREATE SEQUENCE public.predictions_prediction_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- public.blockchain_market_fusion definition

-- Drop table

-- DROP TABLE public.blockchain_market_fusion;

CREATE TABLE public.blockchain_market_fusion (
	join_date text NULL,
	time_bucket timestamptz NULL,
	tx_count int8 NULL,
	tx_count_7d_avg float8 NULL,
	tx_count_daily_change_pct float8 NULL,
	is_anomaly bool NULL,
	symbol text NULL,
	btc_avg_price numeric(38, 18) NULL,
	btc_min_price float8 NULL,
	btc_max_price float8 NULL,
	avg_fee float8 NULL,
	max_fee float8 NULL,
	total_fee float8 NULL,
	avg_tx_size float8 NULL,
	max_tx_size float8 NULL,
	total_tx_size float8 NULL,
	avg_input_value float8 NULL,
	max_input_value float8 NULL,
	total_input_value float8 NULL,
	tx_count_lag_1 float8 NULL,
	avg_fee_lag_1 float8 NULL,
	total_input_value_lag_1 float8 NULL,
	avg_fee_7d_avg float8 NULL,
	total_input_value_7d_avg float8 NULL,
	avg_fee_change_pct float8 NULL,
	input_value_change_pct float8 NULL,
	fee_per_tx float8 NULL,
	input_value_per_tx float8 NULL,
	size_per_tx float8 NULL,
	is_high_tx_count int4 NULL,
	is_high_fee int4 NULL,
	is_high_input_value int4 NULL
);


-- public.crypto_group_summary definition

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


-- public.ml_btc_hourly_realized_volatility_predictions definition

-- Drop table

-- DROP TABLE public.ml_btc_hourly_realized_volatility_predictions;

CREATE TABLE public.ml_btc_hourly_realized_volatility_predictions (
	hour_bucket timestamptz NULL,
	market_close float8 NULL,
	actual_realized_volatility float8 NULL,
	predicted_realized_volatility float8 NULL,
	actual_realized_volatility_pct float8 NULL,
	predicted_realized_volatility_pct float8 NULL,
	high_volatility_threshold float8 NULL,
	actual_high_volatility float8 NULL,
	predicted_high_risk float8 NULL,
	model_name text NOT NULL,
	created_at timestamptz NOT NULL
);


-- public.ml_btc_hourly_volatility_predictions definition

-- Drop table

-- DROP TABLE public.ml_btc_hourly_volatility_predictions;

CREATE TABLE public.ml_btc_hourly_volatility_predictions (
	hour_bucket timestamptz NULL,
	market_close float8 NULL,
	actual_volatility_spike float8 NULL,
	predicted_volatility_spike float8 NULL,
	spike_probability float8 NULL,
	model_name text NOT NULL,
	created_at timestamptz NOT NULL
);


-- public.ml_btc_model_metrics definition

-- Drop table

-- DROP TABLE public.ml_btc_model_metrics;

CREATE TABLE public.ml_btc_model_metrics (
	model_name text NULL,
	rmse float8 NULL,
	mae float8 NULL,
	r2 float8 NULL,
	train_rows int8 NULL,
	test_rows int8 NULL,
	feature_count int8 NULL,
	cv_method text NULL,
	best_params text NULL
);


-- public.ml_btc_predictions definition

-- Drop table

-- DROP TABLE public.ml_btc_predictions;

CREATE TABLE public.ml_btc_predictions (
	event_time timestamptz NULL,
	actual_price float8 NULL,
	predicted_price float8 NOT NULL,
	model_name text NOT NULL,
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


-- public.crypto_daily_prices source

CREATE OR REPLACE VIEW public.crypto_daily_prices
AS SELECT date(realtime_prices.window_start) AS price_date,
    realtime_prices.symbol,
    realtime_prices.type,
    round(avg(realtime_prices.avg_price)::numeric, 2) AS daily_avg_price,
    min(realtime_prices.avg_price) AS daily_min_price,
    max(realtime_prices.avg_price) AS daily_max_price,
    count(*) AS price_points,
    min(realtime_prices.window_start) AS first_price_time,
    max(realtime_prices.window_start) AS last_price_time
   FROM realtime_prices
  WHERE realtime_prices.type = 'crypto'::text
  GROUP BY (date(realtime_prices.window_start)), realtime_prices.symbol, realtime_prices.type;


-- public.ml_btc_predictions_timeseries source

CREATE OR REPLACE VIEW public.ml_btc_predictions_timeseries
AS SELECT ml_btc_predictions.event_time AS "time",
    ml_btc_predictions.model_name AS series_name,
    ml_btc_predictions.actual_price,
    ml_btc_predictions.predicted_price,
    ml_btc_predictions.error_usd,
    ml_btc_predictions.accuracy_pct
   FROM ml_btc_predictions
  WHERE ml_btc_predictions.event_time IS NOT NULL;


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