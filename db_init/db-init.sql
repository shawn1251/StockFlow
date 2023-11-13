create database stockdb;
\c stockdb;

create table stock_price_stage
(
    ticker varchar(20),
    dt timestamptz,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    dividends double precision,
    stock_splits double precision
);

create table stock_price
(
    pk_ser BIGSERIAL not null,
    primary key(pk_ser),
    ticker varchar(20),
    dt timestamptz,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    dividends double precision,
    stock_splits double precision
);

create unique index ind_stockprice on stock_price(ticker, dt);
create unique index ind_stockprice_stage on stock_price_stage(ticker, dt);