pub mod models;
pub mod requests;
pub mod responses;

mod symbol_trading;

mod depth;
#[cfg(test)]
mod depth_test;

mod kline;
#[cfg(test)]
mod kline_test;

mod trade;
#[cfg(test)]
mod trade_test;

mod market_api;
#[cfg(test)]
mod market_api_test;

mod market_stream;
#[cfg(test)]
mod market_stream_test;

mod trade_api;
#[cfg(test)]
mod trade_api_test;

mod trade_stream;
#[cfg(test)]
mod trade_stream_test;

mod parser;
