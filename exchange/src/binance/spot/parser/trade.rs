use crate::binance::spot::{
    models::{ExecutionType, Order, OrderStatus, OrderType, Side, TimeInForce},
    requests::PlaceOrderRequest,
};
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct PlaceOrderRaw {
    symbol: String,
    #[serde(rename = "orderId")]
    order_id: u128,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    #[serde(rename = "transactTime")]
    timestamp: u128,
}

impl From<(PlaceOrderRequest, PlaceOrderRaw)> for Order {
    fn from((req, raw): (PlaceOrderRequest, PlaceOrderRaw)) -> Self {
        Order {
            order_id: raw.order_id,
            client_order_id: raw.client_order_id,
            symbol: raw.symbol,
            order_side: req.side,
            order_type: req.r#type,
            order_quantity: req.quantity.unwrap_or_default(),
            order_price: if req.price.is_some() {
                req.price.unwrap()
            } else {
                Decimal::new(0, 0)
            },
            executed_qty: Decimal::new(0, 0),
            cummulative_quote_qty: Decimal::new(0, 0),
            order_status: OrderStatus::New,
            time_in_force: if req.time_in_force.is_some() {
                req.time_in_force.unwrap()
            } else {
                crate::binance::spot::models::TimeInForce::Gtc
            },
            stop_price: if req.stop_price.is_some() {
                req.stop_price.unwrap()
            } else {
                Decimal::new(0, 0)
            },
            iceberg_qty: if req.iceberg_qty.is_some() {
                req.iceberg_qty.unwrap()
            } else {
                Decimal::new(0, 0)
            },
            create_time: raw.timestamp,
            update_time: raw.timestamp,
        }
    }
}

pub fn parse_place_order(req: PlaceOrderRequest, data: &str) -> Result<Order, serde_json::Error> {
    let raw: PlaceOrderRaw = serde_json::from_str(data)?;
    Ok((req, raw).into())
}

#[derive(Debug, Deserialize)]
pub struct GetOrderRaw {
    symbol: String,
    #[serde(rename = "orderId")]
    order_id: u128,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    price: Decimal,
    #[serde(rename = "origQty")]
    orig_qty: Decimal,
    #[serde(rename = "executedQty")]
    executed_qty: Decimal,
    #[serde(rename = "origQuoteOrderQty")]
    orig_quote_order_qty: Decimal,
    #[serde(rename = "cummulativeQuoteQty")]
    cummulative_quote_qty: Decimal,
    status: OrderStatus,
    #[serde(rename = "timeInForce")]
    time_in_force: TimeInForce,
    r#type: OrderType,
    side: Side,
    #[serde(rename = "stopPrice")]
    stop_price: Decimal,
    #[serde(rename = "icebergQty")]
    iceberg_qty: Decimal,
    time: u128,
    #[serde(rename = "updateTime")]
    update_time: u128,
}

impl From<GetOrderRaw> for Order {
    fn from(raw: GetOrderRaw) -> Self {
        Order {
            order_id: raw.order_id,
            client_order_id: raw.client_order_id,
            symbol: raw.symbol,
            order_side: raw.side,
            order_type: raw.r#type,
            order_quantity: raw.orig_qty,
            order_price: raw.price,
            executed_qty: raw.executed_qty,
            cummulative_quote_qty: raw.cummulative_quote_qty,
            order_status: raw.status,
            time_in_force: raw.time_in_force,
            stop_price: raw.stop_price,
            iceberg_qty: raw.iceberg_qty,
            create_time: raw.time,
            update_time: raw.update_time,
        }
    }
}

pub fn parse_get_order(data: &str) -> Result<Order, serde_json::Error> {
    let raw: GetOrderRaw = serde_json::from_str(data)?;
    Ok(raw.into())
}

#[derive(Debug, Deserialize)]
pub struct Balance {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "f")]
    pub free: Decimal,
    #[serde(rename = "l")]
    pub locked: Decimal,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
pub enum AccountUpdateRaw {
    #[serde(rename = "outboundAccountPosition")]
    OutboundAccountPosition {
        #[serde(rename = "E")]
        event_time: u128,
        #[serde(rename = "u")]
        last_account_update: u128,
        #[serde(rename = "B")]
        balances: Vec<Balance>,
    },
    #[serde(rename = "executionReport")]
    ExecutionReport {
        #[serde(rename = "E")]
        event_time: u128,
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "c")]
        client_order_id: String,
        #[serde(rename = "S")]
        side: Side,
        #[serde(rename = "o")]
        order_type: OrderType,
        #[serde(rename = "f")]
        time_in_force: TimeInForce,
        #[serde(rename = "q")]
        quantity: Decimal,
        #[serde(rename = "p")]
        price: Decimal,
        #[serde(rename = "P")]
        stop_price: Decimal,
        #[serde(rename = "F")]
        iceberg_qty: Decimal,
        #[serde(rename = "C")]
        original_client_order_id: String,
        #[serde(rename = "x")]
        execution_type: ExecutionType,
        #[serde(rename = "X")]
        order_status: OrderStatus,
        #[serde(rename = "r")]
        order_reject_reason: String,
        #[serde(rename = "i")]
        order_id: u128,
        #[serde(rename = "l")]
        last_executed_qty: Decimal,
        #[serde(rename = "z")]
        cumulative_filled_qty: Decimal,
        #[serde(rename = "L")]
        last_executed_price: Decimal,
        #[serde(rename = "n")]
        commission: Decimal,
        #[serde(rename = "N")]
        commission_asset: Option<String>,
        #[serde(rename = "T")]
        transaction_time: u128,
        #[serde(rename = "I")]
        trade_id: u128,
        #[serde(rename = "w")]
        is_order_on_book: bool,
        #[serde(rename = "m")]
        is_maker: bool,
        #[serde(rename = "O")]
        create_time: u128,
        #[serde(rename = "Z")]
        cumulative_quote_qty: Decimal,
        #[serde(rename = "Y")]
        last_quote_qty: Decimal,
        #[serde(rename = "Q")]
        quote_order_qty: Decimal,
        #[serde(rename = "W")]
        working_time: u128,
        #[serde(rename = "V")]
        self_trade_prevention_mode: String,
    },
    #[serde(other)]
    Unknown,
}

impl AccountUpdateRaw {
    pub fn into_outbound_account_position(
        self,
    ) -> crate::binance::errors::Result<crate::binance::spot::models::OutboundAccountPosition> {
        match self {
            AccountUpdateRaw::OutboundAccountPosition {
                event_time,
                last_account_update,
                balances,
            } => Ok(crate::binance::spot::models::OutboundAccountPosition {
                balances: balances
                    .into_iter()
                    .map(|b| crate::binance::spot::models::Balance {
                        asset: b.asset,
                        free: b.free,
                        locked: b.locked,
                    })
                    .collect(),
                transaction_time: event_time,
                update_time: last_account_update,
            }),
            _ => Err(crate::binance::errors::BinanceError::ParseResultError {
                message: "Not an account update event".to_string(),
            }),
        }
    }

    pub fn into_execution_report(
        self,
    ) -> crate::binance::errors::Result<crate::binance::spot::models::ExecutionReport> {
        match self {
            AccountUpdateRaw::ExecutionReport {
                event_time: _,
                symbol,
                client_order_id,
                side,
                order_type,
                time_in_force,
                quantity,
                price,
                stop_price,
                iceberg_qty,
                original_client_order_id,
                execution_type,
                order_status,
                order_reject_reason: _,
                order_id,
                last_executed_qty,
                cumulative_filled_qty,
                last_executed_price,
                commission,
                commission_asset,
                transaction_time,
                trade_id,
                is_order_on_book: _,
                is_maker,
                create_time,
                cumulative_quote_qty,
                last_quote_qty: _,
                quote_order_qty: _,
                working_time: _,
                self_trade_prevention_mode: _,
            } => Ok(crate::binance::spot::models::ExecutionReport {
                symbol,
                order_id,
                client_order_id,
                original_client_order_id,
                order_side: side,
                time_in_force,
                order_type,
                execution_type,
                order_status,
                order_quantity: quantity,
                order_price: price,
                last_executed_qty,
                last_executed_price,
                cumulative_filled_qty,
                cumulative_quote_qty,
                commission,
                commission_asset: commission_asset.unwrap_or_default(),
                transaction_time,
                create_time,
                trade_id,
                is_maker,
                stop_price,
                iceberg_qty,
            }),
            _ => Err(crate::binance::errors::BinanceError::ParseResultError {
                message: "Not an execution report event".to_string(),
            }),
        }
    }
}
