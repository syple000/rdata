use crate::binance::spot::{
    models::{Order, OrderStatus, OrderType, Side, TimeInForce},
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
