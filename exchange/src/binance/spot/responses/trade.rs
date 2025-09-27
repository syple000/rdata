use crate::binance::spot::models::{Account, Order, Trade};

pub type PlaceOrderResponse = Order;
pub type CancelOrderResponse = ();
pub type GetAccountResponse = Account;
pub type GetOrderResponse = Order;
pub type GetOpenOrdersResponse = Vec<Order>;
pub type GetAllOrdersResponse = Vec<Order>;
pub type GetTradesResponse = Vec<Trade>;
