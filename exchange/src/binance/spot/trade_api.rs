use crate::binance::{
    errors::{BinanceError, Result},
    spot::{
        models::{OrderType, TimeInForce},
        parser::{
            parse_get_account, parse_get_all_orders, parse_get_open_orders, parse_get_order,
            parse_get_trades, parse_place_order,
        },
        requests::*,
        responses::*,
    },
    utils::{encode_params, hmac_sha256, sort_params},
};
use log::{error, info};
use rate_limiter::RateLimiter;
use std::sync::Arc;

pub struct TradeApi {
    client: reqwest::Client,
    base_url: String,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    api_key: String,
    secret_key: String,
}

impl TradeApi {
    pub fn new(
        base_url: String,
        rate_limiters: Option<Arc<Vec<RateLimiter>>>,
        api_key: String,
        secret_key: String,
    ) -> Self {
        TradeApi {
            client: reqwest::Client::new(),
            base_url,
            rate_limiters: rate_limiters,
            api_key,
            secret_key,
        }
    }

    pub async fn place_order(&self, req: PlaceOrderRequest) -> Result<PlaceOrderResponse> {
        match &req.r#type {
            OrderType::Limit => {
                if req.time_in_force.is_none() || req.price.is_none() || req.quantity.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "time_in_force, price, quantity are required for LIMIT order"
                            .to_string(),
                    });
                }
            }
            OrderType::Market => {
                if req.quantity.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "quantity is required for MARKET order".to_string(),
                    });
                }
            }
            OrderType::StopLoss => {
                if req.quantity.is_none() || req.stop_price.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "quantity, stop_price are required for STOP_LOSS order"
                            .to_string(),
                    });
                }
            }
            OrderType::StopLossLimit => {
                if req.time_in_force.is_none()
                    || req.quantity.is_none()
                    || req.price.is_none()
                    || req.stop_price.is_none()
                {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "time_in_force, quantity, price, stop_price are required for STOP_LOSS_LIMIT order"
                            .to_string(),
                    });
                }
            }
            OrderType::TakeProfit => {
                if req.quantity.is_none() || req.stop_price.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "quantity, stop_price are required for TAKE_PROFIT order"
                            .to_string(),
                    });
                }
            }
            OrderType::TakeProfitLimit => {
                if req.time_in_force.is_none()
                    || req.quantity.is_none()
                    || req.price.is_none()
                    || req.stop_price.is_none()
                {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "time_in_force, quantity, price, stop_price are required for TAKE_PROFIT_LIMIT order"
                            .to_string(),
                    });
                }
            }
            OrderType::LimitMaker => {
                if req.quantity.is_none() || req.price.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "quantity, price are required for LIMIT_MAKER order".to_string(),
                    });
                }
            }
        }
        if req.iceberg_qty.is_some() {
            if !matches!(req.r#type, OrderType::Limit | OrderType::LimitMaker) {
                return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                    message: "iceberg_qty is only valid for LIMIT and LIMIT_MAKER orders"
                        .to_string(),
                });
            }
            if req.time_in_force.is_none()
                || !matches!(req.time_in_force.as_ref().unwrap(), TimeInForce::Gtc)
            {
                return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                    message: "time_in_force must be GTC when iceberg_qty is set".to_string(),
                });
            }
        }

        let mut params = vec![
            ("symbol", req.symbol.clone()),
            ("side", req.side.as_str().to_string()),
            ("type", req.r#type.as_str().to_string()),
            ("newOrderRespType", "ACK".to_string()),
        ];
        if req.time_in_force.is_some() {
            params.push((
                "timeInForce",
                req.time_in_force.as_ref().unwrap().as_str().to_string(),
            ));
        }
        if req.quantity.is_some() {
            params.push(("quantity", req.quantity.unwrap().to_string()));
        }
        if req.price.is_some() {
            params.push(("price", req.price.unwrap().to_string()));
        }
        if req.new_client_order_id.is_some() {
            params.push((
                "newClientOrderId",
                req.new_client_order_id.as_ref().unwrap().to_string(),
            ));
        }
        if req.stop_price.is_some() {
            params.push(("stopPrice", req.stop_price.unwrap().to_string()));
        }
        if req.iceberg_qty.is_some() {
            params.push(("icebergQty", req.iceberg_qty.unwrap().to_string()));
        }

        let text = self
            .send_signed_request(reqwest::Method::POST, "/api/v3/order", params, 1)
            .await?;

        parse_place_order(req, &text).map_err(|e| {
            crate::binance::errors::BinanceError::ParseResultError {
                message: format!("{}, {}", text, e.to_string()),
            }
        })
    }

    pub async fn cancel_order(&self, req: CancelOrderRequest) -> Result<CancelOrderResponse> {
        if req.order_id.is_none() && req.orig_client_order_id.is_none() {
            return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                message: "order_id or orig_client_order_id is required".to_string(),
            });
        }
        let mut params = vec![("symbol", req.symbol.clone())];
        if req.order_id.is_some() {
            params.push(("orderId", req.order_id.as_ref().unwrap().to_string()));
        }
        if req.orig_client_order_id.is_some() {
            params.push((
                "origClientOrderId",
                req.orig_client_order_id.as_ref().unwrap().to_string(),
            ));
        }
        if req.new_client_order_id.is_some() {
            params.push((
                "newClientOrderId",
                req.new_client_order_id.as_ref().unwrap().to_string(),
            ));
        }

        let text = self
            .send_signed_request(reqwest::Method::DELETE, "/api/v3/order", params, 1)
            .await?;

        if let Ok(error_response) = serde_json::from_str::<serde_json::Value>(&text) {
            if error_response.get("code").is_some() && error_response.get("msg").is_some() {
                return Err(crate::binance::errors::BinanceError::ParseResultError {
                    message: format!(
                        "Error {}: {}",
                        error_response["code"].as_i64().unwrap_or(0),
                        error_response["msg"]
                            .as_str()
                            .unwrap_or("Unknown error")
                            .to_string()
                    ),
                });
            }
        }

        Ok(())
    }

    pub async fn get_order(&self, req: GetOrderRequest) -> Result<GetOrderResponse> {
        if req.order_id.is_none() && req.orig_client_order_id.is_none() {
            return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                message: "order_id or orig_client_order_id is required".to_string(),
            });
        }
        let mut params = vec![("symbol", req.symbol.clone())];
        if req.order_id.is_some() {
            params.push(("orderId", req.order_id.as_ref().unwrap().to_string()));
        }
        if req.orig_client_order_id.is_some() {
            params.push((
                "origClientOrderId",
                req.orig_client_order_id.as_ref().unwrap().to_string(),
            ));
        }

        let text = self
            .send_signed_request(reqwest::Method::GET, "/api/v3/order", params, 1)
            .await?;
        info!("Get order response: {}", text);

        parse_get_order(&text).map_err(|e| crate::binance::errors::BinanceError::ParseResultError {
            message: e.to_string(),
        })
    }

    pub async fn get_open_orders(
        &self,
        req: GetOpenOrdersRequest,
    ) -> Result<GetOpenOrdersResponse> {
        let mut params = vec![];

        let weight = if req.symbol.is_some() {
            params.push(("symbol", req.symbol.as_ref().unwrap().clone()));
            6
        } else {
            80
        };

        let text = self
            .send_signed_request(reqwest::Method::GET, "/api/v3/openOrders", params, weight)
            .await?;

        info!("Get open orders response: {}", text);
        parse_get_open_orders(&text).map_err(|e| {
            crate::binance::errors::BinanceError::ParseResultError {
                message: format!("{}, {}", text, e.to_string()),
            }
        })
    }

    pub async fn get_all_orders(&self, req: GetAllOrdersRequest) -> Result<GetAllOrdersResponse> {
        let start_time = req.start_time.unwrap_or(0);
        let end_time = req.end_time.unwrap_or(0);
        let batch_size = req.limit.unwrap_or(500) as u32;

        if start_time > 0 && start_time >= end_time {
            return Err(BinanceError::ParametersInvalid {
                message: format!(
                    "start_time: {} must be less than end_time: {}",
                    start_time, end_time
                ),
            });
        }
        if end_time > 0 && (end_time - start_time) > 24 * 60 * 60 * 1000 {
            return Err(BinanceError::ParametersInvalid {
                message: "The time between start_time and end_time can't be longer than 24 hours"
                    .to_string(),
            });
        }

        let mut all_orders = Vec::<crate::binance::spot::models::Order>::new();
        let mut current_order_id = req.order_id;

        loop {
            info!(
                "get all orders with parameters start_time: {}, end_time: {}, limit: {}, order_id: {:?}",
                start_time, end_time, batch_size, current_order_id
            );

            let mut params = vec![
                ("symbol", req.symbol.clone()),
                ("limit", batch_size.to_string()),
            ];

            if let Some(order_id) = current_order_id {
                params.push(("orderId", order_id.to_string()));
            } else if start_time > 0 {
                params.push(("startTime", start_time.to_string()));
                params.push(("endTime", end_time.to_string()));
            }

            let text = self
                .send_signed_request(reqwest::Method::GET, "/api/v3/allOrders", params, 20)
                .await?;

            let mut batch_orders = parse_get_all_orders(&text).map_err(|e| {
                error!("Parse result: {:?} error: {:?}", text, e);
                BinanceError::ParseResultError {
                    message: e.to_string(),
                }
            })?;

            batch_orders.sort_by(|a, b| a.order_id.cmp(&b.order_id));

            if start_time > 0 {
                let mut index = batch_orders.len();
                for (i, order) in batch_orders.iter().enumerate() {
                    if order.update_time > end_time {
                        index = i;
                        break;
                    }
                }
                batch_orders.truncate(index);
            }

            all_orders.extend_from_slice(&batch_orders);

            if batch_orders.len() < batch_size as usize {
                break;
            }
            if start_time == 0 {
                break;
            }

            if let Some(last_order) = batch_orders.last() {
                current_order_id = Some(last_order.order_id + 1);
            } else {
                break;
            }
        }

        Ok(all_orders)
    }

    pub async fn get_trades(&self, req: GetTradesRequest) -> Result<GetTradesResponse> {
        let start_time = req.start_time.unwrap_or(0);
        let end_time = req.end_time.unwrap_or(0);
        let batch_size = req.limit.unwrap_or(500) as u32;

        if start_time > 0 && start_time >= end_time {
            return Err(BinanceError::ParametersInvalid {
                message: format!(
                    "start_time: {} must be less than end_time: {}",
                    start_time, end_time
                ),
            });
        }

        if end_time > 0 && (end_time - start_time) > 24 * 60 * 60 * 1000 {
            return Err(BinanceError::ParametersInvalid {
                message: "The time between start_time and end_time can't be longer than 24 hours"
                    .to_string(),
            });
        }

        // 验证参数组合的合法性
        if (req.from_id.is_some() || req.order_id.is_some())
            && (req.start_time.is_some() || req.end_time.is_some())
        {
            return Err(BinanceError::ParametersInvalid {
                message:
                    "If fromId or orderId is set, neither startTime nor endTime can be provided"
                        .to_string(),
            });
        }

        let mut all_trades = Vec::<crate::binance::spot::models::Trade>::new();
        let mut current_from_id = req.from_id;

        loop {
            info!(
                "get trades with parameters symbol: {}, start_time: {}, end_time: {}, limit: {}, order_id: {:?}, from_id: {:?}",
                req.symbol, start_time, end_time, batch_size, req.order_id, current_from_id
            );

            let mut params = vec![
                ("symbol", req.symbol.clone()),
                ("limit", batch_size.to_string()),
            ];

            let weight = if req.order_id.is_some() {
                params.push(("orderId", req.order_id.unwrap().to_string()));
                5
            } else {
                20
            };

            if let Some(from_id) = current_from_id {
                params.push(("fromId", from_id.to_string()));
            } else if start_time > 0 {
                params.push(("startTime", start_time.to_string()));
                params.push(("endTime", end_time.to_string()));
            }

            let text = self
                .send_signed_request(reqwest::Method::GET, "/api/v3/myTrades", params, weight)
                .await?;

            let mut batch_trades = parse_get_trades(&text).map_err(|e| {
                error!("Parse result: {:?} error: {:?}", text, e);
                BinanceError::ParseResultError {
                    message: e.to_string(),
                }
            })?;

            // 按照 trade_id 排序
            batch_trades.sort_by(|a, b| a.trade_id.cmp(&b.trade_id));

            // 如果有时间范围限制，过滤超出范围的交易
            if start_time > 0 && end_time > 0 {
                let mut index = batch_trades.len();
                for (i, trade) in batch_trades.iter().enumerate() {
                    if trade.timestamp > end_time {
                        index = i;
                        break;
                    }
                }
                batch_trades.truncate(index);
            }

            all_trades.extend_from_slice(&batch_trades);

            // 如果返回的数据少于请求的数量，说明已经获取完所有数据
            if batch_trades.len() < batch_size as usize {
                break;
            }

            if start_time == 0 {
                break;
            }

            // 设置下一次请求的 from_id
            if let Some(last_trade) = batch_trades.last() {
                current_from_id = Some(last_trade.trade_id + 1);
            } else {
                break;
            }
        }

        Ok(all_trades)
    }

    pub async fn get_account(&self, _: GetAccountRequest) -> Result<GetAccountResponse> {
        let params = vec![];

        let text = self
            .send_signed_request(reqwest::Method::GET, "/api/v3/account", params, 20)
            .await?;

        info!("Get account response: {}", text);

        parse_get_account(&text).map_err(|e| {
            crate::binance::errors::BinanceError::ParseResultError {
                message: format!("{}, {}", text, e.to_string()),
            }
        })
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        mut params: Vec<(&str, String)>,
        weight: u64,
    ) -> Result<String> {
        // 添加默认窗口和时间戳参数
        params.push(("timestamp", time::get_current_milli_timestamp().to_string()));
        params.push(("recvWindow", "5000".to_string()));

        sort_params(&mut params);

        // 添加签名
        let signature = hmac_sha256(&self.secret_key, encode_params(&params).as_str());
        params.push(("signature", signature));

        if let Some(rate_limiters) = &self.rate_limiters {
            for rl in rate_limiters.iter() {
                _ = rl.wait(weight).await;
            }
        }

        let resp = match method {
            reqwest::Method::GET => {
                self.client
                    .get(format!("{}{}", self.base_url, endpoint).as_str())
                    .header("X-MBX-APIKEY", self.api_key.clone())
                    .query(&params)
                    .send()
                    .await
            }
            reqwest::Method::POST => {
                self.client
                    .post(format!("{}{}", self.base_url, endpoint).as_str())
                    .header("X-MBX-APIKEY", self.api_key.clone())
                    .query(&params)
                    .send()
                    .await
            }
            reqwest::Method::DELETE => {
                self.client
                    .delete(format!("{}{}", self.base_url, endpoint).as_str())
                    .header("X-MBX-APIKEY", self.api_key.clone())
                    .query(&params)
                    .send()
                    .await
            }
            _ => {
                return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                    message: "Unsupported HTTP method".to_string(),
                })
            }
        };

        let resp = resp.map_err(|e| crate::binance::errors::BinanceError::NetworkError {
            message: e.to_string(),
        })?;

        if resp.status() != reqwest::StatusCode::OK {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            error!("Response error: status: {}, text: {}", status, text);
            return Err(BinanceError::ParseResultError {
                message: format!("status: {}, text: {}", status, text),
            });
        }

        let text =
            resp.text()
                .await
                .map_err(|e| crate::binance::errors::BinanceError::NetworkError {
                    message: e.to_string(),
                })?;

        Ok(text)
    }
}
