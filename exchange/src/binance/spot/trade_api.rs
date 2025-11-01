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
use std::{sync::Arc, time::Duration};

pub struct TradeApi {
    client: Option<reqwest::Client>,
    base_url: String,
    proxy_url: Option<String>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    api_key: String,
    secret_key: String,
    timeout_milli_secs: u64,
}

impl TradeApi {
    pub fn new(
        base_url: String,
        proxy_url: Option<String>,
        rate_limiters: Option<Arc<Vec<RateLimiter>>>,
        api_key: String,
        secret_key: String,
        timeout_milli_secs: u64,
    ) -> Self {
        TradeApi {
            client: None,
            base_url,
            proxy_url,
            rate_limiters: rate_limiters,
            api_key,
            secret_key,
            timeout_milli_secs,
        }
    }

    pub fn init(&mut self) -> Result<()> {
        let client_builder = reqwest::Client::builder();

        let client = if let Some(proxy_url) = &self.proxy_url {
            client_builder
                .proxy(reqwest::Proxy::all(proxy_url).map_err(|e| {
                    crate::binance::errors::BinanceError::ParametersInvalid {
                        message: format!("proxy url invalid: {}, error: {}", proxy_url, e),
                    }
                })?)
                .build()
                .map_err(
                    |e| crate::binance::errors::BinanceError::ParametersInvalid {
                        message: format!(
                            "build client with proxy url: {} failed: {}",
                            proxy_url, e
                        ),
                    },
                )?
        } else {
            client_builder.build().map_err(|e| {
                crate::binance::errors::BinanceError::ParametersInvalid {
                    message: format!("build client failed: {}", e),
                }
            })?
        };

        self.client = Some(client);
        Ok(())
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

        let _ = self
            .send_signed_request(reqwest::Method::DELETE, "/api/v3/order", params, 1)
            .await?;

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
        let mut params = vec![
            ("symbol", req.symbol.clone()),
            ("limit", req.limit.unwrap_or(500).to_string()),
        ];
        if let Some(from_order_id) = req.from_id {
            if req.start_time.is_some() || req.end_time.is_some() {
                return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                    message: "from_id cannot be sent with start_time or end_time".to_string(),
                });
            }
            params.push(("orderId", from_order_id.to_string()));
        } else {
            if let Some(start_time) = req.start_time {
                params.push(("startTime", start_time.to_string()));
            }
            if let Some(end_time) = req.end_time {
                params.push(("endTime", end_time.to_string()));
            }
            if req.start_time.is_some() && req.end_time.is_some() {
                if req.end_time.unwrap() <= req.start_time.unwrap() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "end_time must be greater than start_time".to_string(),
                    });
                }
                if req.end_time.unwrap() - req.start_time.unwrap() > 24 * 60 * 60 * 1000 {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message:
                            "time range between start_time and end_time cannot exceed 24 hours"
                                .to_string(),
                    });
                }
            }
        }

        let text = self
            .send_signed_request(reqwest::Method::GET, "/api/v3/allOrders", params, 20)
            .await?;

        let mut orders = parse_get_all_orders(&text).map_err(|e| {
            error!("Parse result: {:?} error: {:?}", text, e);
            BinanceError::ParseResultError {
                message: e.to_string(),
            }
        })?;

        orders.sort_by(|a, b| a.order_id.cmp(&b.order_id));

        Ok(orders)
    }

    pub async fn get_trades(&self, req: GetTradesRequest) -> Result<GetTradesResponse> {
        let mut params = vec![
            ("symbol", req.symbol.clone()),
            ("limit", req.limit.unwrap_or(500).to_string()),
        ];

        if let Some(order_id) = req.order_id {
            if req.start_time.is_some() || req.end_time.is_some() {
                return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                    message: "order_id cannot be sent with start_time or end_time".to_string(),
                });
            }
            params.push(("orderId", order_id.to_string()));
        }
        if let Some(from_id) = req.from_id {
            if req.start_time.is_some() || req.end_time.is_some() {
                return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                    message: "from_id cannot be sent with start_time or end_time".to_string(),
                });
            }
            params.push(("fromId", from_id.to_string()));
        }
        if let Some(start_time) = req.start_time {
            params.push(("startTime", start_time.to_string()));
        }
        if let Some(end_time) = req.end_time {
            params.push(("endTime", end_time.to_string()));
        }

        let weight = if req.order_id.is_some() {
            params.push(("orderId", req.order_id.unwrap().to_string()));
            5
        } else {
            20
        };

        let text = self
            .send_signed_request(reqwest::Method::GET, "/api/v3/myTrades", params, weight)
            .await?;

        let mut trades = parse_get_trades(&text).map_err(|e| {
            error!("Parse result: {:?} error: {:?}", text, e);
            BinanceError::ParseResultError {
                message: e.to_string(),
            }
        })?;
        trades.sort_by(|a, b| a.trade_id.cmp(&b.trade_id));

        Ok(trades)
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
        if let None = &self.client {
            return Err(BinanceError::ParametersInvalid {
                message: "client is not initialized, please call init() first".to_string(),
            });
        }
        let client = self.client.as_ref().unwrap();

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
                client
                    .get(format!("{}{}", self.base_url, endpoint).as_str())
                    .header("X-MBX-APIKEY", self.api_key.clone())
                    .query(&params)
                    .timeout(Duration::from_millis(self.timeout_milli_secs))
                    .send()
                    .await
            }
            reqwest::Method::POST => {
                client
                    .post(format!("{}{}", self.base_url, endpoint).as_str())
                    .header("X-MBX-APIKEY", self.api_key.clone())
                    .query(&params)
                    .timeout(Duration::from_millis(self.timeout_milli_secs))
                    .send()
                    .await
            }
            reqwest::Method::DELETE => {
                client
                    .delete(format!("{}{}", self.base_url, endpoint).as_str())
                    .header("X-MBX-APIKEY", self.api_key.clone())
                    .query(&params)
                    .timeout(Duration::from_millis(self.timeout_milli_secs))
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
            message: format!("send request error: {}", e),
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
