use crate::binance::{
    errors::Result,
    spot::{
        models::{OrderType, TimeInForce},
        parser::{parse_get_order, parse_place_order},
        requests::*,
        responses::*,
    },
    utils::{encode_params, hmac_sha256, sort_params},
};
use log::info;
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
                message: e.to_string(),
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
        unimplemented!()
    }

    pub async fn get_all_orders(&self, req: GetAllOrdersRequest) -> Result<GetAllOrdersResponse> {
        unimplemented!()
    }

    pub async fn get_trades(&self, req: GetTradesRequest) -> Result<GetTradesResponse> {
        unimplemented!()
    }

    pub async fn get_account(&self, req: GetAccountRequest) -> Result<GetAccountResponse> {
        unimplemented!()
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

        let text = resp
            .map_err(|e| crate::binance::errors::BinanceError::NetworkError {
                message: e.to_string(),
            })?
            .text()
            .await
            .map_err(|e| crate::binance::errors::BinanceError::NetworkError {
                message: e.to_string(),
            })?;

        Ok(text)
    }
}
