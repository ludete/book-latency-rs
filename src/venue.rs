use anyhow::{Context, Result, bail, ensure};
use clap::ValueEnum;
use reqwest::Client;
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, ValueEnum)]
pub enum Venue {
    Hyperliquid,
    Entropy,
    Lighter,
    LighterRh,
    Tradexyz,
    Gate,
    Arcus,
    Ondo,
}

impl Venue {
    pub fn name(self) -> &'static str {
        match self {
            Self::Hyperliquid => "hyperliquid",
            Self::Entropy => "entropy",
            Self::Lighter => "lighter",
            Self::LighterRh => "lighter-rh",
            Self::Tradexyz => "tradexyz",
            Self::Gate => "gate",
            Self::Arcus => "arcus",
            Self::Ondo => "ondo",
        }
    }

    pub fn is_lighter(self) -> bool {
        matches!(self, Self::Lighter | Self::LighterRh)
    }

    fn dex(self) -> &'static str {
        match self {
            Self::Entropy => "io",
            Self::Tradexyz => "xyz",
            _ => "",
        }
    }

    pub fn ws_url(self) -> &'static str {
        match self {
            Self::Hyperliquid | Self::Entropy | Self::Tradexyz => "wss://api.hyperliquid.xyz/ws",
            Self::Lighter => "wss://mainnet.zklighter.elliot.ai/stream",
            Self::LighterRh => "wss://api.rh.lighter.xyz/stream",
            Self::Gate => "wss://fx-ws.gateio.ws/v4/ws/usdt",
            Self::Arcus => "wss://api.arcus.xyz/v1/ws",
            Self::Ondo => "wss://api.ondoperps.xyz/ws",
        }
    }

    fn symbol_market(self, symbol: &str) -> String {
        match self {
            Self::Gate if !symbol.contains('_') => format!("{symbol}_USDT"),
            Self::Arcus if !symbol.ends_with("-USD") => format!("{symbol}-USD"),
            Self::Ondo if !symbol.ends_with("-USD.P") => format!("{symbol}-USD.P"),
            Self::Entropy | Self::Tradexyz => format!("{}:{symbol}", self.dex()),
            _ => symbol.to_owned(),
        }
    }

    pub async fn load_market(self, client: &Client, symbol: &str) -> Result<String> {
        let request = match self {
            Self::Hyperliquid | Self::Entropy | Self::Tradexyz => {
                if !self.dex().is_empty() {
                    let dexes: Value = client
                        .post("https://api.hyperliquid.xyz/info")
                        .json(&json!({"type": "perpDexs"}))
                        .send()
                        .await?
                        .error_for_status()?
                        .json()
                        .await?;
                    ensure!(
                        dexes
                            .as_array()
                            .context("invalid perpDexs response")?
                            .iter()
                            .any(|d| d["name"] == self.dex()),
                        "dex '{}' not found",
                        self.dex()
                    );
                }
                client
                    .post("https://api.hyperliquid.xyz/info")
                    .json(&json!({"type": "meta", "dex": self.dex()}))
            }
            Self::Lighter => client.get("https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"),
            Self::LighterRh => client.get("https://api.rh.lighter.xyz/api/v1/orderBooks"),
            Self::Gate => {
                let mut url =
                    reqwest::Url::parse("https://api.gateio.ws/api/v4/futures/usdt/contracts/")?;
                url.path_segments_mut()
                    .unwrap()
                    .pop_if_empty()
                    .push(&self.symbol_market(symbol));
                client.get(url)
            }
            Self::Arcus => client
                .get("https://api.arcus.xyz/v1/markets")
                .query(&[("market", self.symbol_market(symbol))]),
            Self::Ondo => client.get("https://api.ondoperps.xyz/v1/markets"),
        };
        let data: Value = request.send().await?.error_for_status()?.json().await?;
        self.resolve_market(symbol, &data)
    }

    fn resolve_market(self, symbol: &str, data: &Value) -> Result<String> {
        let wanted = self.symbol_market(symbol);
        match self {
            Self::Hyperliquid | Self::Entropy | Self::Tradexyz => {
                for market in data["universe"].as_array().context("missing universe")? {
                    if market["name"] == wanted || market["name"] == symbol {
                        ensure!(market["isDelisted"] != true, "{wanted} is delisted");
                        return Ok(market["name"]
                            .as_str()
                            .context("invalid coin name")?
                            .to_owned());
                    }
                }
            }
            Self::Lighter | Self::LighterRh => {
                for market in data["order_books"]
                    .as_array()
                    .context("missing order_books")?
                {
                    if market["symbol"] == symbol
                        && market.get("market_type").is_none_or(|t| t == "perp")
                    {
                        ensure!(market["status"] == "active", "{symbol} is not active");
                        let id = &market["market_id"];
                        let id = id
                            .as_u64()
                            .or_else(|| id.as_str()?.parse().ok())
                            .context("invalid market_id")?;
                        return Ok(id.to_string());
                    }
                }
            }
            Self::Gate => {
                ensure!(
                    data["name"] == wanted,
                    "unexpected Gate contract metadata for {wanted}"
                );
                ensure!(data["in_delisting"] != true, "{wanted} is delisting");
                return Ok(wanted);
            }
            Self::Arcus => {
                for market in data["markets"].as_array().context("missing markets")? {
                    if market["marketDisplayName"] == wanted {
                        ensure!(
                            market["status"] == "ONLINE" && market["type"] == "PERPETUAL",
                            "{wanted} is not an online perpetual"
                        );
                        return Ok(wanted);
                    }
                }
            }
            Self::Ondo => {
                ensure!(data["success"] == true, "Ondo metadata failed: {data}");
                for market in data["result"]["perps"]["tradingPairs"]
                    .as_array()
                    .context("missing perps.tradingPairs")?
                {
                    if market["market"] == wanted {
                        ensure!(
                            market["disabled"] != true && market["tradingDisabled"] != true,
                            "{wanted} is disabled"
                        );
                        return Ok(wanted);
                    }
                }
            }
        }
        bail!("{wanted} not found")
    }

    pub fn subscription(self, market: &str, now: u64) -> Value {
        match self {
            Self::Hyperliquid | Self::Entropy | Self::Tradexyz => json!({
                "method": "subscribe", "subscription": {"type": "l2Book", "coin": market, "fast": true}}),
            Self::Lighter | Self::LighterRh => {
                json!({"type": "subscribe", "channel": format!("order_book/{market}")})
            }
            Self::Gate => {
                json!({"time": now, "channel": "futures.order_book", "event": "subscribe", "payload": [market, "20", "0"]})
            }
            Self::Arcus => {
                json!({"type": "subscribe", "channel": "l2Orderbook", "id": market, "nLevels": 100})
            }
            Self::Ondo => {
                json!({"op": "subscribe", "channel": "depthBooksPerps", "markets": [market], "limit": 100})
            }
        }
    }

    pub fn heartbeat(self, now: u64) -> Option<Value> {
        match self {
            Self::Hyperliquid | Self::Entropy | Self::Tradexyz => Some(json!({"method": "ping"})),
            Self::Lighter | Self::LighterRh => None,
            Self::Gate => Some(json!({"channel": "futures.ping", "time": now})),
            Self::Arcus => Some(json!({"type": "ping"})),
            Self::Ondo => Some(json!({"op": "ping"})),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_all_deployments_and_market_suffixes() {
        for (venue, symbol, data, expected) in [
            (
                Venue::Hyperliquid,
                "BTC",
                json!({"universe": [{"name": "BTC"}]}),
                "BTC",
            ),
            (
                Venue::Entropy,
                "SNDK",
                json!({"universe": [{"name": "io:SNDK"}]}),
                "io:SNDK",
            ),
            (
                Venue::Tradexyz,
                "SNDK",
                json!({"universe": [{"name": "xyz:SNDK"}]}),
                "xyz:SNDK",
            ),
            (
                Venue::Lighter,
                "BTC",
                json!({"order_books": [{"symbol": "BTC", "market_id": 7, "status": "active", "market_type": "spot"}, {"symbol": "BTC", "market_id": 0, "status": "active"}]}),
                "0",
            ),
            (
                Venue::LighterRh,
                "BTC",
                json!({"order_books": [{"symbol": "BTC", "market_id": "0", "status": "active"}]}),
                "0",
            ),
            (Venue::Gate, "BTC", json!({"name": "BTC_USDT"}), "BTC_USDT"),
            (
                Venue::Arcus,
                "SNDK",
                json!({"markets": [{"marketDisplayName": "SNDK-USD", "status": "ONLINE", "type": "PERPETUAL"}]}),
                "SNDK-USD",
            ),
            (
                Venue::Ondo,
                "SNDK",
                json!({"success": true, "result": {"perps": {"tradingPairs": [{"market": "SNDK-USD.P"}]}}}),
                "SNDK-USD.P",
            ),
        ] {
            assert_eq!(venue.resolve_market(symbol, &data).unwrap(), expected);
            assert!(venue.resolve_market("MISSING", &data).is_err());
            assert!(venue.resolve_market(symbol, &json!({})).is_err());
            assert!(venue.ws_url().starts_with("wss://"));
        }
        for (venue, market) in [
            (Venue::Gate, "BTC_USDT"),
            (Venue::Arcus, "SNDK-USD"),
            (Venue::Ondo, "SNDK-USD.P"),
        ] {
            assert_eq!(venue.symbol_market(market), market);
        }
    }

    #[test]
    fn rejects_unavailable_markets() {
        for (venue, data) in [
            (
                Venue::Hyperliquid,
                json!({"universe": [{"name": "BTC", "isDelisted": true}]}),
            ),
            (
                Venue::Lighter,
                json!({"order_books": [{"symbol": "BTC", "market_id": 0, "status": "inactive"}]}),
            ),
            (
                Venue::Gate,
                json!({"name": "BTC_USDT", "in_delisting": true}),
            ),
            (
                Venue::Arcus,
                json!({"markets": [{"marketDisplayName": "BTC-USD", "status": "OFFLINE", "type": "PERPETUAL"}]}),
            ),
            (
                Venue::Ondo,
                json!({"success": true, "result": {"perps": {"tradingPairs": [{"market": "BTC-USD.P", "tradingDisabled": true}]}}}),
            ),
        ] {
            assert!(venue.resolve_market("BTC", &data).is_err());
        }
    }
}
