import requests
import uuid
import pandas as pd
import time
from typing import List, Dict
import os

# 全局配置
PROXY = {"http": "http://192.168.1.24:7988", "https": "http://192.168.1.24:7988"}
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
RETRY_DELAY = 2

# API端点
EXCHANGE_LISTING_URL = "https://api.coinmarketcap.com/data-api/v3.1/exchange/listing"
WALLETS_URL = "https://api.coinmarketcap.com/data-api/v3/exchange/reserves/wallets?id={exchange_id}"


def generate_request_headers() -> Dict[str, str]:
    """生成包含随机x-request-id的请求头"""
    return {
        "x-request-id": str(uuid.uuid4()).replace("-", ""),
        "platform": "web",
        "cache-control": "no-cache",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    }


def fetch_exchange_list(limit: int = 300) -> List[Dict[str, str]]: #int = 的值为top的交易所
    """获取交易所列表"""
    params = {
        "exType": 1,
        "sort": "rank",
        "direction": "desc",
        "start": 1,
        "limit": limit
    }

    try:
        response = requests.get(
            EXCHANGE_LISTING_URL,
            headers=generate_request_headers(),
            params=params,
            proxies=PROXY,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        return [
            {
                "rank": item["rank"],
                "exchange_name": item["name"],
                "slug": item["slug"],
                "exchange_id": item["id"]
            }
            for item in data.get("data", {}).get("exchanges", [])
        ]
    except Exception as e:
        print(f"获取交易所列表失败: {str(e)}")
        return []


def fetch_exchange_wallets(exchange_id: str, exchange_name: str) -> List[Dict]:
    """获取单个交易所的储备地址"""
    url = WALLETS_URL.format(exchange_id=exchange_id)
    wallets = []

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                url,
                headers=generate_request_headers(),
                proxies=PROXY,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()

            for wallet in data.get('data', {}).get('exchangeWallets', []):
                wallets.append({
                    "exchange_name": exchange_name,
                    "exchange_id": exchange_id,
                    "address": wallet.get('walletAddress', ''),
                    "chain": wallet.get('platformCryptoName', ''),
                    "token_name": wallet.get('name', '')
                })
            return wallets

        except Exception as e:
            print(f"获取 {exchange_name}(ID:{exchange_id}) 钱包数据失败，尝试 {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    return wallets


def process_all_exchanges(output_csv: str = "exchange_wallets.csv") -> None:
    # 第一步：获取交易所列表
    print("正在获取交易所列表...")
    exchanges = fetch_exchange_list()

    if not exchanges:
        print("无法获取交易所列表，程序终止")
        return

    print(f"成功获取 {len(exchanges)} 个交易所")
    print("\nTop 10 交易所:")
    for exchange in exchanges[:10]:
        print(f"{exchange['rank']:>3} | {exchange['exchange_name']:<20} | ID: {exchange['exchange_id']}")

    # 如果文件已存在，删除（确保不会重复写入旧数据）
    if os.path.exists(output_csv):
        os.remove(output_csv)

    # 第二步：逐个交易所处理，边爬边写入
    print("\n开始获取储备地址信息并写入文件...")
    total_wallets = 0
    for idx, exchange in enumerate(exchanges):
        exchange_id = str(exchange["exchange_id"])
        exchange_name = exchange["exchange_name"]

        print(f"\n[{idx+1}/{len(exchanges)}] 正在处理: {exchange_name} (ID: {exchange_id})")
        wallets = fetch_exchange_wallets(exchange_id, exchange_name)

        if wallets:
            print(f"找到 {len(wallets)} 个储备地址")

            df = pd.DataFrame(wallets)

            # 写入文件（首次写入包含列头，之后不包含）
            df.to_csv(output_csv, mode='a', header=not os.path.exists(output_csv), index=False)

            total_wallets += len(wallets)
        else:
            print("未找到储备地址")

        time.sleep(1)  # 避免频率限制

    print(f"\n所有数据已保存到 {output_csv}")
    print(f"共处理 {len(exchanges)} 个交易所，找到 {total_wallets} 个储备地址")


if __name__ == "__main__":
    process_all_exchanges()