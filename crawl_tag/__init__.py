"""
Blockchain Monitor SDK - 区块链地址监控和数据收集SDK

该SDK提供以下功能:
1. 从多条区块链(EVM、TRON、Bitcoin、Solana)收集地址数据
2. 查询地址标签(ARKM、OKLink)
3. 定时调度收集任务
4. 导出CSV数据
"""

from .sdk import BlockchainMonitorSDK
from .config import SDKConfig
from .collectors import EVMCollector, TRONCollector, BitcoinCollector, SolanaCollector
from .labelers import ArkmLabeler, OklinkLabeler

__version__ = "1.1.0"
__all__ = [
    "BlockchainMonitorSDK",
    "SDKConfig",
    "EVMCollector",
    "TRONCollector",
    "BitcoinCollector",
    "SolanaCollector",
    "ArkmLabeler",
    "OklinkLabeler",
]
