"""
Blockchain Monitor SDK 主模块

提供统一的SDK接口，整合数据收集、标签查询和输出功能
"""
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
import os

from .config import SDKConfig
from .base import AddressInfo, AddressType, BlockchainType
from .collectors import EVMCollector, TRONCollector, BitcoinCollector
from .labelers import ArkmLabeler, OklinkLabeler
from .utils import RateLimiter, setup_logger, CSVWriter


class BlockchainMonitorSDK:
    """
    区块链监控SDK主类
    
    整合以下功能:
    - 多链地址数据收集 (EVM、TRON、Bitcoin)
    - 地址标签查询 (ARKM、OKLink)
    - 定时调度
    - CSV数据输出
    """
    
    def __init__(
        self,
        config: Optional[SDKConfig] = None,
        config_dict: Optional[Dict] = None,
        config_json: Optional[str] = None,
    ):
        """
        初始化SDK
        
        Args:
            config: SDKConfig配置对象
            config_dict: 配置字典
            config_json: 配置JSON文件路径
        """
        if config:
            self.config = config
        elif config_dict:
            self.config = SDKConfig.from_dict(config_dict)
        elif config_json:
            self.config = SDKConfig.from_json(config_json)
        else:
            self.config = SDKConfig.from_env()
        
        self.logger = setup_logger(
            name="BlockchainMonitorSDK",
            level=self.config.logging.level,
            log_format=self.config.logging.format,
            file_path=self.config.logging.file_path,
            max_file_size=self.config.logging.max_file_size,
            backup_count=self.config.logging.backup_count,
        )
        
        self.rate_limiter = RateLimiter(
            requests_per_second=self.config.rate_limit.requests_per_second,
            burst_limit=self.config.rate_limit.burst_limit,
            cooldown_period=self.config.rate_limit.cooldown_period,
        ) if self.config.rate_limit.enabled else None
        
        self.collectors = self._init_collectors()
        self.labelers = self._init_labelers()
        self.csv_writer = CSVWriter(
            output_directory=self.config.scheduler.output_directory,
            filename_pattern=self.config.scheduler.output_filename_pattern,
        )
        
        self._running = False
        self._scheduler_task = None
        self._on_collection_complete: Optional[Callable] = None
    
    def _init_collectors(self) -> Dict[str, Any]:
        """初始化收集器"""
        collectors = {}
        
        if self.config.collector.enabled:
            collectors["evm"] = EVMCollector(self.config, self.logger, self.rate_limiter)
            collectors["tron"] = TRONCollector(self.config, self.logger, self.rate_limiter)
            collectors["bitcoin"] = BitcoinCollector(self.config, self.logger, self.rate_limiter)
        
        return collectors
    
    def _init_labelers(self) -> Dict[str, Any]:
        """初始化标签查询器"""
        labelers = {}
        
        if self.config.labeler.enabled:
            if self.config.labeler.arkm_enabled:
                labelers["arkm"] = ArkmLabeler(self.config, self.logger, self.rate_limiter)
            if self.config.labeler.oklink_enabled:
                labelers["oklink"] = OklinkLabeler(self.config, self.logger, self.rate_limiter)
        
        return labelers
    
    async def collect_addresses(
        self,
        total_count: Optional[int] = None,
        real_time_ratio: Optional[float] = None,
        large_value_ratio: Optional[float] = None,
        popular_contract_ratio: Optional[float] = None,
        large_value_threshold: Optional[float] = None,
    ) -> List[AddressInfo]:
        """
        收集地址数据
        
        Args:
            total_count: 总地址数量
            real_time_ratio: 实时交易地址比例
            large_value_ratio: 大额交易地址比例
            popular_contract_ratio: 热门合约交互地址比例
            large_value_threshold: 大额阈值
            
        Returns:
            地址信息列表
        """
        total_count = total_count or self.config.total_addresses
        real_time_ratio = real_time_ratio or self.config.collector.real_time_ratio
        large_value_ratio = large_value_ratio or self.config.collector.large_value_ratio
        popular_contract_ratio = popular_contract_ratio or self.config.collector.popular_contract_ratio
        large_value_threshold = large_value_threshold or self.config.collector.large_value_threshold
        
        self.logger.info(f"Starting address collection, target: {total_count} addresses")
        
        all_addresses = []
        
        evm_count = total_count // 3
        tron_count = total_count // 3
        bitcoin_count = total_count - evm_count - tron_count
        
        tasks = []
        
        if "evm" in self.collectors:
            tasks.append(self._collect_from_collector(
                "evm", evm_count, real_time_ratio, large_value_ratio, 
                popular_contract_ratio, large_value_threshold
            ))
        
        if "tron" in self.collectors:
            tasks.append(self._collect_from_collector(
                "tron", tron_count, real_time_ratio, large_value_ratio,
                popular_contract_ratio, large_value_threshold
            ))
        
        if "bitcoin" in self.collectors:
            tasks.append(self._collect_from_collector(
                "bitcoin", bitcoin_count, real_time_ratio, large_value_ratio,
                popular_contract_ratio, large_value_threshold
            ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Collection task failed: {result}")
            else:
                all_addresses.extend(result)
        
        unique_addresses = self._deduplicate_addresses(all_addresses)
        
        self.logger.info(f"Collected {len(unique_addresses)} unique addresses")
        
        return unique_addresses[:total_count]
    
    async def _collect_from_collector(
        self,
        collector_name: str,
        count: int,
        real_time_ratio: float,
        large_value_ratio: float,
        popular_contract_ratio: float,
        large_value_threshold: float,
    ) -> List[AddressInfo]:
        """从指定收集器收集地址"""
        collector = self.collectors.get(collector_name)
        if not collector:
            return []
        
        try:
            return await collector.collect_all(
                total_count=count,
                real_time_ratio=real_time_ratio,
                large_value_ratio=large_value_ratio,
                popular_contract_ratio=popular_contract_ratio,
                large_value_threshold=large_value_threshold,
            )
        except Exception as e:
            self.logger.error(f"Failed to collect from {collector_name}: {e}")
            return []
    
    def _deduplicate_addresses(self, addresses: List[AddressInfo]) -> List[AddressInfo]:
        """去重地址列表"""
        seen = set()
        unique = []
        
        for addr in addresses:
            if addr.address not in seen:
                seen.add(addr.address)
                unique.append(addr)
        
        return unique
    
    async def query_labels(
        self,
        addresses: List[AddressInfo],
    ) -> List[AddressInfo]:
        """
        查询地址标签
        
        Args:
            addresses: 地址信息列表
            
        Returns:
            带有标签的地址信息列表
        """
        self.logger.info(f"Querying labels for {len(addresses)} addresses")
        
        label_tasks = []
        
        if "arkm" in self.labelers:
            label_tasks.append(("arkm", self._query_labels_from_labeler("arkm", addresses)))
        
        if "oklink" in self.labelers:
            label_tasks.append(("oklink", self._query_labels_from_labeler("oklink", addresses)))
        
        for labeler_name, task in label_tasks:
            try:
                labels = await task
                
                for addr in addresses:
                    label = labels.get(addr.address)
                    if labeler_name == "arkm":
                        addr.arkm_label = label
                    elif labeler_name == "oklink":
                        addr.oklink_label = label
                        
            except Exception as e:
                self.logger.error(f"Failed to query labels from {labeler_name}: {e}")
        
        return addresses
    
    async def _query_labels_from_labeler(
        self,
        labeler_name: str,
        addresses: List[AddressInfo],
    ) -> Dict[str, Optional[str]]:
        """从指定标签查询器查询标签"""
        labeler = self.labelers.get(labeler_name)
        if not labeler:
            return {}
        
        address_list = [
            {"address": addr.address, "chain": addr.chain}
            for addr in addresses
        ]
        
        return await labeler.get_labels_batch(address_list)
    
    async def collect_and_label(
        self,
        total_count: Optional[int] = None,
    ) -> List[Dict]:
        """
        收集地址并查询标签
        
        Args:
            total_count: 总地址数量
            
        Returns:
            带有标签的地址数据列表
        """
        addresses = await self.collect_addresses(total_count)
        
        if self.labelers:
            addresses = await self.query_labels(addresses)
        
        return [addr.to_dict() for addr in addresses]
    
    def export_to_csv(
        self,
        addresses: List[Dict],
        output_path: Optional[str] = None,
    ) -> str:
        """
        导出数据到CSV文件
        
        Args:
            addresses: 地址数据列表
            output_path: 输出文件路径
            
        Returns:
            输出文件路径
        """
        return self.csv_writer.write_addresses(addresses, output_path)
    
    async def run_once(
        self,
        output_path: Optional[str] = None,
    ) -> str:
        """
        执行一次完整的收集和导出流程
        
        Args:
            output_path: 输出文件路径
            
        Returns:
            输出文件路径
        """
        self.logger.info("Starting one-time collection run")
        
        addresses = await self.collect_and_label()
        
        output_file = self.export_to_csv(addresses, output_path)
        
        self.logger.info(f"Collection complete, output saved to: {output_file}")
        
        if self._on_collection_complete:
            await self._on_collection_complete(addresses, output_file)
        
        return output_file
    
    def on_collection_complete(self, callback: Callable):
        """
        设置收集完成回调函数
        
        Args:
            callback: 回调函数，接收 (addresses, output_file) 参数
        """
        self._on_collection_complete = callback
    
    async def start_scheduler(
        self,
        interval_seconds: Optional[int] = None,
    ):
        """
        启动定时调度器
        
        Args:
            interval_seconds: 调度间隔(秒)
        """
        interval = interval_seconds or self.config.scheduler.interval_seconds
        
        self._running = True
        self.logger.info(f"Starting scheduler with interval: {interval} seconds")
        
        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                self.logger.error(f"Scheduler run failed: {e}")
            
            self.logger.info(f"Waiting {interval} seconds until next run...")
            await asyncio.sleep(interval)
    
    def stop_scheduler(self):
        """停止调度器"""
        self._running = False
        self.logger.info("Scheduler stopped")
    
    async def close(self):
        """关闭SDK，释放资源"""
        for collector in self.collectors.values():
            await collector.close()
        
        for labeler in self.labelers.values():
            await labeler.close()
        
        self.logger.info("SDK closed")
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.close()
    
    def get_stats(self) -> Dict:
        """
        获取SDK统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "config": self.config.to_dict(),
            "collectors": list(self.collectors.keys()),
            "labelers": list(self.labelers.keys()),
            "rate_limiter": {
                "available_tokens": self.rate_limiter.available_tokens if self.rate_limiter else None,
                "is_in_cooldown": self.rate_limiter.is_in_cooldown if self.rate_limiter else None,
            },
            "is_running": self._running,
        }
