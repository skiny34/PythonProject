"""
SDK配置模块
"""
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import json


@dataclass
class CollectorConfig:
    """收集器配置"""
    enabled: bool = True
    real_time_ratio: float = 0.3
    large_value_ratio: float = 0.4
    popular_contract_ratio: float = 0.3
    large_value_threshold: float = 10.0
    api_keys: Dict[str, str] = field(default_factory=dict)
    rpc_urls: Dict[str, str] = field(default_factory=dict)


@dataclass
class LabelerConfig:
    """标签器配置"""
    enabled: bool = True
    arkm_enabled: bool = True
    oklink_enabled: bool = True
    arkm_api_key: str = ""
    oklink_api_key: str = ""
    request_timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 5.0


@dataclass
class RateLimitConfig:
    """速率限制配置"""
    enabled: bool = True
    requests_per_second: float = 5.0
    burst_limit: int = 10
    cooldown_period: float = 60.0


@dataclass
class SchedulerConfig:
    """调度器配置"""
    enabled: bool = True
    interval_seconds: int = 3600
    max_addresses_per_run: int = 2000
    output_directory: str = "./output"
    output_filename_pattern: str = "addresses_{timestamp}.csv"


@dataclass
class LoggingConfig:
    """日志配置"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None
    max_file_size: int = 10 * 1024 * 1024
    backup_count: int = 5


@dataclass
class SDKConfig:
    """SDK主配置类"""
    total_addresses: int = 2000
    collector: CollectorConfig = field(default_factory=CollectorConfig)
    labeler: LabelerConfig = field(default_factory=LabelerConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    evm_chains: List[str] = field(default_factory=lambda: ["ethereum", "bsc", "polygon", "arbitrum", "optimism"])
    
    @classmethod
    def from_dict(cls, config_dict: Dict) -> "SDKConfig":
        """从字典创建配置"""
        config = cls()
        
        if "total_addresses" in config_dict:
            config.total_addresses = config_dict["total_addresses"]
        
        if "collector" in config_dict:
            collector_cfg = config_dict["collector"]
            config.collector = CollectorConfig(
                enabled=collector_cfg.get("enabled", True),
                real_time_ratio=collector_cfg.get("real_time_ratio", 0.3),
                large_value_ratio=collector_cfg.get("large_value_ratio", 0.4),
                popular_contract_ratio=collector_cfg.get("popular_contract_ratio", 0.3),
                large_value_threshold=collector_cfg.get("large_value_threshold", 10.0),
                api_keys=collector_cfg.get("api_keys", {}),
                rpc_urls=collector_cfg.get("rpc_urls", {}),
            )
        
        if "labeler" in config_dict:
            labeler_cfg = config_dict["labeler"]
            config.labeler = LabelerConfig(
                enabled=labeler_cfg.get("enabled", True),
                arkm_enabled=labeler_cfg.get("arkm_enabled", True),
                oklink_enabled=labeler_cfg.get("oklink_enabled", True),
                arkm_api_key=labeler_cfg.get("arkm_api_key", ""),
                oklink_api_key=labeler_cfg.get("oklink_api_key", ""),
                request_timeout=labeler_cfg.get("request_timeout", 30),
                max_retries=labeler_cfg.get("max_retries", 3),
                retry_delay=labeler_cfg.get("retry_delay", 5.0),
            )
        
        if "rate_limit" in config_dict:
            rate_cfg = config_dict["rate_limit"]
            config.rate_limit = RateLimitConfig(
                enabled=rate_cfg.get("enabled", True),
                requests_per_second=rate_cfg.get("requests_per_second", 5.0),
                burst_limit=rate_cfg.get("burst_limit", 10),
                cooldown_period=rate_cfg.get("cooldown_period", 60.0),
            )
        
        if "scheduler" in config_dict:
            scheduler_cfg = config_dict["scheduler"]
            config.scheduler = SchedulerConfig(
                enabled=scheduler_cfg.get("enabled", True),
                interval_seconds=scheduler_cfg.get("interval_seconds", 3600),
                max_addresses_per_run=scheduler_cfg.get("max_addresses_per_run", 2000),
                output_directory=scheduler_cfg.get("output_directory", "./output"),
                output_filename_pattern=scheduler_cfg.get("output_filename_pattern", "addresses_{timestamp}.csv"),
            )
        
        if "logging" in config_dict:
            logging_cfg = config_dict["logging"]
            config.logging = LoggingConfig(
                level=logging_cfg.get("level", "INFO"),
                format=logging_cfg.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
                file_path=logging_cfg.get("file_path"),
                max_file_size=logging_cfg.get("max_file_size", 10 * 1024 * 1024),
                backup_count=logging_cfg.get("backup_count", 5),
            )
        
        if "evm_chains" in config_dict:
            config.evm_chains = config_dict["evm_chains"]
        
        return config
    
    @classmethod
    def from_json(cls, json_path: str) -> "SDKConfig":
        """从JSON文件加载配置"""
        with open(json_path, "r", encoding="utf-8") as f:
            config_dict = json.load(f)
        return cls.from_dict(config_dict)
    
    @classmethod
    def from_env(cls) -> "SDKConfig":
        """从环境变量加载配置"""
        config = cls()
        
        if os.getenv("SDK_TOTAL_ADDRESSES"):
            config.total_addresses = int(os.getenv("SDK_TOTAL_ADDRESSES"))
        
        if os.getenv("ARKM_API_KEY"):
            config.labeler.arkm_api_key = os.getenv("ARKM_API_KEY")
        
        if os.getenv("OKLINK_API_KEY"):
            config.labeler.oklink_api_key = os.getenv("OKLINK_API_KEY")
        
        if os.getenv("ETHERSCAN_API_KEY"):
            config.collector.api_keys["etherscan"] = os.getenv("ETHERSCAN_API_KEY")
        
        if os.getenv("BSCSCAN_API_KEY"):
            config.collector.api_keys["bscscan"] = os.getenv("BSCSCAN_API_KEY")
        
        if os.getenv("POLYGONSCAN_API_KEY"):
            config.collector.api_keys["polygonscan"] = os.getenv("POLYGONSCAN_API_KEY")
        
        if os.getenv("TRON_API_KEY"):
            config.collector.api_keys["tron"] = os.getenv("TRON_API_KEY")
        
        if os.getenv("BTC_API_KEY"):
            config.collector.api_keys["bitcoin"] = os.getenv("BTC_API_KEY")
        
        return config
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "total_addresses": self.total_addresses,
            "collector": {
                "enabled": self.collector.enabled,
                "real_time_ratio": self.collector.real_time_ratio,
                "large_value_ratio": self.collector.large_value_ratio,
                "popular_contract_ratio": self.collector.popular_contract_ratio,
                "large_value_threshold": self.collector.large_value_threshold,
                "api_keys": self.collector.api_keys,
                "rpc_urls": self.collector.rpc_urls,
            },
            "labeler": {
                "enabled": self.labeler.enabled,
                "arkm_enabled": self.labeler.arkm_enabled,
                "oklink_enabled": self.labeler.oklink_enabled,
                "arkm_api_key": self.labeler.arkm_api_key,
                "oklink_api_key": self.labeler.oklink_api_key,
                "request_timeout": self.labeler.request_timeout,
                "max_retries": self.labeler.max_retries,
                "retry_delay": self.labeler.retry_delay,
            },
            "rate_limit": {
                "enabled": self.rate_limit.enabled,
                "requests_per_second": self.rate_limit.requests_per_second,
                "burst_limit": self.rate_limit.burst_limit,
                "cooldown_period": self.rate_limit.cooldown_period,
            },
            "scheduler": {
                "enabled": self.scheduler.enabled,
                "interval_seconds": self.scheduler.interval_seconds,
                "max_addresses_per_run": self.scheduler.max_addresses_per_run,
                "output_directory": self.scheduler.output_directory,
                "output_filename_pattern": self.scheduler.output_filename_pattern,
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
                "file_path": self.logging.file_path,
                "max_file_size": self.logging.max_file_size,
                "backup_count": self.logging.backup_count,
            },
            "evm_chains": self.evm_chains,
        }
