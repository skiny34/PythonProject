"""
日志配置模块
"""
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str = "BlockchainMonitorSDK",
    level: str = "INFO",
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    file_path: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """
    配置并返回日志记录器
    
    Args:
        name: 日志记录器名称
        level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: 日志格式
        file_path: 日志文件路径 (可选)
        max_file_size: 日志文件最大大小 (字节)
        backup_count: 保留的日志文件数量
        
    Returns:
        配置好的日志记录器
    """
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
    
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    formatter = logging.Formatter(log_format)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if file_path:
        log_path = Path(file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


class LoggerAdapter:
    """日志适配器，提供额外的上下文信息"""
    
    def __init__(self, logger: logging.Logger, context: dict = None):
        self.logger = logger
        self.context = context or {}
    
    def _format_message(self, msg: str) -> str:
        if self.context:
            context_str = " | ".join(f"{k}={v}" for k, v in self.context.items())
            return f"[{context_str}] {msg}"
        return msg
    
    def debug(self, msg: str, *args, **kwargs):
        self.logger.debug(self._format_message(msg), *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs):
        self.logger.info(self._format_message(msg), *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs):
        self.logger.warning(self._format_message(msg), *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs):
        self.logger.error(self._format_message(msg), *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs):
        self.logger.critical(self._format_message(msg), *args, **kwargs)
    
    def exception(self, msg: str, *args, **kwargs):
        self.logger.exception(self._format_message(msg), *args, **kwargs)
    
    def with_context(self, **kwargs) -> "LoggerAdapter":
        """创建带有额外上下文的日志适配器"""
        new_context = {**self.context, **kwargs}
        return LoggerAdapter(self.logger, new_context)
