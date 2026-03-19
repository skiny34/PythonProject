"""
PDF Address Extractor - 完整版
从PDF文件中提取区块链地址，支持表格提取
只从包含address/wallet的列提取，排除transaction id列
处理多行表头和断裂地址的情况
排除64位hash（Transaction ID）
"""

import os
import re
import csv
import logging
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
import pdfplumber

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AddressExtractor:
    ADDRESS_PATTERNS = {
        'ethereum': {
            'pattern': r'0x[a-fA-F0-9]{40}',
            'name': 'Ethereum/EVM'
        },
        'bitcoin_p2pkh': {
            'pattern': r'\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b',
            'name': 'Bitcoin (P2PKH/P2SH)'
        },
        'bitcoin_bech32': {
            'pattern': r'\bbc1[a-zA-Z0-9]{39,59}\b',
            'name': 'Bitcoin (Bech32)'
        },
        'tron': {
            'pattern': r'\bT[A-Za-z1-9]{33}\b',
            'name': 'Tron'
        },
    }
    
    ADDRESS_COLUMN_KEYWORDS = ['address', 'wallet', 'destination']
    EXCLUDE_COLUMN_KEYWORDS = ['transaction id', 'txid', 'tx id', 'transaction']
    
    def __init__(self, pdf_dir: str = "downloaded_pdfs", output_csv: str = "extracted_addresses.csv"):
        self.pdf_dir = Path(pdf_dir)
        self.output_csv = output_csv
        
    def is_valid_eth_address(self, addr: str) -> bool:
        if addr.startswith('0x'):
            addr = addr[2:]
        if len(addr) != 40:
            return False
        if not re.match(r'^[a-fA-F0-9]{40}$', addr):
            return False
        if addr.lower() == '0' * 40:
            return False
        return True
    
    def normalize_eth_address(self, addr: str) -> str:
        addr = addr.lower()
        if not addr.startswith('0x'):
            addr = '0x' + addr
        return addr
    
    def fix_broken_address(self, text: str) -> str:
        text = re.sub(r'[\r\n]+', '', text)
        text = re.sub(r'\s+', '', text)
        return text
    
    def find_address_columns_in_header(self, header: List) -> List[int]:
        address_cols = []
        exclude_cols = []
        
        if not header:
            return address_cols, exclude_cols
        
        for i, cell in enumerate(header):
            if not cell or not isinstance(cell, str):
                continue
            cell_lower = cell.lower()
            
            is_address_col = any(kw in cell_lower for kw in self.ADDRESS_COLUMN_KEYWORDS)
            is_exclude_col = any(kw in cell_lower for kw in self.EXCLUDE_COLUMN_KEYWORDS)
            
            if is_address_col and not is_exclude_col:
                address_cols.append(i)
            if is_exclude_col:
                exclude_cols.append(i)
        
        return address_cols, exclude_cols
    
    def find_address_columns_in_data(self, table: List[List], exclude_cols: List[int]) -> List[int]:
        address_cols = set()
        
        if len(table) < 2:
            return list(address_cols)
        
        for row in table[1:]:
            for i, cell in enumerate(row):
                if i in exclude_cols:
                    continue
                if not cell or not isinstance(cell, str):
                    continue
                
                cell_clean = self.fix_broken_address(cell)
                
                # 检查是否包含完整的40位地址（带或不带0x前缀）
                if re.search(r'(?:0x)?[a-fA-F0-9]{40}', cell_clean):
                    address_cols.add(i)
        
        return list(address_cols)
    
    def extract_address_from_cell(self, cell: str) -> Set[str]:
        addresses = set()
        
        if not cell or not isinstance(cell, str):
            return addresses
        
        cell_clean = self.fix_broken_address(cell)
        
        # 提取带0x前缀的40位地址
        pattern = r'0x[a-fA-F0-9]{40}'
        matches = re.findall(pattern, cell_clean, re.IGNORECASE)
        for m in matches:
            if self.is_valid_eth_address(m):
                addresses.add(self.normalize_eth_address(m))
        
        # 提取不带0x前缀的40位hex字符串
        pattern = r'(?<![a-fA-F0-9])([a-fA-F0-9]{40})(?![a-fA-F0-9])'
        matches = re.findall(pattern, cell_clean)
        for m in matches:
            if self.is_valid_eth_address(m):
                addresses.add(self.normalize_eth_address(m))
        
        return addresses
    
    def is_valid_table(self, table: List[List]) -> bool:
        if not table or len(table) < 1:
            return False
        
        header = table[0]
        if not header:
            return False
        
        header_text = ' '.join([str(h) for h in header if h])
        header_lower = header_text.lower()
        
        # 必须包含address关键字
        has_address = any(kw in header_lower for kw in self.ADDRESS_COLUMN_KEYWORDS)
        
        return has_address
    
    def has_address_data(self, table: List[List]) -> bool:
        """检查表格是否包含地址数据（用于跨页表格）"""
        if not table or len(table) < 1:
            return False
        
        for row in table:
            for cell in row:
                if not cell or not isinstance(cell, str):
                    continue
                cell_clean = self.fix_broken_address(cell)
                # 检查是否包含40位地址
                if re.search(r'(?:0x)?[a-fA-F0-9]{40}', cell_clean):
                    return True
        
        return False
    
    def extract_from_table(self, table: List[List], prev_header: List = None) -> Set[str]:
        addresses = set()
        
        if not table or len(table) < 1:
            return addresses
        
        header = None
        data_start = 0
        
        # 检查第一行是否是表头
        if self.is_valid_table(table):
            header = table[0]
            data_start = 1
        elif prev_header:
            # 使用前一页的表头（跨页表格）
            header = prev_header
            data_start = 0
        elif self.has_address_data(table):
            # 没有表头但有地址数据，直接从数据中提取
            for row in table:
                for cell in row:
                    cell_addrs = self.extract_address_from_cell(cell)
                    addresses.update(cell_addrs)
            return addresses
        else:
            return addresses
        
        header_address_cols, exclude_cols = self.find_address_columns_in_header(header)
        
        # 从数据行中找出包含地址的列（排除Transaction ID列）
        data_address_cols = self.find_address_columns_in_data(table[data_start:], exclude_cols)
        
        # 合并表头和数据行中的地址列
        all_cols = list(set(header_address_cols + data_address_cols))
        
        if not all_cols:
            return addresses
        
        for row in table[data_start:]:
            for col_idx in all_cols:
                if col_idx < len(row):
                    cell = row[col_idx]
                    cell_addrs = self.extract_address_from_cell(cell)
                    addresses.update(cell_addrs)
        
        return addresses
    
    def extract_from_text(self, text: str) -> Set[str]:
        addresses = set()
        
        text_clean = re.sub(r'[\r\n\t]+', ' ', text)
        
        pattern = self.ADDRESS_PATTERNS['ethereum']['pattern']
        matches = re.findall(pattern, text_clean, re.IGNORECASE)
        for m in matches:
            if self.is_valid_eth_address(m):
                addresses.add(self.normalize_eth_address(m))
        
        return addresses
    
    def extract_bitcoin_from_text(self, text: str) -> Set[str]:
        addresses = set()
        text_clean = re.sub(r'[\r\n\t]+', ' ', text)
        
        for key in ['bitcoin_p2pkh', 'bitcoin_bech32']:
            pattern = self.ADDRESS_PATTERNS[key]['pattern']
            matches = re.findall(pattern, text_clean)
            addresses.update(matches)
        
        return addresses
    
    def extract_tron_from_text(self, text: str) -> Set[str]:
        text_clean = re.sub(r'[\r\n\t]+', ' ', text)
        pattern = self.ADDRESS_PATTERNS['tron']['pattern']
        matches = re.findall(pattern, text_clean)
        return set(matches)
    
    def detect_chain(self, address: str) -> str:
        if address.startswith('0x') and len(address) == 42:
            return 'Ethereum/EVM'
        elif address.startswith('T') and len(address) == 34:
            return 'Tron'
        elif address.startswith('1') or address.startswith('3'):
            return 'Bitcoin'
        elif address.startswith('bc1'):
            return 'Bitcoin (Bech32)'
        return 'Unknown'
    
    def process_pdf(self, pdf_path: Path) -> List[Dict]:
        addresses = []
        seen_in_file = set()
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                full_text = ""
                prev_header = None
                
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    full_text += page_text + "\n"
                    
                    tables = page.extract_tables()
                    for table in tables:
                        # 检查是否是新表格（有表头）还是跨页表格
                        if self.is_valid_table(table):
                            # 新表格，保存表头供后续使用
                            prev_header = table[0]
                        
                        table_addrs = self.extract_from_table(table, prev_header)
                        for addr in table_addrs:
                            if addr not in seen_in_file:
                                seen_in_file.add(addr)
                                addresses.append({
                                    'filename': pdf_path.name,
                                    'address': addr,
                                    'chain': self.detect_chain(addr),
                                    'type': 'EVM',
                                    'source': 'table'
                                })
                
                eth_addrs = self.extract_from_text(full_text)
                for addr in eth_addrs:
                    if addr not in seen_in_file:
                        seen_in_file.add(addr)
                        addresses.append({
                            'filename': pdf_path.name,
                            'address': addr,
                            'chain': self.detect_chain(addr),
                            'type': 'EVM',
                            'source': 'text'
                        })
                
                btc_addrs = self.extract_bitcoin_from_text(full_text)
                for addr in btc_addrs:
                    if addr not in seen_in_file:
                        seen_in_file.add(addr)
                        addresses.append({
                            'filename': pdf_path.name,
                            'address': addr,
                            'chain': self.detect_chain(addr),
                            'type': 'Bitcoin',
                            'source': 'text'
                        })
                
                tron_addrs = self.extract_tron_from_text(full_text)
                for addr in tron_addrs:
                    if addr not in seen_in_file:
                        seen_in_file.add(addr)
                        addresses.append({
                            'filename': pdf_path.name,
                            'address': addr,
                            'chain': self.detect_chain(addr),
                            'type': 'Tron',
                            'source': 'text'
                        })
                
        except Exception as e:
            logger.error(f"处理PDF失败 {pdf_path}: {e}")
        
        return addresses
    
    def deduplicate_addresses(self, addresses: List[Dict]) -> List[Dict]:
        unique = []
        seen = set()
        
        for addr in addresses:
            key = (addr['filename'], addr['address'])
            if key not in seen:
                seen.add(key)
                unique.append(addr)
        
        return unique
    
    def run(self):
        logger.info("=" * 60)
        logger.info("开始提取PDF中的区块链地址")
        logger.info("=" * 60)
        
        pdf_files = list(self.pdf_dir.glob("*.pdf"))
        logger.info(f"找到 {len(pdf_files)} 个PDF文件")
        
        all_addresses = []
        
        for idx, pdf_path in enumerate(pdf_files):
            logger.info(f"处理进度: [{idx+1}/{len(pdf_files)}] - {pdf_path.name}")
            addresses = self.process_pdf(pdf_path)
            all_addresses.extend(addresses)
            logger.info(f"  提取到 {len(addresses)} 个地址")
        
        unique_addresses = self.deduplicate_addresses(all_addresses)
        
        logger.info(f"共提取 {len(all_addresses)} 个地址")
        logger.info(f"去重后 {len(unique_addresses)} 个唯一地址")
        
        self.save_to_csv(unique_addresses)
        
        return unique_addresses
    
    def save_to_csv(self, addresses: List[Dict]):
        if not addresses:
            logger.warning("没有地址需要保存")
            return
        
        with open(self.output_csv, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['filename', 'address', 'chain', 'type', 'source']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(addresses)
        
        logger.info(f"结果已保存到: {self.output_csv}")
        
        chain_stats = {}
        source_stats = {}
        for addr in addresses:
            chain = addr['chain']
            source = addr.get('source', 'unknown')
            chain_stats[chain] = chain_stats.get(chain, 0) + 1
            source_stats[source] = source_stats.get(source, 0) + 1
        
        logger.info("\n地址统计:")
        for chain, count in sorted(chain_stats.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {chain}: {count}")
        
        logger.info("\n来源统计:")
        for source, count in sorted(source_stats.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {source}: {count}")


def main():
    extractor = AddressExtractor(
        pdf_dir="downloaded_pdfs",
        output_csv="extracted_addresses.csv"
    )
    extractor.run()


if __name__ == "__main__":
    main()
