"""
CyberJustice Law PDF Crawler
从 https://cyberjustice.law/cases 获取所有PDF文件

命名规范:
- 文件名格式: {案例标题}_{PDF原始文件名}.pdf
- 案例标题: 从案例页面提取，清理非法字符
- PDF原始文件名: 从PDF下载链接中提取的原始文件名
- 如果同一案例有多个PDF，使用序号区分: {案例标题}_{序号}_{PDF原始文件名}.pdf
"""

import os
import re
import time
import requests
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CyberJusticePDFCrawler:
    BASE_URL = "https://cyberjustice.law"
    CASES_URL = "https://cyberjustice.law/cases"
    
    def __init__(self, output_dir: str = "downloaded_pdfs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        })
        self.downloaded_files = []
        
    def sanitize_filename(self, filename: str) -> str:
        illegal_chars = r'[<>:"/\\|?*\x00-\x1f]'
        filename = re.sub(illegal_chars, '_', filename)
        filename = re.sub(r'\s+', ' ', filename).strip()
        filename = re.sub(r'_+', '_', filename)
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:200-len(ext)] + ext
        return filename
    
    def extract_case_title(self, soup: BeautifulSoup) -> str:
        title_selectors = [
            ('h1', {}),
            ('.case-title', {}),
            ('.entry-title', {}),
            ('title', {}),
        ]
        
        for selector, attrs in title_selectors:
            element = soup.find(selector, attrs) if isinstance(selector, str) else soup.find(selector, **attrs)
            if element:
                title = element.get_text(strip=True)
                if title and title != "Page Not Found":
                    return self.sanitize_filename(title)
        
        return "unknown_case"
    
    def extract_pdf_links(self, soup: BeautifulSoup, page_url: str) -> list:
        pdf_links = []
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            absolute_url = urljoin(page_url, href)
            
            if '.pdf' in absolute_url.lower():
                pdf_name = self.extract_pdf_filename(absolute_url)
                link_text = link.get_text(strip=True)
                
                pdf_links.append({
                    'url': absolute_url,
                    'filename': pdf_name,
                    'link_text': link_text
                })
        
        for iframe in soup.find_all('iframe', src=True):
            src = iframe.get('src', '')
            absolute_url = urljoin(page_url, src)
            
            if '.pdf' in absolute_url.lower():
                pdf_name = self.extract_pdf_filename(absolute_url)
                pdf_links.append({
                    'url': absolute_url,
                    'filename': pdf_name,
                    'link_text': ''
                })
        
        for embed in soup.find_all('embed', src=True):
            src = embed.get('src', '')
            absolute_url = urljoin(page_url, src)
            
            if '.pdf' in absolute_url.lower():
                pdf_name = self.extract_pdf_filename(absolute_url)
                pdf_links.append({
                    'url': absolute_url,
                    'filename': pdf_name,
                    'link_text': ''
                })
        
        seen_urls = set()
        unique_pdf_links = []
        for pdf in pdf_links:
            if pdf['url'] not in seen_urls:
                seen_urls.add(pdf['url'])
                unique_pdf_links.append(pdf)
        
        return unique_pdf_links
    
    def extract_pdf_filename(self, url: str) -> str:
        parsed = urlparse(url)
        path = unquote(parsed.path)
        filename = os.path.basename(path)
        
        if not filename or filename == '':
            filename = f"document_{int(time.time())}.pdf"
        elif not filename.lower().endswith('.pdf'):
            filename = f"{filename}.pdf"
        
        return self.sanitize_filename(filename)
    
    def generate_output_filename(self, case_title: str, pdf_info: dict, index: int, total: int) -> str:
        case_title = self.sanitize_filename(case_title)
        pdf_filename = pdf_info['filename']
        
        if total > 1:
            output_name = f"{case_title}_{index+1:02d}_{pdf_filename}"
        else:
            output_name = f"{case_title}_{pdf_filename}"
        
        return self.sanitize_filename(output_name)
    
    def download_pdf(self, url: str, output_path: Path) -> bool:
        try:
            logger.info(f"正在下载: {url}")
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"下载完成: {output_path}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"下载失败 {url}: {e}")
            return False
    
    def get_case_links(self) -> list:
        logger.info(f"正在获取案例列表: {self.CASES_URL}")
        
        try:
            response = self.session.get(self.CASES_URL, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            case_links = []
            
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                absolute_url = urljoin(self.BASE_URL, href)
                
                if absolute_url.startswith(self.BASE_URL) and '/cases/' in absolute_url:
                    if absolute_url != self.CASES_URL:
                        title = link.get_text(strip=True)
                        case_links.append({
                            'url': absolute_url,
                            'title': title
                        })
            
            seen_urls = set()
            unique_case_links = []
            for case in case_links:
                if case['url'] not in seen_urls:
                    seen_urls.add(case['url'])
                    unique_case_links.append(case)
            
            logger.info(f"找到 {len(unique_case_links)} 个案例链接")
            return unique_case_links
            
        except requests.exceptions.RequestException as e:
            logger.error(f"获取案例列表失败: {e}")
            return []
    
    def process_case(self, case_info: dict) -> list:
        case_url = case_info['url']
        logger.info(f"正在处理案例: {case_url}")
        
        try:
            response = self.session.get(case_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            if "Page Not Found" in soup.get_text():
                logger.warning(f"页面不存在: {case_url}")
                return []
            
            case_title = self.extract_case_title(soup)
            pdf_links = self.extract_pdf_links(soup, case_url)
            
            if not pdf_links:
                logger.info(f"案例 '{case_title}' 没有找到PDF文件")
                return []
            
            logger.info(f"案例 '{case_title}' 找到 {len(pdf_links)} 个PDF文件")
            
            downloaded = []
            for idx, pdf_info in enumerate(pdf_links):
                output_filename = self.generate_output_filename(
                    case_title, pdf_info, idx, len(pdf_links)
                )
                output_path = self.output_dir / output_filename
                
                if output_path.exists():
                    logger.info(f"文件已存在，跳过: {output_path}")
                    downloaded.append(str(output_path))
                    continue
                
                if self.download_pdf(pdf_info['url'], output_path):
                    downloaded.append(str(output_path))
                    self.downloaded_files.append({
                        'case_title': case_title,
                        'pdf_url': pdf_info['url'],
                        'local_path': str(output_path),
                        'original_filename': pdf_info['filename']
                    })
                
                time.sleep(1)
            
            return downloaded
            
        except requests.exceptions.RequestException as e:
            logger.error(f"处理案例失败 {case_url}: {e}")
            return []
    
    def run(self):
        logger.info("=" * 60)
        logger.info("开始爬取 CyberJustice Law PDF 文件")
        logger.info("=" * 60)
        
        case_links = self.get_case_links()
        
        if not case_links:
            logger.warning("没有找到任何案例链接")
            return
        
        logger.info(f"共找到 {len(case_links)} 个案例，开始处理...")
        
        total_downloaded = 0
        for idx, case_info in enumerate(case_links):
            logger.info(f"\n处理进度: [{idx+1}/{len(case_links)}]")
            downloaded = self.process_case(case_info)
            total_downloaded += len(downloaded)
            time.sleep(2)
        
        logger.info("\n" + "=" * 60)
        logger.info("爬取完成!")
        logger.info(f"共下载 {total_downloaded} 个PDF文件")
        logger.info(f"文件保存目录: {self.output_dir.absolute()}")
        logger.info("=" * 60)
        
        return self.downloaded_files


def main():
    crawler = CyberJusticePDFCrawler(output_dir="downloaded_pdfs")
    crawler.run()


if __name__ == "__main__":
    main()
