import os
import re
import time
from collections import OrderedDict, namedtuple
from typing import List

import requests
from urllib.parse import urlparse
from tqdm import tqdm

Info = namedtuple('Info', 'tag value')


def download_image(image_url, save_dir='./images', filename=None):
    """
    下载网络图片到本地

    :param image_url: 图片的URL地址
    :param save_dir: 保存目录（默认当前目录下的images文件夹）
    :param filename: 自定义文件名（默认使用URL中的文件名）
    :return: 下载文件的完整路径
    """
    # 创建保存目录
    os.makedirs(save_dir, exist_ok=True)

    try:
        # 设置请求头模拟浏览器访问
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        # 发送HTTP GET请求
        response = requests.get(image_url, headers=headers, stream=True, timeout=10)
        response.raise_for_status()  # 检查请求是否成功

        # 确定文件名
        if not filename:
            # 从URL中提取文件名
            path = urlparse(image_url).path
            filename = os.path.basename(path) or 'downloaded_image.jpg'

        # 完整保存路径
        save_path = os.path.join(save_dir, filename)

        # 获取文件大小（用于显示进度）
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0

        progress_bar = tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc=f"下载 {os.path.basename(save_path)}",
            ascii=True,
            ncols=80  # 控制进度条宽度
        )

        # 记录开始时间
        start_time = time.time()

        # 写入文件
        # 下载并写入文件
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=total_size):
                file.write(chunk)
                progress_bar.update(len(chunk))

        # 关闭进度条
        progress_bar.close()

        # 计算下载速度和时间
        download_time = time.time() - start_time
        speed = total_size / download_time / (1024 * 1024)  # MB/s

        print(f"\n下载完成! 保存路径: {save_path}")
        print(f"文件大小: {total_size / (1024 * 1024):.2f} MB")
        print(f"下载耗时: {download_time:.2f} 秒")
        print(f"平均速度: {speed:.2f} MB/s")

        return save_path


    except requests.exceptions.RequestException as e:
        print(f"下载失败: {str(e)}")
        return None
    except Exception as e:
        print(f"发生未知错误: {str(e)}")
        return None


def find_all_images(directory, extensions=None, include_subdirs=True):
    """
    查找目录中的所有图片文件

    参数:
        directory: 要搜索的目录路径
        extensions: 指定要查找的图片扩展名列表（可选）
        include_subdirs: 是否包含子目录（默认True）

    返回:
        图片文件路径的列表
    """
    # 默认支持的图片格式
    default_extensions = [
        '.jpg', '.jpeg', '.png'
    ]

    # 如果没有指定扩展名，使用默认列表
    if extensions is not None:
        # 确保扩展名以点开头并转换为小写
        extensions = [ext.lower() if ext.startswith('.') else '.' + ext.lower()
                      for ext in extensions]
    else:
        extensions = default_extensions

    image_files = []

    # 遍历目录
    for root, dirs, files in os.walk(directory):
        if not include_subdirs and root != directory:
            continue

        for file in files:
            file_path = os.path.join(root, file)
            file_ext = os.path.splitext(file_path)[1].lower()

            # 检查扩展名
            if file_ext in extensions:
                image_files.append(file_path)

            # 额外检查没有扩展名或扩展名不匹配但实际上是图片的文件
            elif not file_ext or file_ext not in extensions:
                try:
                    # 使用imghdr检查文件类型
                    img_type = imghdr.what(file_path)
                    if img_type:
                        # imghdr返回的类型如'jpeg'、'png'等
                        if img_type in [ext.strip('.') for ext in extensions]:
                            image_files.append(file_path)
                except (IOError, OSError, PermissionError):
                    # 跳过无法访问的文件
                    continue

    return image_files


def parse(text: str, tag: str, patterns: List[re.Pattern]):
    results = []
    res = set()
    for _re in patterns:
        values = _re.findall(text)
        for v in values:
            if isinstance(v,tuple):
                if v[1].startswith('-'):
                    v = f"发送方{v[0]}"
                else:
                    v = f"接收方{v[0]}"

            res.add(v)
            # 每次正则匹配后都将已匹配的数据替换为标记，防止后续重复匹配
            text = text.replace(v, f"<{tag}>")

    for item in res:
        results.append(Info(tag=tag, value=item))

    return text, results


def fuzzy_match_addresses(text_str):
    """
    :param text_str:
    :return:
    """
    # 定义正则表达式模式

    from_to_patters = OrderedDict(
        {
            "from": [
                re.compile(r'(发送方|发款方|转账方|Payer|From|from|付款地址|充币地址)'),
            ],
            "to": [
                re.compile(r'(接收方|收款方|Payee|To|to|收款地址|钱包地址|提币地址)'),
            ]
        },

    )
    chain_patterns = OrderedDict({
        # 链地址

        "chain_address": [
            re.compile(
                r'(?:发送方|接收方|发款方|收款方|转账方|Payee|To|to|From|from|付款地址|钱包地址|充币地址|提币地址)[0O]x[0-9a-fA-F]{40}'),
            re.compile(
                r'(?:发送方|接收方|发款方|收款方|转账方|Payee|To|to|From|from|付款地址|钱包地址|充币地址|提币地址)T[1-9A-HJ-NP-Za-km-z]{33}'),
            re.compile(
                r'(?:发送方|接收方|发款方|收款方|转账方|Payee|To|to|From|from|付款地址|钱包地址|充币地址|提币地址)1[1-9A-HJ-NP-Za-km-z]{25,34}|3[1-9A-HJ-NP-Za-km-z]{25,34}|bc1[qpzry9x8gf2tvdw0s3jn54khce6mua7l]{39,59}|(?:p|q)[0-9a-hj-np-z]{41}'),
        ],

        # 链模糊地址
        "chain_fuzzy_address": [

            re.compile(r'(?:发送方|接收方|发款方|收款方|转账方|Payee|To|to|From|from|付款地址|充币地址|钱包地址|提币地址)[0O]x(?:[0-9a-fA-F]{6,20}[\.\*\s\…]+[0-9a-fA-F]{6,20})'),
            re.compile(r'(?:发送方|接收方|发款方|收款方|转账方|Payee|To|to|From|from|付款地址|充币地址|钱包地址|提币地址)[0O]x(?:[0-9a-fA-F]{2,20}[\.\*\s\…]+[0-9a-fA-F]{2,20})'),
            re.compile(r'(T[1-9A-HJ-NP-Za-km-z]{3,20}[\.\*\s\…]+[1-9A-HJ-NP-Za-km-z]+)\s*(-?\d+\.?\d*)\s*USDT'),
            re.compile(r'(?:发送方|接收方|发款方|收款方|转账方|Payee|To|to|From|from|付款地址|充币地址|钱包地址|提币地址)T(?:[1-9A-HJ-NP-Za-km-z]{6,20}[\.\*\s\…]+[1-9A-HJ-NP-Za-km-z]+)'),
            re.compile(r'(?:发送方|接收方|发款方|收款方|转账方|Payee|To|to|From|from|付款地址|充币地址|钱包地址|提币地址)T(?:[1-9A-HJ-NP-Za-km-z]{2,20}[\.\*\s\…]+[1-9A-HJ-NP-Za-km-z]+)'),
            re.compile(r'(?:发送方|接收方|发款方|收款方|转账方|Payee|To|to|From|from|付款地址|充币地址|钱包地址|提币地址)(?:1|3|bc1|[pq])(?:[1-9A-HJ-NP-Za-km-z][\*\.\s]*)+'),
        ],

    })
    results = []
    for tag, patterns in chain_patterns.items():
        text, _results = parse(text_str, tag, patterns)
        for r in _results:
            _from_or_ro = r.value
            for tag, d_patterns in from_to_patters.items():
                text, d_results = parse(_from_or_ro, tag, d_patterns)
                for d in d_results:
                    direction = d.tag
                    di = d.value
                    addr = _from_or_ro.split(di)[1]
                    results.append({
                        "fuzzy_addr": addr,
                        "direction": direction,
                    })
    return results


if __name__ == '__main__':
    string = [
        "TAeFJd..TCDA 20USDT2021-04-0601:04:21TSNnt...5USv9-51.266USDT2021-04-0601:02:18TTRUu...5ms5 100 USDT2021-04-0601:00:39THU2s...ArXTV-51.25 USDT2021-04-0600:59:21TTafN..11fsB0 20 USDT2021-04-0600:57:00TCT9Y...9ZeW-52.1992021-04-0600:50:03TH1Bv...6e6nL 20 USDT2021-04-0600:49:06THah2.x8xQ-82.292 USDT2021-04-0600:47:51壹米财经TEqTX..Ww8 T1w.cgym.com"]
    for _str in string:
        res = fuzzy_match_addresses(_str.strip())

        print(res)
