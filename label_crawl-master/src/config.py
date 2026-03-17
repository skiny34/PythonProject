from nb_util.type_conversion import update_dict

config = {
    "USER_AGENT_LIST": [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    ],
    "SERVICE_CONFIG": {
        "type": "baidu_images"
    },
    "CHAIN_SOURCE":"eth",
    "MONGODB": {
        "tag": {
            "client": {
                "uri": "mongodb://label_expand:Vp11OwR9Fpn!CF0lekq@123.60.159.112:8635/test?authSource=admin&replicaSet=replica",
                # "uri": "mongodb://rwuser:haQUNSj7L16H9vHJ~@192.168.0.197:27017",
                "db": "tag_database"
            }
        },
        "tag_local": {
            "client": {
                "uri": "mongodb://rwuser:haQUNSj7L16H9vHJ~@192.168.0.197:27017",
                "db": "tag_database"
            }
        },
        "tag_algorithm": {
            "client": {
                "uri": "mongodb://label_expand:Vp11OwR9Fpn!CF0lekq@60.204.171.204:8635,121.36.214.159:8635/test?authSource=admin&replicaSet=replica"
            },
            "suspected_hot_wallet": {"db": "rule_expansion", "collection": "suspected_hot_wallet"}

        },
        "data": {
            "client": {
                "uri": "mongodb://label_expand:NjLNsxxdYR5QmB5T!vmCH@101.206.244.178:27017/test?authSource=admin&loadBalanced=false&directConnection=true"
            },
            "token": {
                "eth": {"db": "eth", "collection": "token_v3"},
                "bsc": {"db": "bsc", "collection": "token_v3"},
                "tron": {"db": "tron", "collection": "token_v3"},
                "polygon": {"db": "polygon", "collection": "token_v3"},
                "arbitrum": {"db": "arbitrum", "collection": "token_v3"},
                "optimism": {"db": "optimism", "collection": "token_v3"}
            },
        }

    },
    "OCR_API": "http://192.168.10.155:8888/ocr_text",
    "SPIDERS": {
        "oklink": {
            "address_tag": {
                'eth': 'https://www.oklink.com/zh-hant/ethereum/address/{address}',
                'bsc': 'https://www.oklink.com/zh-hant/bsc/address/{address}',
                'polygon': 'https://www.oklink.com/zh-hans/polygon/address/{address}',
                'arbitrum': 'https://www.oklink.com/zh-hans/arbitrum-one/address/{address}',
                'optimism': 'https://www.oklink.com/zh-hans/optimism/address/{address}',
                'avalanche': 'https://www.oklink.com/zh-hans/avax/address/{address}',
                'zksync': 'https://www.oklink.com/zh-hans/zksync-era/address/{address}',
                'tron': 'https://www.oklink.com/zh-hant/tron/address/{address}',
                'btc': 'https://www.oklink.com/zh-hans/btc/address/{address}',
                'solana': 'https://www.oklink.com/zh-hans/solana/address/{address}',
                'etc': 'https://www.oklink.com/zh-hans/etc/address/{address}',
                'base': 'https://www.oklink.com/zh-hans/base/address/{address}',
                'aptos': 'https://www.oklink.com/zh-hans/aptos/address/{address}',
            }
        },
        "scan": {
            "address_tag": {
                'eth': 'https://etherscan.io/address/{address}',
                'bsc': 'https://bscscan.com/address/{address}',
                'polygon': 'https://polygonscan.com/address/{address}',
                'arbitrum': 'https://bscscan.io/address/{address}',
                'avalanche': 'https://snowtrace.io/address/{address}',
                'tron': 'https://apilist.tronscanapi.com/api/multiple/chain/query?address={address}'
            }
        },
        "arkham": {
            "address_tag": {
                "url":"https://api.arkm.com/intelligence/address/{address}"
                # "eth": 'https://api.arkm.com/intelligence/address/{address}',
                # "bsc": "https://api.arkm.com/intelligence/address/{address}?chain=bsc",
                # "polygon": "https://api.arkm.com/intelligence/address/{address}?chain=polygon",
                # "tron": "https://api.arkm.com/intelligence/address/{address}?chain=tron",
                # "arbitrum": "https://api.arkm.com/intelligence/address/{address}?chain=arbitrum"
            },
            "transfer": {
                "url": "https://api.arkm.com/transfers"
            }

        },
        "chain_abuse": {
            "block_address": {
                "chains": ["btc", "eth", "bsc", "tron", "polygon", "arbitrum", "avalanche"],
                "url": "https://www.chainabuse.com/api/graphql-proxy",
                "report_count_query": "query GetChainFilterOptions($input: ChainsInput) {\n  chains(input: $input) {\n    kind\n    reportsFiledCount\n    __typename\n  }\n}\n",
                "report_query": "query GetReports($input: ReportsInput, $after: String, $before: String, $last: Float, $first: Float) {\n  reports(\n    input: $input\n    after: $after\n    before: $before\n    last: $last\n    first: $first\n  ) {\n    pageInfo {\n      hasNextPage\n      hasPreviousPage\n      startCursor\n      endCursor\n      __typename\n    }\n    edges {\n      cursor\n      node {\n        ...Report\n        __typename\n      }\n      __typename\n    }\n    count\n    totalCount\n    __typename\n  }\n}\n\nfragment Report on Report {\n  id\n  isPrivate\n  ...ReportPreviewDetails\n  ...ReportAccusedScammers\n  ...ReportAuthor\n  ...ReportAddresses\n  ...ReportEvidences\n  ...ReportCompromiseIndicators\n  ...ReportTokenIDs\n  ...ReportTransactionHashes\n  __typename\n}\n\nfragment ReportPreviewDetails on Report {\n  createdAt\n  scamCategory\n  categoryDescription\n  biDirectionalVoteCount\n  viewerDidVote\n  description\n  lexicalSerializedDescription\n  commentsCount\n  source\n  checked\n  __typename\n}\n\nfragment ReportAccusedScammers on Report {\n  accusedScammers {\n    id\n    info {\n      id\n      contact\n      type\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ReportAuthor on Report {\n  reportedBy {\n    id\n    username\n    trusted\n    __typename\n  }\n  __typename\n}\n\nfragment ReportAddresses on Report {\n  addresses {\n    id\n    address\n    chain\n    domain\n    label\n    __typename\n  }\n  __typename\n}\n\nfragment ReportEvidences on Report {\n  evidences {\n    id\n    description\n    photo {\n      id\n      name\n      description\n      url\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ReportCompromiseIndicators on Report {\n  compromiseIndicators {\n    id\n    type\n    value\n    __typename\n  }\n  __typename\n}\n\nfragment ReportTokenIDs on Report {\n  tokens {\n    id\n    tokenId\n    __typename\n  }\n  __typename\n}\n\nfragment ReportTransactionHashes on Report {\n  transactionHashes {\n    id\n    hash\n    chain\n    label\n    __typename\n  }\n  __typename\n}\n",
                "report_loss_query": "query GetLossesForReport($input: ReportInput!) {\n  report(input: $input) {\n    id\n    source\n    ...ReportLosses\n    __typename\n  }\n}\n"
            }
        },
        "merkle_science": {
            "address_tag": {
                "oauth_url": "https://accounts.merklescience.com/oauth/token",
                "oauth_params": {
                    "client_id": "1fL8KomYuWri6OPUmr4WSRXwu6KuwG16",
                    "redirect_uri": "https://tracker.app.merklescience.com",
                    "code_verifier": "KuenakBZPSgha-.DGuKMHyM-i7w8~sUHZKg4d44eu.K",
                    "code": "PbDuVT2L_dNSXbk_xgclEKvMl58XXQ4TEtmSBYKeJeA83",
                    "grant_type": "authorization_code"
                },
                "api_key":"",
                "url": "https://tracker.app.merklescience.com/api/bifrost/api/v2/unified_search/search/",
                "chain_conversion": {
                    "eth": "ethereum",
                    "base": "ethereum",
                    "polygon": "matic",
                    'tron': 'tron',
                    'bsc': 'bsc',
                    'bitcoin': 'btc',
                    'litecoin': 'ltc',
                    'zksync': 'ethereum',
                    'arbitrum': 'arbitrum',
                    'optimism': 'optimism',
                    'avalanche': 'avax',
                    'xrp': 'ripple',
                    'solana': 'sol',
                    'bitcoin_cash': 'bch',
                    'polkadot': 'dot',
                    'stellar': 'xlm',

                }
            }
        },
        "mist_track": {
            "address_tag": {
                "url": "https://dashboard.misttrack.io/api/v1/address_label_list?coin={coin}&address={address}",
                "chain_conversion": {
                    "eth": "ETH",
                    "bsc": "BNB",
                    "tron": "TRX",
                    "polygon": "POL",
                    "btc": "BTC",
                    "arbitrum": "ETH",
                    "avalanche": "AVAX",
                    "zksync": "ETH-zkSync",

                }
            }
        },
        "baidu": {
            "images_url": "https://graph.baidu.com/ajax/pcsimi",
            "session_id": "",
            "sign": "",
            "tk": "",
        },
        "btc_wallet_explorer": {
            "entity_url": "https://www.walletexplorer.com/",
            "wallet_api_url": "https://www.walletexplorer.com/api/1/wallet-addresses"
        }
    },
    "ES": {
        "client": {
            "hosts": ["http://nb-calculate-token-price:vGCRA9SWQdLxDye9N3Uq@101.206.244.178:19200"],
            "timeout": 600,

        }
    },
    "EVM_CHAINS": [
        "eth",
        "bsc",
        "polygon",
        "arbitrum",
        "optimism",
        "zksync",
        "klaytn",
        "avalanche",
        "hsk",
    ],
    "OBS": {
        "conn": {
            "connect": {
                "ak": "",
                "sk": "",
                "endpoint": "",
                "default_bucket": "",
            },
            "path": None,
        }

    }

}
try:
    from local_config import config as local_config

    config = update_dict(config, local_config)
except:
    pass
