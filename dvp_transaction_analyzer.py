"""
Arbitrum DVP Drop Transaction Analyzer

For each transaction hash in the input CSV, fetches on-chain data from
Arbitrum One via the Etherscan API to determine:
  - Which delegate lost voting power (from DelegateVotesChanged log)
  - How much DVP was lost (from the same log)
  - Who initiated the transaction
  - Where ARB tokens were transferred (if any)
  - A tag explaining the DVP drop

Usage:
    python dvp_transaction_analyzer.py <input_csv> [output_csv]

    input_csv  : CSV where the only required column is tx_hash.
                 Any extra columns (e.g. dvp_delta_arb, delegate_address
                 from your original pull) are passed through to the output.
    output_csv : optional; defaults to dvp_analysis_results.csv

Column name aliases accepted for tx_hash:
    transaction_hash, hash, txhash, tx

Tags produced:
    SOLD_CEX          ARB sent to a known centralised exchange
    SOLD_DEX          ARB swapped on a DEX (Uniswap, Camelot, Balancer …)
    SOLD_COWSWAP      ARB routed through CowSwap GPv2Settlement
    BRIDGE_OUT        ARB sent to a bridge (Hop, Stargate, Across …)
    VESTING_RELEASE   ARB transferred from a vesting contract
    UNDELEGATE_ONLY   User removed delegation without moving tokens
    REDELEGATE        Delegation moved to a different address
    MULTISIG_TRANSFER Gnosis Safe / multisig wallet involved
    DAO_INTERNAL      Arbitrum Foundation / DAO treasury movement
    WALLET_TRANSFER   Moved to an unrecognised wallet
    TX_FAILED         Transaction reverted
    FETCH_ERROR       Could not retrieve the receipt
    UNKNOWN           No ARB transfers or delegation events found

Output columns (appended to your input columns):
    block_number, initiator,
    delegate_address_onchain, dvp_delta_arb_onchain,
    arb_from, arb_to, arb_amount_arb,
    recipient_label, recipient_category,
    tag, confidence, notes
"""

import csv
import json
import sys
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

ETHERSCAN_API_KEY = "ACHRSWAPDPPKRMB6JS1DRNK2PDHR3886IQ"
ETHERSCAN_BASE    = "https://api.etherscan.io/v2/api"
CHAIN_ID          = 42161  # Arbitrum One

ARB_TOKEN = "0x912ce59144191c1204e64559fe8253a0e49e6548"  # lowercase

# ERC-20 / ERC20Votes event topic0 hashes
TRANSFER_TOPIC           = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
DELEGATE_CHANGED_TOPIC   = "0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f"
# keccak256("DelegateVotesChanged(address,uint256,uint256)")
DELEGATE_VOTES_CHG_TOPIC = "0x8e40b9b56e7a96c2f6d7e36bc6e7af2b7ff3a4e7bef5f6e24c3d7c06ad4c3e6"

RATE_LIMIT_DELAY   = 0.25   # seconds between Etherscan calls
CHECKPOINT_EVERY   = 10     # flush output every N transactions
ADDRESS_CACHE_FILE = "address_label_cache.json"

# ── Known Arbitrum One address labels ─────────────────────────────────────────
# Keys: lowercase 0x-prefixed address
# Values: (human_label, category)
# Categories: CEX | DEX | COWSWAP | BRIDGE | VESTING | DAO | GNOSIS_SAFE | UNKNOWN

KNOWN_ADDRESSES: dict[str, tuple[str, str]] = {
    # ── Centralised exchanges ──────────────────────────────────────────────
    # Binance
    "0xb38e8c17e38363af6ebdcb3dae12e0243582891d": ("Binance Hot Wallet 1", "CEX"),
    "0xe0f0cfde7ee664943906f17f7f14342e76a5cec1": ("Binance Hot Wallet 2", "CEX"),
    "0xf977814e90da44bfa03b6295a0616a897441acec": ("Binance Hot Wallet 3", "CEX"),
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": ("Binance Deposit", "CEX"),
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": ("Binance Exchange", "CEX"),
    "0xd551234ae421e3bcba99a0da6d736074f22192ff": ("Binance (Old)", "CEX"),
    "0x564286362092d8e7936f0549571a803b203aaced": ("Binance Hot Wallet", "CEX"),
    "0x0681d8db095565fe8a346fa0277bffde9c0edbbf": ("Binance Hot Wallet", "CEX"),
    "0xfe9e8709d3215310075d67e3ed32a380ccf451c8": ("Binance Hot Wallet", "CEX"),
    "0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67": ("Binance Hot Wallet", "CEX"),
    # Coinbase / Coinbase Prime
    "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": ("Coinbase Prime", "CEX"),
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": ("Coinbase", "CEX"),
    "0x503828976d22510aad0201ac7ec88293211d23da": ("Coinbase 2", "CEX"),
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": ("Coinbase 3", "CEX"),
    "0x3cd751e6b0078be393132286c442345e5dc49699": ("Coinbase 4", "CEX"),
    "0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511": ("Coinbase 5", "CEX"),
    "0xeb2629a2734e272bcc07bda959863f316f4bd4cf": ("Coinbase 6", "CEX"),
    "0x881d40237659c251811cec9c364ef91dc08d300c": ("Metamask Swap / Coinbase-related", "CEX"),
    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fd25a": ("OKX Exchange", "CEX"),
    "0x98ec059dc3adfbdd63429454aeb0c990fba4a128": ("OKX Hot Wallet", "CEX"),
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": ("OKX Wallet", "CEX"),
    "0xa7efae728d2936e78bda97dc267687568dd593f3": ("OKX Wallet 2", "CEX"),
    # Bybit
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": ("Bybit Hot Wallet", "CEX"),
    "0xd090e597f5b8d87e98a600e7c2c03d745feef35c": ("Bybit Deposit", "CEX"),
    # Kraken
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": ("Kraken Exchange", "CEX"),
    "0xcda68dfbe4c26a1ef8a9df81e4f9c96aa7e89ade": ("Kraken Hot Wallet", "CEX"),
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": ("Kraken 2", "CEX"),
    "0xe853c56864a2ebe4576a807d26fdc4a0ada51919": ("Kraken 3", "CEX"),
    # KuCoin
    "0x2b5634c42055806a59e9107ed44d43c426e99c00": ("KuCoin Hot Wallet", "CEX"),
    "0xa1d8d972560c2f8144af871db508f0b0b10a3fbf": ("KuCoin 2", "CEX"),
    "0xd6216fc19db775df9774a6e33526131da7d19a2c": ("KuCoin 3", "CEX"),
    # Gate.io
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": ("Gate.io Hot Wallet", "CEX"),
    "0x7793cd85c11a924478d358d49b05b37e91b5810f": ("Gate.io 2", "CEX"),
    # Bitget
    "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23": ("Bitget", "CEX"),
    "0x0639556f03714a74a5feeaf5736a4a64ff70d206": ("Bitget 2", "CEX"),
    # HTX (Huobi)
    "0xab5c66752a9e8167967685f1450532fb96d5d24f": ("HTX/Huobi Hot Wallet", "CEX"),
    "0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b": ("HTX/Huobi 2", "CEX"),
    "0xfdb16996831753d5331ff813c29a93c76834a0ad": ("HTX/Huobi 3", "CEX"),
    "0x1062a747393198f70f71ec65a582423dba7e5ab3": ("HTX/Huobi 4", "CEX"),
    # Crypto.com
    "0xcffad3200574698b78f32232aa9d63eabd290703": ("Crypto.com", "CEX"),
    # MEXC
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": ("MEXC Hot Wallet", "CEX"),

    # ── DEX routers & vaults (Arbitrum One) ──────────────────────────────
    # Uniswap V3
    "0xe592427a0aece92de3edee1f18e0157c05861564": ("Uniswap V3 SwapRouter", "DEX"),
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": ("Uniswap V3 SwapRouter 02", "DEX"),
    "0x5e325eda8064b456f4781070c0738d849c824258": ("Uniswap V3 Universal Router", "DEX"),
    "0x4648a43b2c14da09fdf82b161150d3f634f40491": ("Uniswap Universal Router", "DEX"),
    "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad": ("Uniswap Universal Router 2", "DEX"),
    # Camelot
    "0xc873fecbd354f5a56e00e710b90ef4201db2448d": ("Camelot V2 Router", "DEX"),
    "0x1f721e2e82f6676fce4ea07a5958cf098d339e18": ("Camelot V3 Router", "DEX"),
    # Balancer
    "0xba12222222228d8ba445958a75a0704d566bf2c8": ("Balancer Vault", "DEX"),
    # GMX
    "0xabbc5f99639c9b6bcb58544ddf04efa6802f4064": ("GMX Router", "DEX"),
    "0x09f77e8a13de9a35a7231028187e9fd5db8a2acb": ("GMX OrderBook", "DEX"),
    # Curve
    "0x1337bedc9d22ecbe766df105c9623922a27963ec": ("Curve 2pool", "DEX"),
    "0x960ea3e3c7fb317332d990873d354e18d7645590": ("Curve Tricrypto", "DEX"),
    "0xa5407eae9ba41422680e2e00537571bcc53efbfd": ("Curve sUSD", "DEX"),
    # SushiSwap
    "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506": ("SushiSwap Router", "DEX"),
    # Trader Joe
    "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7": ("Trader Joe Router", "DEX"),
    "0xb4315e873dbcf96ffd0acd8ea43f689d8c20fb30": ("Trader Joe V2 Router", "DEX"),
    # DODO
    "0xa867241cdc8d3b0c07c85cc06f25a0cd3b5474d8": ("DODO Proxy", "DEX"),
    # Pendle
    "0x888888888889758f76e7103c6cbf23abbf58f946": ("Pendle Router", "DEX"),
    # Paraswap
    "0xdef171fe48cf0115b1d80b88dc8eab59176fee57": ("Paraswap Router", "DEX"),
    # 1inch
    "0x1111111254eeb25477b68fb85ed929f73a960582": ("1inch V5 Router", "DEX"),
    "0x111111125421ca6dc452d289314280a0f8842a65": ("1inch V6 Router", "DEX"),
    # Odos
    "0xa669e7a0d4b3e4fa48af2de86bd4cd7126be4e13": ("Odos Router", "DEX"),
    "0x4e3288c9ca110bcc82bf38f09a7b425c095d92bf": ("Odos V2 Router", "DEX"),
    # KyberSwap
    "0x6131b5fae19ea4f9d964eac0408e4408b66337b5": ("KyberSwap Aggregator", "DEX"),
    "0xc1e624c810d297fd70ef53b0e08f44fabe468591": ("KyberSwap Router", "DEX"),
    # OpenOcean
    "0x6352a56caadC4F1E25CD6c75970Fa768A3304e64": ("OpenOcean Exchange", "DEX"),

    # ── CowSwap ───────────────────────────────────────────────────────────
    "0x9008d19f58aabd9ed0d60971565aa8510560ab41": ("CowSwap GPv2Settlement", "COWSWAP"),

    # ── Bridges ──────────────────────────────────────────────────────────
    # Arbitrum native bridge
    "0x0000000000000000000000000000000000000064": ("Arbitrum L2 ArbSys", "BRIDGE"),
    "0x5288c571fd7ad117bea99bf60fe0846c4e84f933": ("Arbitrum L2 Gateway Router", "BRIDGE"),
    "0x096760f208390250649e3e8763348e783aef5562": ("Arbitrum L2 ARB Gateway", "BRIDGE"),
    # Hop Protocol
    "0x33ceb27b39d2bb7d2e61f7564d3df29344020417": ("Hop Bridge ARB", "BRIDGE"),
    "0x3e4a3a4796d16c0cd582c382691998f7c06420b6": ("Hop AMM ARB", "BRIDGE"),
    # Stargate (LayerZero)
    "0x352d8275aae3e0c2404d9f68f6cee084b5beb3dd": ("Stargate Router ARB", "BRIDGE"),
    "0x53bf833a5d6c4dda888f69c22c88c9f356a41614": ("Stargate Router 2", "BRIDGE"),
    # Synapse
    "0x6f4e8eba4d337f874ab57478acc2cb5bacdc19c9": ("Synapse Bridge", "BRIDGE"),
    # Across
    "0xe35e9842fceaca96570b734083f4a58e8f7c5f2a": ("Across SpokePool ARB", "BRIDGE"),
    "0x09aea4b2242abc8bb4bb78d537a67a245a7bec64": ("Across SpokePool ARB v2", "BRIDGE"),
    # Connext
    "0xee9dec2712cce65174b561151701bf54b99c24c8": ("Connext ARB Diamond", "BRIDGE"),
    # Celer cBridge
    "0x1619de6b6b20ed217a58d00f37b9d47c7663feca": ("Celer cBridge", "BRIDGE"),
    # Multichain
    "0x650af55d5877f289837c30b94af91538a7504b76": ("Multichain Router", "BRIDGE"),
    # Orbiter Finance
    "0x80c67432656d59144ceff962e8faf8926599bcf8": ("Orbiter Finance Bridge", "BRIDGE"),

    # ── Arbitrum DAO / Foundation ─────────────────────────────────────────
    "0x67329e00f0fa2a30a5c6db95ec15d5f43fbbf4a0": ("Arbitrum Foundation Treasury", "DAO"),
    "0xf3fc178157fb3c87548baa86f9d24ba38e649b58": ("Arbitrum DAO Treasury", "DAO"),
    "0x789fc99093b09ad01c34dc7251d0c89ce743e5a4": ("Arbitrum DAO Core Governor", "DAO"),
    "0xf07ded9dc292157749b6fd268e37df6ea38395b9": ("Arbitrum DAO Treasury Governor", "DAO"),
    "0x1d62ffeaa6a143bf23d45dd2b1510e3b7c2e02c5": ("Arbitrum DAO Security Council", "DAO"),
    "0x423552c0f05baca3b5a5c3aa2e64d008e1b7f698": ("Arbitrum Foundation Multisig", "DAO"),
    "0x912ce59144191c1204e64559fe8253a0e49e6548": ("ARB Token Contract", "DAO"),

    # ── Known Arbitrum vesting / lockup contracts ─────────────────────────
    # These are the long-term unlock contracts created at token launch (Mar 2023)
    # If ARB transfers FROM these addresses, it's a vesting release
    "0xf3fc178157fb3c87548baa86f9d24ba38e649b58": ("Arbitrum Foundation Lockup", "VESTING"),
    "0x3c4461a2a44059e2adabacc8c2e72db34dff95f7": ("Offchain Labs Vesting Wallet", "VESTING"),
    "0x0e2dd55d3b87975cacc86e7ebfc9b53c8fc95555": ("Investor Vesting Wallet", "VESTING"),
}

# Addresses excluded from the DVP calculation per the Arbitrum DVP framework.
# Delegations to/from these addresses receive dedicated tags.
EXCLUDED_DELEGATE_ADDRESSES: set[str] = {
    "0x00000000000000000000000000000000000a4b86",
}

# Keyword patterns for contract-name–based categorisation (fallback via API)
CONTRACT_NAME_PATTERNS: list[tuple[str, str, str]] = [
    # (substring_lower, label_prefix, category)
    ("gnosis", "Gnosis Safe", "GNOSIS_SAFE"),
    ("safe",   "Gnosis Safe", "GNOSIS_SAFE"),
    ("multisig", "MultiSig Wallet", "GNOSIS_SAFE"),
    ("vesting", "Vesting Contract", "VESTING"),
    ("timelock", "Timelock Controller", "DAO"),
    ("governor", "Governor Contract", "DAO"),
    ("uniswap", "Uniswap", "DEX"),
    ("sushiswap", "SushiSwap", "DEX"),
    ("camelot", "Camelot", "DEX"),
    ("balancer", "Balancer", "DEX"),
    ("curve", "Curve", "DEX"),
    ("pendle", "Pendle", "DEX"),
    ("paraswap", "Paraswap", "DEX"),
    ("1inch", "1inch", "DEX"),
    ("kyber", "KyberSwap", "DEX"),
    ("router", "DEX Router", "DEX"),
    ("swap", "Swap Contract", "DEX"),
    ("exchange", "Exchange Contract", "DEX"),
    ("aggregator", "DEX Aggregator", "DEX"),
    ("settlement", "Settlement Contract", "COWSWAP"),
    ("cowswap", "CowSwap", "COWSWAP"),
    ("cow protocol", "CowSwap", "COWSWAP"),
    ("bridge", "Bridge Contract", "BRIDGE"),
    ("gateway", "Bridge Gateway", "BRIDGE"),
    ("stargate", "Stargate Bridge", "BRIDGE"),
    ("hop ", "Hop Bridge", "BRIDGE"),
    ("across", "Across Bridge", "BRIDGE"),
    ("synapse", "Synapse Bridge", "BRIDGE"),
    ("binance", "Binance", "CEX"),
    ("coinbase", "Coinbase", "CEX"),
    ("kraken", "Kraken", "CEX"),
    ("okx", "OKX", "CEX"),
    ("bybit", "Bybit", "CEX"),
    ("bitget", "Bitget", "CEX"),
    ("huobi", "Huobi/HTX", "CEX"),
    ("kucoin", "KuCoin", "CEX"),
]

# ── Column aliases ─────────────────────────────────────────────────────────────
# Only tx_hash is required; the rest are optional pass-through columns.

HASH_ALIASES = {"tx_hash", "transaction_hash", "hash", "txhash", "tx"}

# ── Etherscan helpers ─────────────────────────────────────────────────────────

def etherscan_get(params: dict, retries: int = 4) -> dict | None:
    """Call Etherscan V2 API with exponential backoff (2 / 4 / 8 / 16 s)."""
    params = {**params, "apikey": ETHERSCAN_API_KEY, "chainid": CHAIN_ID}
    delay = 2
    for attempt in range(retries):
        try:
            resp = requests.get(ETHERSCAN_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") == "1" or data.get("message") == "OK":
                return data
            if "rate limit" in str(data.get("result", "")).lower():
                print(f"  [rate-limit] waiting {delay}s …")
                time.sleep(delay)
                delay *= 2
                continue
            return data  # pass through even non-success for caller to inspect
        except Exception as exc:
            print(f"  [error] {exc}; retrying in {delay}s …")
            time.sleep(delay)
            delay *= 2
    return None


def get_tx_receipt(tx_hash: str) -> dict | None:
    """
    Fetch a full transaction receipt via eth_getTransactionReceipt proxy.
    Returns the raw result dict (from, to, status, blockNumber, logs, …).
    """
    data = etherscan_get({
        "module": "proxy",
        "action": "eth_getTransactionReceipt",
        "txhash": tx_hash,
    })
    if data and data.get("result"):
        return data["result"]
    return None


def get_contract_name(address: str, cache: dict) -> str | None:
    """
    Return a contract name for address, or None if it's an EOA or lookup fails.
    Results are cached in-memory and persisted to ADDRESS_CACHE_FILE.
    """
    addr_l = address.lower()
    if addr_l in cache:
        return cache[addr_l]
    time.sleep(RATE_LIMIT_DELAY)
    data = etherscan_get({
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
    })
    name = None
    if data and data.get("result") and isinstance(data["result"], list):
        src = data["result"][0]
        raw = src.get("ContractName", "").strip()
        if raw and raw != "0":
            name = raw
    cache[addr_l] = name  # None means EOA or unverified
    return name

# ── Event parsing ─────────────────────────────────────────────────────────────

def decode_address(topic: str) -> str:
    """Convert a padded 32-byte topic to a checksummed-ish 20-byte address."""
    return "0x" + topic[-40:].lower()


def parse_arb_transfers(logs: list) -> list[dict]:
    """
    Extract ARB token Transfer events from a receipt log list.
    Returns [{from, to, amount_wei}].
    """
    transfers = []
    for log in logs:
        if (log.get("address", "").lower() == ARB_TOKEN
                and log.get("topics")
                and log["topics"][0].lower() == TRANSFER_TOPIC):
            topics = log["topics"]
            if len(topics) < 3:
                continue
            from_addr = decode_address(topics[1])
            to_addr   = decode_address(topics[2])
            try:
                amount = int(log.get("data", "0x0"), 16)
            except ValueError:
                amount = 0
            transfers.append({"from": from_addr, "to": to_addr, "amount_wei": amount})
    return transfers


def parse_delegate_changed(logs: list) -> list[dict]:
    """
    Extract DelegateChanged events from ARB token logs.
    Returns [{delegator, from_delegate, to_delegate}].
    """
    events = []
    for log in logs:
        if (log.get("address", "").lower() == ARB_TOKEN
                and log.get("topics")
                and log["topics"][0].lower() == DELEGATE_CHANGED_TOPIC):
            topics = log["topics"]
            if len(topics) < 4:
                continue
            events.append({
                "delegator":     decode_address(topics[1]),
                "from_delegate": decode_address(topics[2]),
                "to_delegate":   decode_address(topics[3]),
            })
    return events


def parse_delegate_votes_changed(logs: list) -> list[dict]:
    """
    Extract DelegateVotesChanged events from ARB token logs.
    Returns [{delegate, previous_votes_wei, new_votes_wei, delta_wei}].

    Identified structurally (no keccak256 needed):
      - emitted by the ARB token contract
      - exactly 2 topics  (topics[0]=sig, topics[1]=delegate address, indexed)
      - data = 64 bytes   (previousVotes uint256 + newVotes uint256, non-indexed)
    This combination is unique to DelegateVotesChanged on the ARB token.
    """
    events = []
    for log in logs:
        if log.get("address", "").lower() != ARB_TOKEN:
            continue
        topics = log.get("topics", [])
        data   = log.get("data", "")
        # 2 topics + 64-byte data (128 hex chars + "0x" prefix = 130 chars)
        if len(topics) != 2 or len(data) != 130:
            continue
        delegate = decode_address(topics[1])
        data_hex = data[2:]
        try:
            prev_votes = int(data_hex[:64], 16)
            new_votes  = int(data_hex[64:], 16)
        except ValueError:
            continue
        events.append({
            "delegate":          delegate,
            "previous_votes_wei": prev_votes,
            "new_votes_wei":      new_votes,
            "delta_wei":          new_votes - prev_votes,  # negative = DVP loss
        })
    return events

# ── Address labelling ─────────────────────────────────────────────────────────

def lookup_label(
    address: str,
    name_cache: dict,
    use_api: bool = True,
) -> dict:
    """
    Return a label dict: {label, category, confidence}.
    confidence: HIGH (hardcoded) | MEDIUM (API contract name) | LOW (EOA / unknown)
    """
    addr_l = address.lower()

    # 1. Hardcoded dict — highest confidence
    if addr_l in KNOWN_ADDRESSES:
        lbl, cat = KNOWN_ADDRESSES[addr_l]
        return {"label": lbl, "category": cat, "confidence": "HIGH"}

    # 2. Contract name from Etherscan API
    if use_api:
        cname = get_contract_name(address, name_cache)
        if cname:
            cname_l = cname.lower()
            for substr, lbl_prefix, cat in CONTRACT_NAME_PATTERNS:
                if substr in cname_l:
                    return {
                        "label":      f"{lbl_prefix} ({cname})",
                        "category":   cat,
                        "confidence": "MEDIUM",
                    }
            # Verified contract but category unknown
            return {
                "label":      cname,
                "category":   "CONTRACT",
                "confidence": "MEDIUM",
            }
        else:
            # No source code → EOA or unverified contract
            return {"label": "Unknown EOA/Wallet", "category": "WALLET", "confidence": "LOW"}

    return {"label": "Unknown", "category": "UNKNOWN", "confidence": "LOW"}

# ── Tagging logic ─────────────────────────────────────────────────────────────

CATEGORY_TO_TAG = {
    "CEX":        "SOLD_CEX",
    "DEX":        "SOLD_DEX",
    "COWSWAP":    "SOLD_COWSWAP",
    "BRIDGE":     "BRIDGE_OUT",
    "VESTING":    "VESTING_RETURN",
    "DAO":        "DAO_INTERNAL",
    "GNOSIS_SAFE":"MULTISIG_TRANSFER",
    "CONTRACT":   "WALLET_TRANSFER",
    "WALLET":     "WALLET_TRANSFER",
    "UNKNOWN":    "WALLET_TRANSFER",
}


def tag_transaction(
    receipt: dict,
    transfers: list[dict],
    delegate_changed_events: list[dict],
    votes_changed: list[dict],
    name_cache: dict,
) -> dict:
    """
    Determine a single tag (and confidence / notes) for this transaction.
    Returns a dict with tag, confidence, notes, and a primary_transfer dict.
    """
    notes_parts = []
    initiator = receipt.get("from", "").lower()

    # ── Failed transaction ──────────────────────────────────────────────
    if receipt.get("status", "0x1") == "0x0":
        return {
            "tag": "TX_FAILED",
            "confidence": "HIGH",
            "notes": "Transaction reverted",
            "arb_from": "", "arb_to": "",
            "arb_amount_arb": "",
            "recipient_label": "",
            "recipient_category": "",
        }

    # ── Check if any transfer came FROM a vesting contract ─────────────
    for xfer in transfers:
        lbl = lookup_label(xfer["from"], name_cache, use_api=True)
        if lbl["category"] == "VESTING" or (
            lbl.get("label", "") and "vesting" in lbl["label"].lower()
        ):
            notes_parts.append(f"Vesting release from {xfer['from']} ({lbl['label']})")

    # ── ARB transfers exist in this tx ─────────────────────────────────
    if transfers:
        # Find the primary outgoing transfer (largest amount, or first)
        outgoing = sorted(transfers, key=lambda x: x["amount_wei"], reverse=True)
        primary = outgoing[0]

        to_label = lookup_label(primary["to"], name_cache, use_api=True)
        tag = CATEGORY_TO_TAG.get(to_label["category"], "WALLET_TRANSFER")

        # Special case: tokens sent to the ARB contract itself → unusual
        if primary["to"].lower() == ARB_TOKEN:
            tag = "DAO_INTERNAL"
            notes_parts.append("ARB sent to token contract itself")

        # Note if there were multiple transfers
        if len(transfers) > 1:
            extras = [f"{t['to']} ({int(t['amount_wei'])/1e18:.0f} ARB)" for t in outgoing[1:]]
            notes_parts.append(f"Additional transfers: {'; '.join(extras)}")

        # Note if the vesting note was added above
        if notes_parts:
            if tag == "WALLET_TRANSFER" and any("vesting" in n.lower() for n in notes_parts):
                tag = "VESTING_RELEASE"

        # Override: token transfer caused excluded address to lose delegation to an active delegate
        excluded_lost = any(
            e["delegate"].lower() in EXCLUDED_DELEGATE_ADDRESSES and e["delta_wei"] < 0
            for e in votes_changed
        )
        active_gained = any(
            e["delegate"].lower() not in EXCLUDED_DELEGATE_ADDRESSES and e["delta_wei"] > 0
            for e in votes_changed
        )
        if excluded_lost and active_gained:
            tag = "REDELEGATE_FROM_EXCLUDED"
            notes_parts.append(
                "Token transfer caused excluded address to lose delegation to an active delegate"
            )

        return {
            "tag":                tag,
            "confidence":         to_label["confidence"],
            "notes":              " | ".join(notes_parts) if notes_parts else "",
            "arb_from":           primary["from"],
            "arb_to":             primary["to"],
            "arb_amount_arb":     f"{int(primary['amount_wei']) / 1e18:.2f}",
            "recipient_label":    to_label["label"],
            "recipient_category": to_label["category"],
        }

    # ── No ARB transfers: check delegation events ───────────────────────
    if delegate_changed_events:
        for ev in delegate_changed_events:
            to_d      = ev["to_delegate"].lower()
            from_d    = ev["from_delegate"].lower()
            delegator = ev["delegator"].lower()
            zero = "0x" + "0" * 40
            if to_d in EXCLUDED_DELEGATE_ADDRESSES:
                tag = "DELEGATE_TO_EXCLUDED"
                notes_parts.append(
                    f"Delegator {ev['delegator']} delegated to excluded address {ev['to_delegate']}"
                )
            elif from_d in EXCLUDED_DELEGATE_ADDRESSES:
                if to_d != zero and to_d != delegator:
                    tag = "REDELEGATE_FROM_EXCLUDED"
                    notes_parts.append(
                        f"Delegator {ev['delegator']} moved delegation from excluded address "
                        f"{ev['from_delegate']} to {ev['to_delegate']}"
                    )
                else:
                    tag = "UNDELEGATE_FROM_EXCLUDED"
                    notes_parts.append(
                        f"Delegator {ev['delegator']} removed delegation from excluded address "
                        f"{ev['from_delegate']}"
                    )
            elif to_d == zero or to_d == delegator:
                tag = "UNDELEGATE_ONLY"
                notes_parts.append(
                    f"Delegator {ev['delegator']} removed delegation from {ev['from_delegate']}"
                )
            else:
                tag = "REDELEGATE"
                notes_parts.append(
                    f"Delegator {ev['delegator']} changed delegate "
                    f"from {ev['from_delegate']} to {ev['to_delegate']}"
                )
        return {
            "tag":                tag,
            "confidence":         "HIGH",
            "notes":              " | ".join(notes_parts),
            "arb_from":           "",
            "arb_to":             "",
            "arb_amount_arb":     "",
            "recipient_label":    "",
            "recipient_category": "",
        }

    # ── Fallback ────────────────────────────────────────────────────────
    return {
        "tag":                "UNKNOWN",
        "confidence":         "LOW",
        "notes":              "No ARB transfers or delegation events found in logs",
        "arb_from":           "",
        "arb_to":             "",
        "arb_amount_arb":     "",
        "recipient_label":    "",
        "recipient_category": "",
    }

# ── CSV helpers ───────────────────────────────────────────────────────────────

EXTRA_OUTPUT_COLS = [
    "block_number", "initiator",
    "delegate_address_onchain", "dvp_delta_arb_onchain",
    "arb_from", "arb_to", "arb_amount_arb",
    "recipient_label", "recipient_category",
    "tag", "confidence", "notes",
]


def detect_hash_column(header: list[str]) -> str:
    """
    Find which column in header holds the transaction hash.
    Raises ValueError if none found.
    """
    header_lower = {col.lower(): col for col in header}
    for alias in HASH_ALIASES:
        if alias in header_lower:
            return header_lower[alias]
    raise ValueError(
        f"Could not find a tx_hash column. "
        f"Tried: {HASH_ALIASES}. "
        f"Available columns: {header}"
    )


def load_checkpoint(output_path: Path) -> set[str]:
    """Return a set of tx_hashes already written to the output file."""
    done = set()
    if output_path.exists():
        with open(output_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("tx_hash"):
                    done.add(row["tx_hash"].lower())
    return done


def save_address_cache(cache: dict) -> None:
    with open(ADDRESS_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def load_address_cache() -> dict:
    p = Path(ADDRESS_CACHE_FILE)
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    input_path  = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("dvp_analysis_results.csv")

    if not input_path.exists():
        print(f"Error: input file not found: {input_path}")
        sys.exit(1)

    # ── Load state ──────────────────────────────────────────────────────
    name_cache  = load_address_cache()
    done_hashes = load_checkpoint(output_path)
    if done_hashes:
        print(f"Resuming: {len(done_hashes)} transactions already processed.")

    # ── Read input ──────────────────────────────────────────────────────
    with open(input_path, newline="") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)
        header = list(reader.fieldnames or [])

    col_hash = detect_hash_column(header)

    # Build output header: passthrough all input cols + new analysis cols,
    # avoiding duplicates if the user already has some of these column names.
    new_cols       = [c for c in EXTRA_OUTPUT_COLS if c not in header]
    out_fieldnames = header + new_cols

    # ── Open output (append if checkpoint exists) ───────────────────────
    file_mode = "a" if done_hashes else "w"
    write_hdr = not done_hashes
    out_file  = open(output_path, file_mode, newline="")
    writer    = csv.DictWriter(out_file, fieldnames=out_fieldnames, extrasaction="ignore")
    if write_hdr:
        writer.writeheader()

    total   = len(rows)
    skipped = 0
    errors  = 0

    try:
        for idx, row in enumerate(rows, 1):
            tx_hash = row[col_hash].strip()
            if not tx_hash:
                continue
            if tx_hash.lower() in done_hashes:
                skipped += 1
                continue

            print(f"[{idx}/{total}] {tx_hash[:20]}…", end="  ")

            # ── Fetch receipt ───────────────────────────────────────────
            time.sleep(RATE_LIMIT_DELAY)
            receipt = get_tx_receipt(tx_hash)
            if receipt is None:
                print("ERROR: receipt not found")
                out_row = dict(row)
                for c in new_cols:
                    out_row[c] = ""
                out_row["tag"]        = "FETCH_ERROR"
                out_row["confidence"] = "LOW"
                out_row["notes"]      = "Could not retrieve transaction receipt"
                writer.writerow(out_row)
                errors += 1
                continue

            # ── Parse all relevant events from logs ─────────────────────
            logs             = receipt.get("logs", [])
            transfers        = parse_arb_transfers(logs)
            delegate_changed = parse_delegate_changed(logs)
            votes_changed    = parse_delegate_votes_changed(logs)

            block_hex = receipt.get("blockNumber", "0x0")
            block_int = int(block_hex, 16) if block_hex else 0
            initiator = receipt.get("from", "").lower()

            # ── Extract on-chain delegate address and DVP delta ─────────
            # Use the DelegateVotesChanged event(s) to get ground truth.
            # If multiple delegates were affected (rare), take the one with
            # the largest absolute change.
            if votes_changed:
                primary_vc = max(votes_changed, key=lambda e: abs(e["delta_wei"]))
                delegate_onchain  = primary_vc["delegate"]
                dvp_delta_onchain = f"{primary_vc['delta_wei'] / 1e18:.2f}"
                if len(votes_changed) > 1:
                    others = [e["delegate"] for e in votes_changed if e != primary_vc]
                    # will be appended to notes below
            else:
                delegate_onchain  = ""
                dvp_delta_onchain = ""

            # ── Determine tag ────────────────────────────────────────────
            result = tag_transaction(receipt, transfers, delegate_changed, votes_changed, name_cache)

            # Append multi-delegate note if needed
            if votes_changed and len(votes_changed) > 1:
                others_str = "; ".join(
                    f"{e['delegate']} ({e['delta_wei']/1e18:.0f} ARB)"
                    for e in votes_changed if e != primary_vc
                )
                extra_note = f"Other delegates affected: {others_str}"
                result["notes"] = (result["notes"] + " | " + extra_note).lstrip(" | ")

            lost_addr   = delegate_onchain or "?"
            gained_addr = None
            if delegate_changed and result["tag"] in (
                "REDELEGATE", "DELEGATE_TO_EXCLUDED", "REDELEGATE_FROM_EXCLUDED"
            ):
                gained_addr = delegate_changed[0]["to_delegate"]
            # For REDELEGATE_FROM_EXCLUDED via token transfer (no DelegateChanged event),
            # derive gained address from votes_changed
            if not gained_addr and result["tag"] == "REDELEGATE_FROM_EXCLUDED" and votes_changed:
                gained_addr = next(
                    (e["delegate"] for e in votes_changed
                     if e["delegate"].lower() not in EXCLUDED_DELEGATE_ADDRESSES
                     and e["delta_wei"] > 0),
                    None,
                )
            addr_part = f"lost={lost_addr}"
            if gained_addr:
                addr_part += f"  →  gained={gained_addr}"
            print(
                f"  {addr_part}  "
                f"delta={dvp_delta_onchain or '?':>14} ARB  "
                f"tag={result['tag']:28s}  conf={result['confidence']}"
            )

            # ── Write output row ─────────────────────────────────────────
            out_row = dict(row)
            out_row["block_number"]            = block_int
            out_row["initiator"]               = initiator
            out_row["delegate_address_onchain"] = delegate_onchain
            out_row["dvp_delta_arb_onchain"]   = dvp_delta_onchain
            out_row["arb_from"]                = result["arb_from"]
            out_row["arb_to"]                  = result["arb_to"]
            out_row["arb_amount_arb"]          = result["arb_amount_arb"]
            out_row["recipient_label"]         = result["recipient_label"]
            out_row["recipient_category"]      = result["recipient_category"]
            out_row["tag"]                     = result["tag"]
            out_row["confidence"]              = result["confidence"]
            out_row["notes"]                   = result["notes"]
            writer.writerow(out_row)

            # Periodic flush + cache save
            if idx % CHECKPOINT_EVERY == 0:
                out_file.flush()
                save_address_cache(name_cache)

    finally:
        out_file.flush()
        out_file.close()
        save_address_cache(name_cache)

    print(f"\nDone.  Processed={total - skipped - errors}  "
          f"Skipped(checkpoint)={skipped}  Errors={errors}")
    print(f"Output → {output_path}")
    print(f"Address cache → {ADDRESS_CACHE_FILE}  ({len(name_cache)} entries)")


if __name__ == "__main__":
    main()
