"""
On-chain Governor Analytics via Etherscan API

Fetches contract deployment dates and first ProposalCreated events for known
DAO governor contracts (GovernorAlpha, GovernorBravo, OZ Governor variants).
Covers the pre-2022 era missing from Tally's API data.
"""

import json
import time
import requests
from datetime import datetime, timezone
from collections import defaultdict

ETHERSCAN_API_KEY = "ACHRSWAPDPPKRMB6JS1DRNK2PDHR3886IQ"
ETHERSCAN_BASE = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1  # Ethereum mainnet

# ProposalCreated event topic (same for GovernorAlpha, Bravo, and OZ Governor)
# keccak256("ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string)")
PROPOSAL_CREATED_TOPIC = "0x7d84a6263ae0d98d3329bd7b46bb4e8d6f98cd35a7adb45c274c8b7fd5ebd5e0"

# Known major DAO governor contracts on Ethereum mainnet
# Ordered by approximate deployment date
GOVERNOR_CONTRACTS = [
    # --- GovernorAlpha era (2020) ---
    {
        "dao": "Compound",
        "contract": "0xc0dA01a04C3f3E0be433606045bB7017A7323E38",
        "type": "GovernorAlpha",
        "chain": "mainnet",
    },
    {
        "dao": "Uniswap",
        "contract": "0x5e4be8Bc9637f0EAA1A755019e06A68ce081D58F",
        "type": "GovernorAlpha",
        "chain": "mainnet",
    },
    # --- GovernorBravo era (2021-2022) ---
    {
        "dao": "Compound",
        "contract": "0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
        "type": "GovernorBravo",
        "chain": "mainnet",
    },
    {
        "dao": "Uniswap",
        "contract": "0x408ED6354d4973f66138C91495F2f2FCbd8724C3",
        "type": "GovernorBravo",
        "chain": "mainnet",
    },
    {
        "dao": "Gitcoin",
        "contract": "0xDbD27635A534A3d3169Ef0498beB56Fb9c937489",
        "type": "GovernorBravo",
        "chain": "mainnet",
    },
    {
        "dao": "FEI Protocol",
        "contract": "0x0BEF27FEB58e857046d630B2c03dFb7bae567494",
        "type": "GovernorBravo",
        "chain": "mainnet",
    },
    {
        "dao": "Indexed Finance",
        "contract": "0x95129751769f99CC39824a0793eF4933DD8Bb74B",
        "type": "GovernorBravo",
        "chain": "mainnet",
    },
    # --- OZ Governor era (2021+) ---
    {
        "dao": "ENS",
        "contract": "0x323A76393544d5ecca80cd6ef2A560C6a395b7E3",
        "type": "OZGovernor",
        "chain": "mainnet",
    },
    {
        "dao": "Nouns DAO",
        "contract": "0x6f3E6272A167e8AcCb32072d08E0957F9c79223d",
        "type": "NounsGovernor",
        "chain": "mainnet",
    },
    {
        "dao": "PoolTogether",
        "contract": "0xB3a87172F555ae2a2AB79Be60B336D2F7D0187f0",
        "type": "OZGovernor",
        "chain": "mainnet",
    },
    {
        "dao": "Hop Protocol",
        "contract": "0xed8Bdb5895B8B7f9Fdb3C087628FD8410E853D48",
        "type": "OZGovernor",
        "chain": "mainnet",
    },
    {
        "dao": "Rari Capital",
        "contract": "0x637deEED4e4deb1D222650bD4B64192abf002c00",
        "type": "OZGovernor",
        "chain": "mainnet",
    },
    {
        "dao": "Tribe DAO",
        "contract": "0xE087F94c3081e1832dC7a22B48c6f2b5fAaE579B",
        "type": "OZGovernor",
        "chain": "mainnet",
    },
    {
        "dao": "Element Finance",
        "contract": "0x8F1B82c2C49b6cFeFCd2b5C4Ece5e7E2b1C7e3Fe",
        "type": "OZGovernor",
        "chain": "mainnet",
    },
]


def etherscan_get(params: dict, retries: int = 4) -> dict | None:
    """Call Etherscan V2 API with retry/backoff."""
    params["apikey"] = ETHERSCAN_API_KEY
    params["chainid"] = CHAIN_ID
    delay = 2
    for attempt in range(retries):
        try:
            resp = requests.get(ETHERSCAN_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") == "1" or data.get("message") == "OK":
                return data
            # Rate limit
            if "rate limit" in str(data.get("result", "")).lower():
                print(f"  Rate limited, waiting {delay}s...")
                time.sleep(delay)
                delay *= 2
                continue
            return data
        except Exception as e:
            print(f"  Request error: {e}, retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2
    return None


def get_contract_creation(address: str) -> dict | None:
    """Get contract creation info (block, timestamp, creator) via Etherscan V2."""
    data = etherscan_get({
        "module": "contract",
        "action": "getcontractcreation",
        "contractaddresses": address,
    })
    if data and data.get("result") and isinstance(data["result"], list):
        result = data["result"][0]
        return {
            "tx_hash": result.get("txHash"),
            "creator": result.get("contractCreator"),
            "block": int(result["blockNumber"]) if result.get("blockNumber") else None,
            "timestamp": int(result["timestamp"]) if result.get("timestamp") else None,
        }
    return None


def get_first_proposal_event(address: str) -> dict | None:
    """
    Get the first ProposalCreated event emitted by this governor contract.
    Uses fromBlock=0 to search from genesis, Etherscan returns earliest first.
    """
    data = etherscan_get({
        "module": "logs",
        "action": "getLogs",
        "address": address,
        "topic0": PROPOSAL_CREATED_TOPIC,
        "fromBlock": 0,
        "toBlock": "latest",
        "page": 1,
        "offset": 1,  # Only need the first one
    })
    if data and isinstance(data.get("result"), list) and len(data["result"]) > 0:
        log = data["result"][0]
        if not isinstance(log, dict):
            return None
        block_hex = log.get("blockNumber", "0x0")
        ts_hex = log.get("timeStamp", "0x0")
        return {
            "block": int(block_hex, 16),
            "timestamp": int(ts_hex, 16),
            "tx_hash": log.get("transactionHash"),
        }
    return None


def get_total_proposal_count(address: str) -> int:
    """Count all ProposalCreated events for this contract."""
    data = etherscan_get({
        "module": "logs",
        "action": "getLogs",
        "address": address,
        "topic0": PROPOSAL_CREATED_TOPIC,
        "fromBlock": 0,
        "toBlock": "latest",
        "page": 1,
        "offset": 1000,
    })
    if data and isinstance(data.get("result"), list):
        return len(data["result"])
    return 0


def ts_to_year(ts: int) -> int:
    return datetime.fromtimestamp(ts, tz=timezone.utc).year


def ts_to_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def main():
    print("=" * 60)
    print("On-chain Governor Analytics via Etherscan")
    print("=" * 60)

    results = []

    for gov in GOVERNOR_CONTRACTS:
        dao = gov["dao"]
        address = gov["contract"]
        gov_type = gov["type"]
        print(f"\n[{dao}] {gov_type} @ {address}")

        # 1. Contract deployment
        time.sleep(0.25)  # Respect rate limits
        creation = get_contract_creation(address)
        deploy_ts = None
        deploy_year = None
        deploy_block = None
        if creation and creation.get("timestamp"):
            deploy_ts = creation["timestamp"]
            deploy_block = creation["block"]
            deploy_year = ts_to_year(deploy_ts)
            print(f"  Deployed: {ts_to_date(deploy_ts)} (block {deploy_block})")

        # 2. First proposal event
        time.sleep(0.25)
        first_proposal = get_first_proposal_event(address)
        first_proposal_ts = None
        first_proposal_year = None
        first_proposal_date = None
        first_proposal_block = None
        if first_proposal:
            first_proposal_ts = first_proposal["timestamp"]
            first_proposal_year = ts_to_year(first_proposal_ts)
            first_proposal_date = ts_to_date(first_proposal_ts)
            first_proposal_block = first_proposal["block"]
            print(f"  First proposal: {first_proposal_date} (block {first_proposal_block})")
        else:
            print(f"  First proposal: NOT FOUND")

        # 3. Total proposals (capped at 1000 by Etherscan free tier)
        time.sleep(0.25)
        total = get_total_proposal_count(address)
        print(f"  Proposals found: {total}")

        results.append({
            "dao": dao,
            "contract_address": address,
            "governor_type": gov_type,
            "chain": gov["chain"],
            "deploy_date": ts_to_date(deploy_ts) if deploy_ts else None,
            "deploy_year": deploy_year,
            "deploy_block": deploy_block,
            "first_proposal_date": first_proposal_date,
            "first_proposal_year": first_proposal_year,
            "first_proposal_block": first_proposal_block,
            "proposal_count_onchain": total,
        })

    # --- Analytics ---
    print("\n" + "=" * 60)
    print("ANALYTICS")
    print("=" * 60)

    # Group by first proposal year (using DAO-level earliest across all contracts)
    dao_earliest = {}
    for r in results:
        dao = r["dao"]
        yr = r["first_proposal_year"]
        if yr is None:
            continue
        if dao not in dao_earliest or yr < dao_earliest[dao]:
            dao_earliest[dao] = yr

    # Count unique DAOs per year (by earliest governor deployment)
    daos_by_year = defaultdict(list)
    for dao, yr in dao_earliest.items():
        daos_by_year[yr].append(dao)

    # Also count contracts deployed per year
    contracts_by_deploy_year = defaultdict(list)
    for r in results:
        if r["deploy_year"]:
            contracts_by_deploy_year[r["deploy_year"]].append(r["contract_address"])

    years_stats = []
    all_years = sorted(set(list(daos_by_year.keys()) + list(contracts_by_deploy_year.keys())))
    for yr in all_years:
        daos = daos_by_year.get(yr, [])
        contracts = contracts_by_deploy_year.get(yr, [])
        total_proposals = sum(
            r["proposal_count_onchain"]
            for r in results
            if r["first_proposal_year"] == yr
        )
        stats = {
            "year": yr,
            "unique_daos_first_proposal": len(set(daos)),
            "governor_contracts_deployed": len(contracts),
            "total_proposals_onchain": total_proposals,
            "daos": sorted(set(daos)),
        }
        years_stats.append(stats)
        print(f"\n{yr}:")
        print(f"  DAOs with first proposal: {len(set(daos))} — {', '.join(sorted(set(daos)))}")
        print(f"  Governor contracts deployed: {len(contracts)}")
        print(f"  Total proposals (capped 1000/contract): {total_proposals}")

    # Build final output
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Etherscan API (on-chain events)",
        "note": "Proposal counts capped at 1000 per contract by Etherscan free tier getLogs",
        "contracts": results,
        "yearly_stats": years_stats,
        "dao_first_proposal_year": dao_earliest,
    }

    out_path = "onchain_governor_data.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved to {out_path}")
    print(f"Total contracts analyzed: {len(results)}")
    print(f"Total DAOs covered: {len(dao_earliest)}")


if __name__ == "__main__":
    main()
