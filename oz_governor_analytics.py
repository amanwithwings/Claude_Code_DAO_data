#!/usr/bin/env python3
"""
OpenZeppelin Governor Analytics via Tally API
Fetches all OZ Governor DAOs (organizations) and analyzes creation year
(proxied by first on-chain proposal date) and proposal activity.
"""

import requests
import json
from datetime import datetime
from collections import defaultdict
import time

TALLY_URL = "https://api.tally.xyz/query"
API_KEY = "8e15078bcf09864ce8a19d74c964e945eca0fe0ec7cd8a02d39e17381b4585e9"
HEADERS = {"Api-Key": API_KEY}


def gql(query, variables=None, retries=5):
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    for attempt in range(retries):
        try:
            resp = requests.post(TALLY_URL, json=payload, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                # Non-fatal GraphQL error (e.g. missing data for one org)
                return None
            return data.get("data")
        except Exception as e:
            wait = 2 ** attempt
            if attempt < retries - 1:
                print(f"\n  Retrying in {wait}s ({e})...")
                time.sleep(wait)
            else:
                print(f"\n  Failed after {retries} attempts: {e}")
                return None


# ---------------------------------------------------------------------------
# Step 1: Fetch all organizations with their proposalsCounts
# ---------------------------------------------------------------------------

ORG_QUERY = """
query GetOrgs($afterCursor: String) {
  organizations(input: {
    page: { limit: 20, afterCursor: $afterCursor },
    sort: { sortBy: id, isDescending: false }
  }) {
    nodes {
      ... on Organization {
        id
        name
        slug
        chainIds
        proposalsCount
      }
    }
    pageInfo { lastCursor }
  }
}
"""


def fetch_all_orgs():
    all_orgs = []
    cursor = None
    print("Fetching all organizations from Tally...")
    while True:
        variables = {}
        if cursor:
            variables["afterCursor"] = cursor
        data = gql(ORG_QUERY, variables)
        if not data:
            print(f"\n  Failed at cursor={cursor}, stopping.")
            break
        nodes = data["organizations"]["nodes"]
        cursor = data["organizations"]["pageInfo"]["lastCursor"]
        all_orgs.extend(nodes)
        print(f"  Fetched {len(all_orgs):,} orgs...", end="\r")
        if len(nodes) < 20:
            break
        time.sleep(0.3)  # be kind to the API
    print(f"\nTotal organizations fetched: {len(all_orgs):,}")
    return all_orgs


# ---------------------------------------------------------------------------
# Step 2: For each org with proposals, fetch its first proposal's createdAt
#         (used as proxy for "when the DAO was created / became active")
# ---------------------------------------------------------------------------

FIRST_PROPOSAL_QUERY = """
query FirstProposal($orgId: IntID!) {
  proposals(input: {
    filters: { organizationId: $orgId, isDraft: false },
    page: { limit: 1 },
    sort: { sortBy: id, isDescending: false }
  }) {
    nodes {
      ... on Proposal {
        id
        createdAt
      }
    }
  }
}
"""


def get_first_proposal_year(org_id):
    """Returns the year of the first on-chain proposal, or None on failure."""
    data = gql(FIRST_PROPOSAL_QUERY, {"orgId": org_id})
    if not data:
        return None
    nodes = data.get("proposals", {}).get("nodes", [])
    if not nodes:
        return None
    created_at = nodes[0].get("createdAt")
    if not created_at:
        return None
    # createdAt is an ISO 8601 string
    try:
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        return dt.year
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Step 3: Analyze
# ---------------------------------------------------------------------------

def analyze(orgs_with_year):
    year_counts = defaultdict(int)
    year_p1 = defaultdict(int)
    year_p5 = defaultdict(int)
    year_p10 = defaultdict(int)

    for org, year in orgs_with_year:
        pc = org.get("proposalsCount", 0)
        year_counts[year] += 1
        if pc >= 1:
            year_p1[year] += 1
        if pc >= 5:
            year_p5[year] += 1
        if pc >= 10:
            year_p10[year] += 1

    return {
        "year_counts": dict(sorted(year_counts.items())),
        "year_p1": dict(sorted(year_p1.items())),
        "year_p5": dict(sorted(year_p5.items())),
        "year_p10": dict(sorted(year_p10.items())),
        "total_p1": sum(year_p1.values()),
        "total_p5": sum(year_p5.values()),
        "total_p10": sum(year_p10.values()),
    }


def print_report(total_orgs, orgs_with_year, analysis):
    n = len(orgs_with_year)
    print("\n" + "=" * 70)
    print("  OPENZEPPELIN GOVERNOR DAOs — TALLY ANALYTICS REPORT")
    print("=" * 70)
    print(f"\nTotal organizations on Tally:         {total_orgs:,}")
    print(f"With a resolvable first-proposal year: {n:,}")
    print(f"  With >= 1  proposal:  {analysis['total_p1']:,}  ({analysis['total_p1']/n*100:.1f}% of dated)")
    print(f"  With >= 5  proposals: {analysis['total_p5']:,}  ({analysis['total_p5']/n*100:.1f}% of dated)")
    print(f"  With >= 10 proposals: {analysis['total_p10']:,}  ({analysis['total_p10']/n*100:.1f}% of dated)")
    print()
    print("Note: 'Year' = year of first on-chain proposal (proxy for DAO creation year).")
    print()
    print("-" * 70)
    print(f"{'Year':<8} {'Total':>8} {'>=1 Prop':>10} {'>=5 Props':>10} {'>=10 Props':>11}")
    print("-" * 70)
    for year in sorted(analysis["year_counts"].keys()):
        nt = analysis["year_counts"].get(year, 0)
        p1 = analysis["year_p1"].get(year, 0)
        p5 = analysis["year_p5"].get(year, 0)
        p10 = analysis["year_p10"].get(year, 0)
        print(f"{year:<8} {nt:>8,} {p1:>10,} {p5:>10,} {p10:>11,}")
    print("-" * 70)
    yc = analysis["year_counts"]
    print(f"{'TOTAL':<8} {sum(yc.values()):>8,} "
          f"{analysis['total_p1']:>10,} "
          f"{analysis['total_p5']:>10,} "
          f"{analysis['total_p10']:>11,}")
    print("=" * 70)


def main():
    # Step 1: all orgs
    all_orgs = fetch_all_orgs()
    if not all_orgs:
        print("No organizations fetched. Exiting.")
        return

    # Step 2: get first-proposal year for orgs with proposals
    orgs_with_proposals = [o for o in all_orgs if o.get("proposalsCount", 0) >= 1]
    print(f"\n{len(orgs_with_proposals):,} orgs have >= 1 proposal; fetching first proposal dates...")

    orgs_with_year = []
    failed = 0
    for i, org in enumerate(orgs_with_proposals):
        year = get_first_proposal_year(org["id"])
        if year:
            orgs_with_year.append((org, year))
        else:
            failed += 1
        print(f"  {i+1}/{len(orgs_with_proposals)} — resolved {len(orgs_with_year)}, failed {failed}", end="\r")
        time.sleep(0.2)

    print(f"\nDone. Resolved year for {len(orgs_with_year)} orgs ({failed} failed/no proposals found).")

    # Step 3: analyze & report
    analysis = analyze(orgs_with_year)
    print_report(len(all_orgs), orgs_with_year, analysis)

    # Save raw data
    output = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "total_orgs_on_tally": len(all_orgs),
        "orgs_with_resolved_year": len(orgs_with_year),
        "analysis": analysis,
        "orgs": [
            {
                "id": org["id"],
                "name": org["name"],
                "slug": org.get("slug"),
                "chainIds": org.get("chainIds", []),
                "proposalsCount": org.get("proposalsCount", 0),
                "first_proposal_year": year,
            }
            for org, year in orgs_with_year
        ],
    }
    with open("oz_governor_data.json", "w") as f:
        json.dump(output, f, indent=2)
    print("\nDetailed data saved to oz_governor_data.json")


if __name__ == "__main__":
    main()
