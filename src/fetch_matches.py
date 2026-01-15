import pandas as pd
import requests
import numpy as np
from ratelimit import limits, sleep_and_retry
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor


MATCH_ROUTING = {
    "eun1": "europe",
    "euw1": "europe",
    "ru": "europe",
    "na1": "americas",
    "br1": "americas",
    "kr": "asia",
    "jp": "asia",
    "oc1": "sea"
}
API_KEY = np.loadtxt("api.txt", dtype=str)

def make_riot_get():
    @sleep_and_retry
    @limits(calls=100, period=120)  # per routing
    def riot_get(url, session):
        return session.get(url)
    return riot_get

riot_get_by_routing = {
    "europe": make_riot_get(),
    "americas": make_riot_get(),
    "asia": make_riot_get(),
    "sea": make_riot_get()
}

def get_match_ids(match_routing: str, puuid: str, count: int, session, riot_get, max_retries=3):
    
    url = f"https://{match_routing}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count={count}&api_key={API_KEY}"

    for attempt in range(max_retries):
        response = riot_get(url, session)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 1))
            print(f"Response: retry-after {retry_after}s")
            print(f"Rate limited, sleeping for 120 s (attempt {attempt + 1}/{max_retries})")
            time.sleep(retry_after + 1)
            # Continue to retry instead of returning []
        elif response.status_code < 416:
            error = response.json()['message']
            print(f"Error: {error}, {response.status_code}")
            return []
        else:
            print(f"Error {response.status_code} for puuid {puuid} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)

    print(f"Failed to fetch matches for puuid {puuid} after {max_retries} attempts")
    return []

def fetch_match_ids_for_routing(players_subset, routing, match_ids, lock):
    session = requests.Session()
    riot_get = riot_get_by_routing[routing]

    
    total = len(players_subset)
    for idx, (_, row) in enumerate(players_subset.iterrows(), 1):
        if idx % 100 == 0:
            print(f"[{routing}] Processed {idx}/{total} players")
        
        ids = get_match_ids(
            routing,
            row.puuid,
            count=10,
            session=session,
            riot_get=riot_get
        )
        
        if ids:
            with lock:
                match_ids.update(ids)


def main():
    players = pd.read_parquet("src\data\player_index.parquet")

    players["match_routing"] = players["region"].map(MATCH_ROUTING)

    match_ids = set()
    lock = Lock()

    with ThreadPoolExecutor(max_workers = 4) as exec:
        for routing, group in players.groupby("match_routing"):
            exec.submit(
                fetch_match_ids_for_routing,
                group,
                routing,
                match_ids,
                lock
            )

    print(f"\nTotal unique match IDs collected: {len(match_ids)}")
    
    data = pd.DataFrame({"match_id": list(match_ids)})
    data.to_parquet("data/match_ids.parquet", index=False)
    data.to_csv("data/match_ids.csv", index=False)


if __name__ == "__main__":
    main()