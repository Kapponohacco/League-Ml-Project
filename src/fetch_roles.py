import pandas as pd
import requests
import numpy as np
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

START = time.time()
ROUTING_INTERVAL = 120 / 100  # 1.2s
last_call_time = defaultdict(float)
routing_lock = defaultdict(Lock)


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

def riot_get(session, routing, url):
    with routing_lock[routing]:
        elapsed = time.time() - last_call_time[routing]
        if elapsed < ROUTING_INTERVAL:
            time.sleep(ROUTING_INTERVAL - elapsed)

        response = session.get(url)
        last_call_time[routing] = time.time()

    return response

def get_roles(match_routing: str, match_id: str, session, max_retries=3):
    url = f"https://{match_routing}.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={API_KEY}"

    for attempt in range(max_retries):
        response = riot_get(session, match_routing, url)

        if response.status_code == 200:
            return response.json()

        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 2))
            print(f"[{match_routing}] 429 → sleeping {retry_after}s")
            time.sleep(retry_after+1)

        else:
            print(f"Error {response.status_code} for {match_id}")
            return None
        
    print(f"Failed to fetch for match_id {match_id}.")
    return None


def parse_match_roles(match_json):

    match_id = match_json["metadata"]["matchId"]
    puuids = match_json["metadata"]["participants"]
    type = match_json["info"]["gameMode"]

    if type =="CLASSIC":

        players = {
            i + 1: {
                "puuid": puuids[i],
                "match_id": match_id,
                "role": None,
                "champion": None
            }
            for i in range(10)
        }

        for idx, pdata in enumerate(match_json["info"]["participants"], start=1):
            role = pdata.get("teamPosition")
            champion = pdata.get("championName")
            if role is None:
                continue

            players[idx]["role"] = role
            players[idx]["champion"] = champion

        return list(players.values()), match_id
    
    else:
        return None,None


def fetch_roles_for_routing(match_ids_subset, routing, roles: list, matches: set, lock):
    session = requests.Session()
    
    total = len(match_ids_subset)
    for idx, (_, row) in enumerate(match_ids_subset.iterrows(), 1):
        if idx % 50 == 0:
            print(f"[{routing}] Processed {idx}/{total} match ids, time since start{time.time() - START}")
        
        timeline = get_roles(
            routing,
            row.match_id,
            session=session,
        )
        
        if timeline:
            parsed_rows, match_id = parse_match_roles(timeline)
            with lock:
                if parsed_rows and match_id:
                    roles.extend(parsed_rows)
                    matches.add(match_id)


def main():
    
    match_ids = pd.read_parquet("data/match_ids.parquet")

    match_ids["match_routing"] = (
        match_ids["match_id"]
        .str.split("_")
        .str[0]
        .str.lower()
        .map(MATCH_ROUTING)
    )

    roles = list()
    matches = set()
    lock = Lock()

    with ThreadPoolExecutor(max_workers = 4) as exec:
        futures = []
        for routing, group in match_ids.groupby("match_routing"):
            future = exec.submit(
                fetch_roles_for_routing,
                group,
                routing,
                roles,
                matches,
                lock
            )
            futures.append(future)
        
        # Wait for all futures to complete
        for future in futures:
            future.result()

    print(f"\nTotal unique roles collected: {len(roles)}")
    
    data = pd.DataFrame(roles)
    data.to_parquet("data/player_roles.parquet", index=False)
    data.to_csv("data/player_roles.csv", index=False)

    matches_filtered = pd.DataFrame({"match_id": list(matches)})
    matches_filtered.to_parquet("data/match_ids_filtered.parquet", index=False)
    matches_filtered.to_csv("data/match_ids_filtered.csv", index=False)


if __name__ == "__main__":
    main()