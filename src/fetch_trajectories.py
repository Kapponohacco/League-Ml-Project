import pandas as pd
import requests
import numpy as np
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

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

def get_trajectories(match_routing: str, match_id: str, session, max_retries=3):
    url = f"https://{match_routing}.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={API_KEY}"

    for attempt in range(max_retries):
        try:
            response = riot_get(session, match_routing, url)
        except requests.exceptions.RequestException as e:
            print(f"[{match_routing}] Connection error for {match_id}: {e}")
            time.sleep(2 + attempt)
            continue

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


def parse_match_timeline(timeline_json):
    match_id = timeline_json["metadata"]["matchId"]
    puuids = timeline_json["metadata"]["participants"]
    frames = timeline_json["info"]["frames"]

    players = {
        i + 1: {
            "puuid": puuids[i],
            "match_id": match_id,
            "team": None,
            "positions": []
        }
        for i in range(10)
    }

    for frame_idx, frame in enumerate(frames):
        for pid_str, pdata in frame["participantFrames"].items():
            pid = int(pid_str)
            pos = pdata.get("position")
            if pos is None:
                continue

            x, y = pos["x"], pos["y"]
            if pid == 11:
                print(f"{match_id} , {frame_idx}")
            players[pid]["positions"].append((x, y))

            if frame_idx == 0:
                if x < 2000 and y < 2000:
                    players[pid]["team"] = 0
                elif x > 13000 and y > 13000:
                    players[pid]["team"] = 1

    return list(players.values())


def fetch_trajectories_for_routing(match_ids_subset, routing, trajectories, lock):
    session = requests.Session()
    
    total = len(match_ids_subset)
    for idx, (_, row) in enumerate(match_ids_subset.iterrows(), 1):
        if idx % 100 == 0:
            print(f"[{routing}] Processed {idx}/{total} match ids")
        
        timeline = get_trajectories(
            routing,
            row.match_id,
            session=session,
        )
        
        if timeline:
            parsed_rows = parse_match_timeline(timeline)
            with lock:
                trajectories.extend(parsed_rows)


def main():
    match_ids = pd.read_parquet("data/match_ids_filtered.parquet")

    match_ids["match_routing"] = (
        match_ids["match_id"]
        .str.split("_")
        .str[0]
        .str.lower()
        .map(MATCH_ROUTING)
    )

    trajectories = list()
    lock = Lock()

    with ThreadPoolExecutor(max_workers = 4) as exec:
        futures = []
        for routing, group in match_ids.groupby("match_routing"):
            future = exec.submit(
                fetch_trajectories_for_routing,
                group,
                routing,
                trajectories,
                lock
            )
            futures.append(future)
        
        # Wait for all futures to complete
        for future in futures:
            future.result()

    print(f"\nTotal unique trajectories collected: {len(trajectories)}")
    
    data = pd.DataFrame(trajectories)
    data.to_parquet("data/trajectories.parquet", index=False)
    data.to_csv("data/trajectories.csv", index=False)


if __name__ == "__main__":
    main()