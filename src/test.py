import pandas as pd

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

match_ids = pd.read_parquet("data/match_ids.parquet")

match_ids["match_routing"] = (
    match_ids["match_id"]
    .str.split("_")
    .str[0]
    .str.lower()
    .map(MATCH_ROUTING)
)

print(match_ids)