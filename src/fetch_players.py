import requests
import numpy as np
import pandas as pd
import argparse

REGIONS = {"eun1", "euw1", "kr", "na1", "br1", "jp", "ru", "oc1"}
RANKS_HIGHELO = {"challenger", "grandmaster", "master"} #the lowercase uppercase changes are due to api url conventions
RANKS_LOWELO = {"DIAMOND", "EMERALD", "PLATINUM", "GOLD", "SILVER"}
DIVISIONS = {"IV","III","II","I"}
RANK_PRIORITY = {
    "CHALLENGER": 7,
    "GRANDMASTER": 6,
    "MASTER": 5,
    "DIAMOND": 4,
    "EMERALD": 3,
    "PLATINUM": 2,
    "GOLD": 1,
    "SILVER": 0
}
API_KEY = np.loadtxt("api.txt", dtype = str) 
    
def get_ranked_data(region: str, rank: str):
    
    if region not in REGIONS:
        print(region)
        raise "What the helly!"
    
    
    if rank.lower() in RANKS_HIGHELO:
        url = f"https://{region}.api.riotgames.com/lol/league/v4/{rank}leagues/by-queue/RANKED_SOLO_5x5?api_key={API_KEY}"
        
        try:
            response = requests.get(url)

            if response.status_code == 200:
                players = response.json()
                return players["entries"]
            else:
                error = response.json()
                print(error["message"])
                return None
            
        except requests.exceptions.RequestException as e:
            print('Error:', e)
            return None
        
    elif rank.upper() in RANKS_LOWELO:
        for division in DIVISIONS:
            url = f"https://{region}.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{rank}/{division}?page=1&api_key={API_KEY}"
            players = []
            try:
                response = requests.get(url)

                if response.status_code == 200:
                    data = response.json()
                    players.append(data["entries"]) 
                else:
                    error = response.json()
                    print (error["message"])
                    return None
            
            except requests.exceptions.RequestException as e:
                print('Error:', e)
                return None
        return players

def parse_data(entries, region: str, rank: str):
    results = []

    for entry in entries:
        results.append({"puuid": entry["puuid"],
                "region": region,
                "rank": rank.upper()})

    return results
        
def deduplicate_player_data(data):
    '''
    In case there is some lag and someone manages to lose/earn a rank during the downlad time and shows up in 2 ranks at the same time.
    '''
    df_players = pd.DataFrame(data)

    df_players["rank_priority"] = df_players["rank"].map(RANK_PRIORITY)

    df_players = (
        df_players.sort_values("rank_priority", ascending=False)
        .drop_duplicates(subset="puuid", keep="first")
        .drop(columns="rank_priority")
    )

    df_players["region"] = df_players["region"].astype("category")
    df_players["rank"] = df_players["rank"].astype("category")

    return df_players


def main():
    #we do NOT want to analyze bronze and iron trajctories
    #loading all divisions of gold and silver will be a suicide as well ,but eh
    parser = argparse.ArgumentParser()

    parser.add_argument("--region", nargs="+", default=["eun1", "euw1", "kr", "na1"])
    parser.add_argument("--rank", nargs="+", default=["challenger", "grandmaster", "master"])

    args = parser.parse_args()

    data_players = []

    for region in args.region:
        for rank in args.rank:
            players = parse_data(get_ranked_data(region, rank), region, rank)
            for player in players:
                data_players.append(player)

    df_players = deduplicate_player_data(data_players)

    df_players.to_parquet("data/player_index.parquet", index=False)
    df_players.to_csv("data/player_index.csv", index=False)


if __name__ == "__main__":
    main()