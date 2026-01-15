# League-Ml-Project
- Ściągamy listę Challengerów/GrandMasterów/Masterów z paru regionów (EUNE/EUW/KR/NA) (możemy potem inne rangi dodawać)
  - https://euw1.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5?{api_key}
- Ściągamy listę meczów dla każdego gracza
  - https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{PUUID}/ids?start=0&count=100&{api_key} (100 meczów)
- Odczytujemy id tego gracza w danym meczu i zapisujemy jego trajektorię
  - https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?{api_key}
- Robimy klasteryzację na jakiej pozycji gra dany gracz (po jego trajektoriach)
- I sprawdzamy czy typy graczy jakoś się łączą z naszymi predykcjami pozycji
^^^
TO TRZEBA POPRAWIĆ


## READMEEEEEE
OD KAPPO:
Musisz stworzyc plik api.txt, i dodac do niego swoj klucz api. Plik jest wrzucont do gitignore wiec nie zostanie wrzucony na githuba.

Dane wczytujemy z plikow parquet, pliki csv są tylko do podglądu ludzkiego dla wygody pracy!!!
Żeby zobaczyć jakie dane zawieraja nasze pliki wystarczy spojrzeć do nich :) nie są one długie.

pipeline pobierania danych:
fetch_players.py (~13s, might vary based on ranks and regions) --> fetch_matches.py (~1,5h) --
--> fetch_roles.py (idk between 4h and 9h, filters out matches not played on summoners rift and fetches the roles for the rest, still testing it) --> fetch_trajectories (same time as roles so 4-9h, the script is in the testing phase)

Niestety plik fetch_matches zbiera po 10 grach zagranych przez wszystkich graczy i usuwa duplikaty, jednak nie mamy jak sprawdzic jakim typem meczu jest, jako ze dostajemy same id. Typ meczu zapisywany jest w tym samym miejscu co informacje o rolach, więc gdy zbieramy role, sprawdamy jaki typ meczu własnie załadowalismy z id i jesli jest classic to zbieramy dane i zapisujemy id meczu, else ignorujemy. Pod koniec mamy set wszystkich uzytych meczy i informacje z nich. Dane te zapisujemu do match_ids_filtered i player_roles. 

Gdybyśmy chcieli filtrowac mecze w pliku fetch_matches.py czas oczekiwania zwiekszyłby sie z 1,5h do 10,5h :)

Plik fetch_trajectories.py powinien kozystac tylko z przefiltrowanych id.
