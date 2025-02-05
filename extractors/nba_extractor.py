from nba_api.stats.endpoints import commonallplayers
import pandas as pd

# Fetch career stats for a player by ID
all_players = commonallplayers.CommonAllPlayers()
career_data = all_players.get_data_frames()[0]

# Show the first few rows of the data
print(career_data)