from nba_api.stats.endpoints import playercareerstats
import pandas as pd

# Fetch career stats for a player by ID
career = playercareerstats.PlayerCareerStats(player_id='2544')  # LeBron James' player ID
career_data = career.get_data_frames()[0]

# Show the first few rows of the data
print(career_data.head())