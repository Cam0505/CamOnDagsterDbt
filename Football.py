import os
import logging
import requests
import dlt
from dotenv import load_dotenv
import time

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")
logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv("RAPIDAPI_KEY")  # Your RapidAPI key
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {
    "x-rapidapi-key": API_KEY,  # RapidAPI Key
    "x-rapidapi-host": "v3.football.api-sports.io"
}

league_id = 39  # Premier League
season = 2021


def handle_rate_limit():
    """Handle rate limit error by waiting for 60 seconds before retrying"""
    logging.info("Rate limit exceeded, waiting for 60 seconds...")
    time.sleep(60)  # Wait for 1 minute


def get_api_data(endpoint: str, params: dict) -> list:
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, headers=HEADERS, params=params, timeout=15)

    # Log the full response content for debugging
    logging.info(f"Response from {endpoint}: {response.text}")

    if response.status_code == 429:
        handle_rate_limit()
        return get_api_data(endpoint, params)  # Retry fetching data

    if response.status_code == 200:
        data = response.json().get("response", [])
        logging.info(f"Success: {endpoint} returned {len(data)} records")
        time.sleep(6)  # Sleep for 6 seconds to avoid hitting the rate limit
        return data
    else:
        logging.error(
            f"Error fetching {endpoint}: {response.status_code} - {response.text}")
        return []


@dlt.resource(write_disposition="replace")
def league_data():
    leagues = get_api_data("leagues", {"id": league_id, "season": season})
    for league in leagues:
        yield {
            "league_id": league["league"]["id"],
            "name": league["league"]["name"],
            "country": league["country"]["name"],
            "season": league["seasons"][0]["year"] if league["seasons"] else season,
            "rounds": league.get("rounds")
        }


@dlt.resource(write_disposition="replace")
def teams_data():
    teams = get_api_data("teams", {"league": league_id, "season": season})
    for team in teams:
        # Adding fixture_id to teams data
        fixture_id = team["team"].get("fixture_id", None)
        yield {
            "team_id": team["team"]["id"],
            "name": team["team"]["name"],
            "winner": team["team"].get("winner", None),
            "fixture_id": fixture_id  # Added fixture_id to the team data
        }


@dlt.resource(write_disposition="replace")
def fixtures_data():
    fixtures = get_api_data(
        "fixtures", {"league": league_id, "season": season})
    for fix in fixtures:
        yield {
            "fixture_id": fix["fixture"]["id"],
            "date": fix["fixture"]["date"],
            "venue": fix["fixture"]["venue"]["name"],
            "referee": fix["fixture"].get("referee"),
            "status": fix["fixture"]["status"]["long"],
            "season": season  # Added season to the fixtures data
        }


@dlt.resource(write_disposition="replace")
def scores_data():
    fixtures = get_api_data(
        "fixtures", {"league": league_id, "season": season})
    for fix in fixtures:
        yield {
            "fixture_id": fix["fixture"]["id"],
            "home": fix["goals"]["home"],
            "away": fix["goals"]["away"]
        }


@dlt.resource(write_disposition="replace")
def players_data():
    teams = get_api_data("teams", {"league": league_id, "season": season})
    for team in teams:
        team_id = team["team"]["id"]
        players = get_api_data("players", {"team": team_id, "season": season})
        for player_entry in players:
            player = player_entry["player"]
            stats = player_entry["statistics"][0] if player_entry["statistics"] else {
            }
            yield {
                "player_id": player["id"],
                "name": player["name"],
                "age": player.get("age"),
                "nationality": player.get("nationality"),
                "height": player.get("height"),
                "weight": player.get("weight"),
                "position": stats.get("games", {}).get("position"),
                "appearances": stats.get("games", {}).get("appearences"),
                "minutes": stats.get("games", {}).get("minutes"),
                "goals": stats.get("goals", {}).get("total"),
                "assists": stats.get("goals", {}).get("assists"),
                "yellow_cards": stats.get("cards", {}).get("yellow"),
                "red_cards": stats.get("cards", {}).get("red"),
                "rating": stats.get("games", {}).get("rating")
            }


@dlt.resource(write_disposition="replace")
def players_per_match():
    fixtures = get_api_data(
        "fixtures", {"league": league_id, "season": season})
    for fix in fixtures:
        fixture_id = fix["fixture"]["id"]
        try:
            stats = get_api_data("fixtures/players", {"fixture": fixture_id})
            for team in stats:
                for player_entry in team["players"]:
                    player = player_entry["player"]
                    stat = player_entry["statistics"][0] if player_entry["statistics"] else {
                    }
                    yield {
                        "fixture_id": fixture_id,
                        "player_id": player["id"],
                        "name": player["name"],
                        "nationality": player.get("nationality"),
                        "position": stat.get("games", {}).get("position"),
                        "minutes": stat.get("games", {}).get("minutes"),
                        "goals": stat.get("goals", {}).get("total"),
                        "assists": stat.get("goals", {}).get("assists")
                    }
        except Exception as e:
            logging.warning(
                f"Stats not available for fixture {fixture_id}: {e}")


@dlt.source
def epl_data_source():
    return [
        league_data(),
        teams_data(),
        fixtures_data(),
        scores_data(),
        players_data(),
        players_per_match()
    ]


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="epl_pipeline",
        destination="duckdb",
        dataset_name="epl_data",
        dev_mode=False
    )
    load_info = pipeline.run(epl_data_source())
    print(load_info)
