import sys
import os

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import sqlite3 as sq
from my_app.scraper import get_ufc_fighters, get_fighter_records

conn = sq.connect('data/testing.db')
cursor = conn.cursor()

def test_get_ufc_fighters():
    fighters = get_ufc_fighters()

    for fighter in fighters:
        name = fighter["first_name"] + " " + fighter["last_name"]
        height = fighter["height"]
        weight = fighter["weight"]
        reach = fighter["reach"]
        stance = fighter["stance"]
        wins = fighter["wins"]
        losses = fighter["losses"]
        draws = fighter["draws"]
        belt = fighter["belt"]
        url = fighter['url']

        cursor.execute("insert into fighters (name, height, weight, reach, stance, wins, losses, draws, champ_status, url) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                       (name, height, 
                                weight, reach, stance, wins
                                        , losses, draws, belt, url))
        conn.commit()
        
def test_get_fighter_records():
    records = get_fighter_records()
