from flask import Flask, flash, redirect, render_template, request, session
from flask_session import Session
import logging
from pathlib import Path
from my_app.utilities import login_required

#this is going to be the file for my website

logger = logging.getLogger(__name__)

app = Flask(__name__)

# Custom filter
# app.jinja_env.filters["usd"] = usd

# Configure session to use filesystem (instead of signed cookies)
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

db_path = (Path(__file__).parent).parent / "data" / "testing.db"

@app.after_request
def after_request(response):
    """Ensure responses aren't cached"""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response

@app.route('/')
def index():
    return render_template('index.html')
