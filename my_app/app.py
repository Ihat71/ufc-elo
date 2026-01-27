from flask import Flask, flash, redirect, render_template, request, session, g
from flask_session import Session
import logging
from pathlib import Path
from my_app.utilities import login_required, apology
import sqlite3 as sq
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import datetime, date
from my_app.utilities import get_upcoming_events_list, get_upcoming_event_info, get_completed_event_info
from my_app.plots import *
# from my_app.analysis import elo_analysis, career_analysis, fight_analysis
from my_app.analysis import *

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

weight_hash = {
    'p4p':None,
    "Flyweight":'125 lbs.',
    'Bantamweight':'135 lbs.',
    'Featherweight':'145 lbs.',
    'Lightweight': '155 lbs.',
    'Welterweight': '170 lbs.',
    'Middleweight': '185 lbs.',
    'LightHeavyweight':'205 lbs.',
    'Heavyweight':'205 lbs.'
}

def get_db():
    if "db" not in g:
        conn = sq.connect(db_path)
        conn.row_factory = sq.Row
        g.db = conn.cursor()
    return conn, g.db

def get_db_no_row():
    if "db" not in g:
        g.db = sq.connect(db_path)
    return g.db

@app.teardown_appcontext
def close_db(error):
    db = g.pop("db", None)
    if db is not None:
        db.close()

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

@app.route('/roster', methods=['GET', 'POST'])
def roster():
    default_weight, default_country, default_team = ('', '', '')
    db = get_db_no_row()
    countries = [row[0] for row in db.execute('select distinct country from fighters').fetchall()]
    teams = [row[0] for row in db.execute('select distinct team from fighters').fetchall()]
    fighters = []

    if request.method == "POST":
        weight = request.form.get('weight_class')
        country = request.form.get('country')
        team = request.form.get('team')

        if len(weight) == 1 and weight[0] == default_weight:
            weight = None
        if country == default_country:
            country = None
        if team == default_team:
            team = None

        query = 'select fighter_id, name from fighters'
        filters = []
        answers = []

        if weight:
            if weight != "All" and weight != "Heavyweight":
                placeholders = ', '.join(['?'])
                filters.append(f"weight IN ({placeholders})")
                answers.extend([weight_hash[weight]])
            elif weight == "All":
                #error here broski
                # placeholders = ', '.join(['?'] * 8)
                # filters.append(f'weight in ({placeholders})')
                # answers.extend([weight for weight_class, weight in weight_hash.items()])
                pass
            elif weight == "Heavyweight":
                placeholders = '?'
                filters.append(f"cast(replace(weight, ' lbs.', '') as integer) > {placeholders}")
                answers.extend([207])

        if country and country != 'None':
            filters.append("country = ?")
            answers.append(country)
        if team and team != 'None':
            filters.append("team = ?")
            answers.append(team)

        if filters:
            query += " where " + " and ".join(filters)

        fighters = db.execute(query, tuple(answers)).fetchall()
        print("QUERY:", query)
        print("PARAMS:", answers)
        print("number of fighters:", len(fighters))


    return render_template('roster.html', countries=countries, teams=teams, fighters=fighters)


@app.route('/fights/<sub>/', methods=['GET', 'POST'])
def fights(sub):
    conn, db = get_db()
    rows = []
    upcoming_events = []
    fights = []
    event_query = 'select * from events;'
    if sub == 'upcoming':
        upcoming = get_upcoming_events_list()
        if upcoming:
            for i in range(len(upcoming.keys())):
                upcoming_events.append(upcoming[i+1])
                print(upcoming_events)
        session['fights_upcoming'] = upcoming_events
    elif sub == 'completed':
        rows = db.execute(event_query).fetchall()
        rows = sorted(rows, key=lambda x: datetime.strptime(x['event_date'], "%B %d, %Y"), reverse=True)
    else:
        if sub.isnumeric() or type(sub) == int:
            cursor = db.execute('select * from events where event_id = ?', (sub,)).fetchall()
            fights = get_completed_event_info(url=cursor[0]['event_url'])
            print('fights: ', fights)
        else:
            for event in session['fights_upcoming']:
                if event['event_id'] == sub:
                    fights = get_upcoming_event_info(url=event['event_url'])



    return render_template('fights.html', events=rows, upcoming_events=upcoming_events, sub=sub, fights=fights)

@app.route('/match-ups')
def match_ups():
    return render_template('match_ups.html')

@app.route('/predictions')
def predictions():
    return render_template('predictions.html')

@app.route("/login", methods=["GET", "POST"])
def login():
    """Log user in"""
    conn, db = get_db()
    # Forget any user_id
    session.clear()

    # User reached route via POST (as by submitting a form via POST)
    if request.method == "POST":
        # Ensure username was submitted
        if not request.form.get("username"):
            return apology("must provide username", 403)

        # Ensure password was submitted
        elif not request.form.get("password"):
            return apology("must provide password", 403)

        # Query database for username
        rows = db.execute(
            "SELECT * FROM users WHERE username = ?", (request.form.get("username"),)
        ).fetchall()

        # Ensure username exists and password is correct
        if len(rows) != 1 or not check_password_hash(
            rows[0]["hash"], request.form.get("password")
        ):
            return apology("invalid username and/or password", 403)

        # Remember which user has logged in
        session["user_id"] = rows[0]["id"]
        session["username"] = request.form.get("username")

        # Redirect user to home page
        return redirect("/")

    # User reached route via GET (as by clicking a link or via redirect)
    else:
        return render_template("login.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    conn, db = get_db()

    username = request.form.get("username")
    password = request.form.get("password")
    confirmation = request.form.get("confirmation")

    if request.method == "POST":
        if not username:
            return apology("Write a username!")
        elif not password or not confirmation:
            return apology("Write a password!")
        elif password != confirmation:
            return apology("Wrong confirmation")

        try:
            db.execute("INSERT INTO users (username, hash) VALUES (?, ?)", (username, generate_password_hash(password)))
            conn.commit()
        except ValueError:
            return apology("Already registered!")
    return render_template("register.html")

@app.route('/search/', methods=['GET', 'POST'])
def search():
    conn, db = get_db()
    fighters_list = []
    fighter_search = request.args.get('query')
    fighter_search_first_name = fighter_search + "%"
    fighter_search_last_name = "%" + fighter_search
    match_1 = db.execute('select * from fighters where name like ?', (fighter_search_first_name.lower().title().strip(),)).fetchall()
    match_2 = db.execute('select * from fighters where name like ?', (fighter_search_last_name.lower().title().strip(),)).fetchall()

    if len(fighter_search) != 1:
        if match_1 and not match_2:
            fighters_list = match_1
        elif match_2 and not match_1:
            fighters_list = match_2
        elif match_2 and match_1:
            if len(match_1)>len(match_2):
                fighters_list = match_1
            elif len(match_2)>=len(match_2):
                fighters_list = match_2
    else:
        # this is to make sure that if the search is only 1 letter long the searches are first name (starts from the first letter only) searches
        if match_1:
            fighters_list = match_1

    matches = len(fighters_list)

    return render_template('search.html', matched_number = matches, fighters_list=fighters_list)

@app.route('/rankings', methods=['GET', 'POST'])
def rankings():
    conn, db = get_db()
    fighters = []
    action = None
    # chosen_class = None
    original_query = 'Select f.fighter_id, f.name, e.elo from fighters f join elo e on f.fighter_id = e.fighter_id'
    query = 'Select f.fighter_id, f.name, e.elo from fighters f join elo e on f.fighter_id = e.fighter_id'
    answers = []
    if request.method == "POST":
        action = request.form.get('action')
        if action in weight_hash.keys():
            if action == 'Heavyweight':
                query += " where cast(replace(f.weight, ' lbs.', '') as integer) > ?"
                answers.append(207)
            elif action == 'p4p':
                query = original_query #ik it is redundant
            else:
                query += " where f.weight = ?"
                answers.append(weight_hash[action])
        
        query += " order by e.elo desc;"
        print("class:", action)
        print('query', query)
        fighters = db.execute(query, tuple(answers)).fetchall()

    return render_template('rankings.html', fighters=fighters, chosen_class=action)

@app.route('/fighter/<id>/', methods=['GET', 'POST'])
def fighter(id):
    selection = 'career'
    conn, db = get_db()
    fighter = db.execute('select * from fighters where fighter_id = ?', (id,)).fetchall()
    fighter = dict(fighter[0])
    fighter['birthday'] = date.today().year - datetime.strptime(fighter['birthday'], '%m/%d/%Y').year if fighter['birthday'] != None else None

    plot = elo_history_plot(id).to_html(full_html=False)

    elo_hash = elo_analysis(id)
    career_hash = career_analysis(db=db, id=id)
    data_hash = career_hash
    print(data_hash)
    last_5 = data_hash['last_5']
    if request.method == 'POST':
        selection = request.form.get('action')
        if selection == "striking":
            plot = striking_analysis_plot(id, db).to_html(full_html=False)
            data_hash = get_hash_data(db, 'striking', id)
        elif selection == "clinch":
            plot = clinching_analysis_plot(id, db).to_html(full_html=False)
            data_hash = get_hash_data(db, 'clinching', id)
    return render_template('fighter.html', fighter=fighter, elo_data_hash=elo_hash, selection=selection, plot=plot, data_hash=data_hash, last_5=last_5, last_fight=career_hash['last_fight'])

@app.route('/versus/<fight_id>/', methods=['GET', 'POST'])
def versus(fight_id):
    conn, db = get_db()
    fight = db.execute('select * from records where fight_id = ?', (fight_id,)).fetchone()

    fighter_1, fighter_2 = fight_analysis(db, fight)

    return render_template('versus.html', fighter_1=fighter_1, fighter_2=fighter_2)


@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')


