import random
from flask import redirect, render_template, session
from functools import wraps

def get_fighter_id(conn, name):
    '''this function is for the db_setup to modularize the getting of fighter ids'''
    cursor = conn.cursor()
    row = cursor.execute('select fighter_id from fighters where LOWER(name) = ?', (name.lower(),)).fetchone()
    if row:
        return row[0]
    else:
        return None

def parse_espn_stats(i):
    '''this function is for the scraper where it is needed to parse the stats strings'''
    return i.text.lower().strip().replace(' ', '_').replace('/', '_').replace('-', '_').replace('.', '')


def replace_last(text, old, new, count=1):
    """Replace the last occurence with a new word"""
    parts = text.rsplit(old, count)
    return new.join(parts)

def get_fighter_pair_url(fighter_pairs, fighter_name):
    '''this returms the url from the fighter_pairs variable I made'''
    for url, name in fighter_pairs:
        if name == fighter_name:
            return url
        
def get_random_ip(ip_list):
    '''retruns a random IP from a list of valid IPs'''
    return random.choice(ip_list)

def login_required(f):
    """
    Decorate routes to require login.

    https://flask.palletsprojects.com/en/latest/patterns/viewdecorators/
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        if session.get("user_id") is None:
            return redirect("/login")
        return f(*args, **kwargs)

    return decorated_function

def apology(message, code=400):
    """Render message as an apology to user."""

    def escape(s):
        """
        Escape special characters.

        https://github.com/jacebrowning/memegen#special-characters
        """
        for old, new in [
            ("-", "--"),
            (" ", "-"),
            ("_", "__"),
            ("?", "~q"),
            ("%", "~p"),
            ("#", "~h"),
            ("/", "~s"),
            ('"', "''"),
        ]:
            s = s.replace(old, new)
        return s

    return render_template("apology.html", top=code, bottom=escape(message)), code
