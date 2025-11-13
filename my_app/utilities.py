import random
from flask import redirect, render_template, session
from functools import wraps
import requests
from bs4 import BeautifulSoup
import logging
import uuid

logger = logging.getLogger(__name__)


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

def get_upcoming_events_list():
    '''This function is to get the upcoming events from the ufc stats website, returns None if error, returns {event_name:[event_url, event_date, event_location, number]} if no error'''
    url = "http://ufcstats.com/statistics/events/upcoming"
    events_hash = {}
    try:
        page = requests.get(url)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
        for i, tr in enumerate(tr_list):
            if i == 0:
                continue
            td_list = tr.find_all('td')
            event_url = td_list[0].find('a')['href'] if td_list else None
            event_name = td_list[0].find('a').text.strip() if td_list else None
            event_date = td_list[0].find('span').text.strip() if td_list else None
            event_location = td_list[1].text.strip() if td_list else None

            events_hash[i] = {
                'event_url':event_url,
                'event_date':event_date,
                'event_location':event_location,
                'event_name':event_name,
                'event_id':get_web_route()
            }

    except Exception as e:
        logger.warning(f'error getting upcoming events: {e}')
        return None

    return events_hash

def get_upcoming_event_info(url):
    '''gets the actual details of any ufc event. returns None if error. Returns {fight_number:{fighter_1, fighter_2, weight_class, is_title}} for upcoming events
    returns {fight_number:{a lot of data}} for completed ones lol'''

    #figure out how to differentiate between completed and upcoming fight
    #figure out the data structure
    try:
        page = requests.get(url)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
        fights = {}
        for number, tr in enumerate(tr_list):
            is_title = False
            td_list = tr.find_all('td')
            for i, td in enumerate(td_list):
                #the juicy stuff is only in the 1st and 6th td
                if i == 1:
                    fighter_list = td.find_all('p')
                    fighter_1 = fighter_list[0].text.strip() if fighter_list else None
                    fighter_2 = fighter_list[1].text.strip() if fighter_list else None
                # elif i == 6:
                #     x = td.find('p')
                #     weight_class = x.text.strip() if x else None
                #     if x.find('img'):
                #         is_title = True
                var = td.find_all('p')
                for p in var:
                    if 'weight' in p.text.strip():
                        weight_class = p.text.strip()
                        if p.find('img'):
                            is_title = True
                    

            fights[number + 1] = {
                'fighter_1': fighter_1,
                'fighter_2': fighter_2,
                'weight_class': weight_class,
                'is_title': is_title
            }

    except Exception as e:
        logger.warning(f'exception occuted getting details for event: {e}')
        return None
    
    return fights
    
def get_completed_event_info(url):
    '''
    get the completed events info,
    args:
    plug in an url   
    returns:
    dict: {number:{winner, loser, kd, str, td, sub, weight_class, is_title, is_extra, method, round_time}}'''
    # I guess I do need to fix this later
    try:
        page = requests.get(url)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
        fights = {}
        for number, tr in enumerate(tr_list):
            is_title = False
            is_extra = None
            if tr.find_all('td'):
                td_list = tr.find_all('td')
                fighter_list = td_list[1].find_all('p')
                fighter_1 = fighter_list[0].text.strip()
                fighter_2 = fighter_list[1].text.strip()
                kd = (td_list[2].find_all('p')[0].text.strip(), td_list[2].find_all('p')[1].text.strip())
                strikes = (td_list[3].find_all('p')[0].text.strip(), td_list[3].find_all('p')[1].text.strip())
                takedowns = (td_list[4].find_all('p')[0].text.strip(), td_list[4].find_all('p')[1].text.strip())
                sub = (td_list[5].find_all('p')[0].text.strip(), td_list[5].find_all('p')[1].text.strip())
                weight_class = td_list[6].text.strip()
                if td_list[6].find('img'):
                    src = td_list[6].find('img')['src']
                    for i in ['perf', 'fight', 'sub', 'ko']:
                        if i in src:
                            is_extra = i
                    if 'belt' in src:
                        is_title = True
                method = (td_list[7].find_all('p')[0].text.strip(), td_list[7].find_all('p')[1].text.strip())

                round_number = td_list[8].text.strip()
                time = td_list[9].text.strip()
                round_time = f"{round_number} - {time}"



            fights[number + 1] = {
                'winner': fighter_1,
                'loser': fighter_2,
                'kd': kd,
                'str': strikes,
                'td': takedowns,
                'sub':sub,
                'weight_class':weight_class,
                'is_title':is_title,
                'is_extra':is_extra,
                'method':method,
                'round_time':round_time
            }

    except Exception as e:
        logger.warning(f'exception occured getting details for event: {e}')
        return None
    
    return fights

def get_web_route():
    random_id = str(uuid.uuid4())
    return random_id
