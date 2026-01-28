import logging
from db_setup import *
from db_update import *

logging.basicConfig(
    filemode="w",  #w overwrites, a appends
    filename='main.log',
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) 

logger = logging.getLogger(__name__)

def setup():
    db_tables_setup()
    # fighters_table_setup()
    #events_table_setup()
    #records_table_setup()
    # advanced_table_setup()
    #fights_table_setup()
    # advanced_espn_setup()

def update():
    # update_events()
    # update_records_and_fights()
    # update_advanced_stats()
    #update_fighters_threaded()
    # update_fighters_threaded(type=2)
    all_fighters_gctrl()
    ...
def main():
    update()
    


if __name__ == "__main__":
    main()