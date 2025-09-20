import logging
from db_setup import *

logging.basicConfig(
    filemode="w",  #w overwrites, a appends
    filename='main.log',
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) 

logger = logging.getLogger(__name__)


def main():
    db_tables_setup()
    # fighters_table_setup()
    #events_table_setup()
    #records_table_setup()
    # advanced_table_setup()
    #fights_table_setup()
    advanced_espn_setup()
    


if __name__ == "__main__":
    main()