"""ratdbtools.py
This code has tools to access ratdb documents for the Run Selection checks. 

Author: Gersende Prior
        <gersende@lip.pt>

Uses the RATDB methods from examples by F. Descamps/N. Barros
"""
import sys
import subprocess
import datetime
import time

from rat import ratdb

def get_table(runnumber, tablename, db_address, db_host, db_username, db_password,db_name,db_port): 
    """Function to retrieve a table from the postgresql ratdb database. 
    :param: The run number (string)
    :param: The table name (e.g. RUN, DQLL)
    :param: The address of the postgresql database (string)
    :param: The hostname for the postgresql database (string).
    :param: The read-mode username for the postgresql database (string).
    :param: The read-mode password for the postgresql database (string).
    :returns: True and the table for the specified run number if it exists
    """

    db_connector_address = db_address+"://"+db_username+":"+db_password+"@"+db_host+":"+str(db_port)+"/"+db_name

    try:
        db = ratdb.RATDBConnector(db_connector_address)

        result = db.fetch(obj_type = tablename, run = runnumber)

        if not len(result):

	    table = "none"

            sys.stderr.write("%s - get_table():ERROR:cannot find %s.ratb table\n"
				 % (datetime.datetime.now().replace(microsecond = 0),tablename))

        else:    
            
            table = result[0]['data']

        return len(result) > 0, table

    except Exception as e:
        
        sys.stderr.write("%s - get_table():ERROR: %s\n"
                         % (datetime.datetime.now().replace(microsecond = 0),e))
        sys.exit(1)
