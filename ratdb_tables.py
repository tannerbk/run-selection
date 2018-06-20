import psycopg2
import time
import sys
import argparse
from datetime import datetime

'''
   Author: T. Kaptanoglu

   Detail:
   Check the tables exist for all the various important ratdb tables.
   Note these are tables that are posted to RATDB to be used in processing
   and do not include tables used for monitoring purposes.

   Primarily designed to be used for run selection purposes to make sure
   the necessary RATDB tables all exist before identifying a run as gold.

   Example use: 'python ratdb_tables.py -l 111900 -u 111950'
'''

DETECTOR_DB_USER = 'snoplus'
DETECTOR_DB_HOST = 'pgsql.snopl.us'
DETECTOR_DB_PORT = 5400
DETECTOR_DB_NAME = 'detector'
DETECTOR_DB_PASS = ''

RATDB_USER = 'snoplus'
RATDB_HOST = 'pgsql.snopl.us'
RATDB_PORT = 5400
RATDB_NAME = 'ratdb'
RATDB_PASS = ''

# Hard-coded list of 'critical tables'
critical_tables = {
'RUN': "",
'DAQ_RUN_LEVEL': "",
'PMT_DQXX': "",
'TPMUONFOLLOWER': "",
'MISSEDMUONFOLLOWER': "",
'BLINDNESS_CHUNKS': "",
'TRIGGER_CLOCK_JUMPS': "",
'MISSED_COUNTS': "",
'PEDCUT': "",
'ATMOSPHERICS': "",
'NOISE_RUN_LEVEL': "",
'LIVETIME_CUT': ["retriggercut", "burstcut", "caenlosscut"],
'CALIB_COMMON_RUN_LEVEL': ["MANIP"]
}

def list_of_gold_runs(curr, lower, upper):
    ''' Get a list of golden runs '''

    curr.execute("SELECT a.run, b.run_type FROM evaluated_runs AS a "
                 "INNER JOIN run_state AS b ON a.list = 1 AND a.run = b.run " 
                 "WHERE a.run >= %s AND a.run <= %s ORDER BY a.run ASC", (lower, upper))
    rows = curr.fetchall()

    gold_runs = []

    for run, run_type in rows:
        run = int(run)
        gold_runs.append((run, run_type))

    return gold_runs

def list_of_runs(curr, lower, upper):
    ''' Get a list of physics and deployed source runs '''

    curr.execute("SELECT run, run_type FROM run_state WHERE run >= %s "
                 "AND run <= %s ORDER BY run ASC", (lower, upper))
    rows = curr.fetchall()

    runs = []

    for run, run_type in rows:
        if run_type is None:
            continue
        if int(run) < 100000:
            continue
        if run_type & 0x4 or run_type & 0x8:
            runs.append((run, run_type))

    return runs

def is_table_in_ratdb(curr, lower, upper, table_name):
    ''' Returns a run list for runs for a given table '''

    curr.execute("SELECT DISTINCT ON (run_begin) run_begin "
                 "FROM ratdb_header_v2 WHERE run_begin >= %s " 
                 "AND run_begin <= %s AND type = %s ORDER BY "
                 "run_begin ASC", (lower, upper, table_name))

    rows = curr.fetchall()    

    runs = []
    for run in rows:
        if run[0] in runs:
            continue
        runs.append(int(run[0]))

    return runs

def is_indexed_table_in_ratdb(curr, lower, upper, table_name, index):
    ''' Returns a run list of runs for a given indexed table '''

    curr.execute("SELECT DISTINCT ON (run_begin) run_begin "
                 "FROM ratdb_header_v2 WHERE run_begin >= %s "
                 "AND run_begin <= %s AND type = %s and "
                 "index = %s ORDER BY run_begin ASC", \
                 (lower, upper, table_name, index))

    rows = curr.fetchall()    

    runs = []
    for run in rows:
        if run[0] in runs:
            continue
        runs.append(int(run[0]))

    return runs

def run_length(curr, run):
    ''' Return the length of a run according to the detector DB '''

    curr.execute("SELECT timestamp, end_timestamp FROM run_state "
                 "WHERE run = %s", (run,))
    rows = curr.fetchall()

    for timestamp, end_timestamp in rows:
        d1 = datetime.strptime(str(timestamp), "%Y-%m-%d %H:%M:%S.%f")
        t1 = time.mktime(d1.timetuple())
        d2 = datetime.strptime(str(end_timestamp), "%Y-%m-%d %H:%M:%S.%f")
        t2 = time.mktime(d2.timetuple())
        return float(t2 - t1)

if __name__=="__main__":

    parser = argparse.ArgumentParser(description="find missing ratdb tables")
    parser.add_argument('--upper', '-u', type=str, help='Upper run range')
    parser.add_argument('--lower', '-l', type=str, help='Lower run range')
    parser.add_argument('--run_length', '-r', action='store_false', help='Look at runs > 30 mins')
    parser.add_argument('--filename', '-f', type=str, default='missing_tables.txt')
    parser.add_argument('--gold', '-g', action='store_true', help='Only look at gold runs')
    args = parser.parse_args()

    if not args.upper or not args.lower:
        print "Provide both an upper and lower run range."
        sys.exit()

    # Connect to detector database
    dconn = psycopg2.connect('postgresql://%s:%s@%s:%i/%s' %
                             (DETECTOR_DB_USER, DETECTOR_DB_PASS,
                              DETECTOR_DB_HOST, DETECTOR_DB_PORT,
                              DETECTOR_DB_NAME))
    dcurr = dconn.cursor()

    all_runs = list_of_runs(dcurr, args.lower, args.upper)
    gold_runs = list_of_gold_runs(dcurr, args.lower, args.upper)

    # Either check just gold runs or all physics/deployed source runs
    if args.gold:
        print "Only looking at gold runs."
        runs = gold_runs
    else:
        runs = all_runs

    # Connect to rat database
    conn = psycopg2.connect('postgresql://%s:%s@%s:%i/%s' %
                             (RATDB_USER, RATDB_PASS,
                              RATDB_HOST, RATDB_PORT,
                              RATDB_NAME))
    curr = conn.cursor()

    f = open(args.filename,"w")

    print "Finding missing tables between runs", args.lower, args.upper

    # Loop over critical tables
    for i, table in enumerate(critical_tables.keys()):
        print "Identifying missing", table, "tables"
        # Tables with no index
        if not critical_tables[table]:
            tables = is_table_in_ratdb(curr, args.lower, args.upper, table)
            for run, run_type in runs:
                if run not in tables:
                    # Skip runs under 30 minutes
                    runlength = run_length(dcurr, run)/60.0 # in minutes
                    if(args.run_length and runlength < 30.0):
                        continue
                    f.write(table + ' ' + str(run) +'\n')
            continue
        # Tables with an index
        for j, index in enumerate(critical_tables[table]):
            tables = is_indexed_table_in_ratdb(curr, args.lower, args.upper, table, index)  
            for run, run_type in runs:
                # Treat the table specifying source location specially
                if table == 'CALIB_COMMON_RUN_LEVEL' and run_type & 0x8:
                    # Note not skipping runs under 30 minutes
                    if run not in tables:
                        if j == 0: # Just note the missing table once
                            f.write(str(table) + ' ' + str(run) +'\n')
                elif table != 'CALIB_COMMON_RUN_LEVEL':
                    if run not in tables:
                        # Skip runs under 30 minutes
                        runlength = run_length(dcurr, run)/60.0 # in minutes
                        if(args.run_length and runlength < 30.0):
                            continue
                        if j == 0:
                            f.write(str(table) + ' ' + str(run) +'\n')

    print "Missing tables written to", args.filename
    f.close()
    dconn.close()
    conn.close()

