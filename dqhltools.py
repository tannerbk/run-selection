#!/user/bin/env python

""" dqhltools.py
Compute the Pass/Fail results for each DQHL processor
Write the results in the run list

Author: Elisabeth Falk
        <E.Falk@sussex.ac.uk>

Last modified: 2017-05-23 
"""

import argparse
import sys
import couchdb
import datetime

import settings

import couchdbtools

from dqhlProcChecks import *

def processRun(runNumber, data, runlistFile):

    # Unpack validity range and results:
    run_range = data['run_range']
    checks = data['checks']

    # Unpack results from the four DQ procs:
    triggerProc = checks['dqtriggerproc']
    timeProc = checks['dqtimeproc']
    runProc = checks['dqrunproc']
    pmtProc = checks['dqpmtproc']

    # Print run results list:
    runlistFile.write(" %i%i%i%i   | %i%i%i%i   |" % \
           (modifTriggerProcChecksOK(triggerProc), \
           modifTimeProcChecksOK(timeProc), \
           modifRunProcChecksOK(runProc), pmtProcChecksOK(pmtProc), \
           triggerProcChecksOK(triggerProc), timeProcChecksOK(timeProc), \
           runProcChecksOK(runProc), pmtProcChecksOK(pmtProc)) + \
                      " %i     %i     %i    %i      |" % \
           (triggerProc['n100l_trigger_rate'], \
           triggerProc['esumh_trigger_rate'], \
           triggerProc['triggerProcMissingGTID'], 
           triggerProc['triggerProcBitFlipGTID']) + \
                      " %i     %i      %i     %i     " % \
           (timeProc['event_rate'], timeProc['event_separation'], \
           timeProc['retriggers'], timeProc['run_header']) + \
                      " %i       %i     %i      |" % \
           (timeProc['10Mhz_UT_comparrison'], timeProc['clock_forward'], \
           timeProc['delta_t_comparison']) + \
                      " %i       %i     %i      %i    | %i      %i     %i\n" % \
           (runProc['run_type'], runProc['mc_flag'], \
           runProc['run_length'], runProc['trigger'], \
           pmtProc['general_coverage'], pmtProc['crate_coverage'], \
           pmtProc['panel_coverage']))

    return

def dqhlPassFailList(currentrun,runlistFile):

    # Download DQ ratdb table:
    db = couchdb.Server(settings.COUCHDB_SERVER_HL)

    data = couchdbtools.getCouchDBDict(db, currentrun)
    
    if (data != None):

       processRun(currentrun, data, runlistFile)
    
    else:

       # Print run results list with 9 flag:
       runlistFile.write(" %i%i%i%i   | %i%i%i%i   |" % \
                        (9, 9, 9, 9, 9, 9, 9, 9) + \
                        " %i     %i     %i    %i      |" % \
                        (9, 9, 9, 9) + \
                        " %i     %i      %i     %i     " % \
                        (9, 9, 9, 9) + \
                        " %i       %i     %i      |" % \
                        (9, 9, 9) + \
                        " %i       %i     %i      %i    | %i      %i     %i\n" % \
                        (9, 9, 9, 9, 9, 9, 9))
		
       sys.stderr.write("%s - dqhltools():ERROR: DQHL results not present\n"
                             % (datetime.datetime.now().replace(microsecond = 0))) 

    return

