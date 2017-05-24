#!/usr/bin/env python
"""rscheck.py
This script performs data-quality low-level and high-level checks for Run Selection
Takes as argument a start run number, the total number of run you want to process
and the name of a log file

Author: Gersende Prior
        <gersende@lip.pt>
"""

import argparse
import sys
import subprocess
import tempfile
import os
import rstools
import ratdbtools
import dqhltools
import math
import array
import dateutil
import datetime

import settings

from pprint import pprint
from dateutil import parser

# Check if the rat environment is set
if "RATROOT" not in os.environ:
    print "--- dqll(): please set the RATROOT environment variable"
    sys.exit()

def main():
    # Parse the arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-n", dest = "firstrun", help = "First run number to process", type = int, required = True)
    parser.add_argument("-i", dest = "lastrun", help = "Last run number to process", type = int, required = True)

    args = parser.parse_args()

    # Exit if no run numbers supplied
    if args.firstrun == "0" or args.lastrun == "0":
    	
	sys.stderr.write("%s - rscheck():ERROR: please supply a start run number using \'-n\'"
                             % (datetime.datetime.now().replace(microsecond = 0)))
 	sys.exit(1)
   
    # Run type bit masks	
    PHYSICS_RUN_MASK = 0x4 # bit 2

    # Detector State bit masks
    DCR_ACTIVITY_MASK = 0x200000 # bit 21
    COMP_COIL_OFF_MASK = 0x400000 # bit 22 
    PMT_OFF_MASK = 0x800000 # bit 23
    SLASSAY_MASK = 0x4000000 # bit 26
    UNUSUAL_ACTIVITY_MASK = 0x8000000 # bit 2

    # Create run list
    runlistname = "runlist_{0}-{1}.txt".format(args.firstrun,args.lastrun)

    runlist = open(runlistname,'w')

    # Write run list header
    rstools.write_header(runlist)

    # Loop over runs from <firstrun> to <lastrun>  
    for run in range(args.firstrun,args.lastrun + 1):	

    	sys.stdout.write("%s - rscheck():INFO: preparing the Run Selection checks for run %s\n" 
     		             % (datetime.datetime.now().replace(microsecond = 0),run))

	
	# Some cosmetics taken from E. Falk
	p = 0
        if ((run % 10 == 0) and (i != 0)):
	   runlist.write("-------|----------|----------|" + \
                  	 "-----------|-----------|" + \
                         "------------------|----"
                         "---------------------|" + \
                         "------------------------------------------------|" + \
                         "---------------------------|---------------------\n")
						
        # Read detector state database & alarms
	# TO DO


	runtype = 1
	
	# Read RUN.ratdb
	rundatatuple = ratdbtools.get_table(run, "RUN", settings.RATDB_ADDRESS, settings.RATDB_HOST, 
                                            settings.RATDB_READ_USER, settings.RATDB_READ_PASSWORD,
                                            settings.RATDB_NAME, settings.RATDB_PORT)

        rundata = rundatatuple[1]

        if rundatatuple[0]:

	   runtypemask = rundata['runtype']

	   # NOT a physics run		
	   if runtypemask & PHYSICS_RUN_MASK != PHYSICS_RUN_MASK:
		
	      sys.stdout.write("%s - rscheck():INFO: run %i is not a PHYSICS run\n"
              		           % (datetime.datetime.now().replace(microsecond = 0),run))
     
              continue

           # DCR Activity bit set
	   if runtypemask & DCR_ACTIVITY_MASK == DCR_ACTIVITY_MASK:

	      sys.stdout.write("%s - rscheck():INFO: run %i has DCR Activity bit set\n"
                                   % (datetime.datetime.now().replace(microsecond = 0),run))

              runtype = 0

           # Compensation Coils OFF
           if runtypemask & COMP_COIL_OFF_MASK == COMP_COIL_OFF_MASK:

              sys.stdout.write("%s - rscheck():INFO: run %i has Comp Coils OFF\n"
                                   % (datetime.datetime.now().replace(microsecond = 0),run))

              runtype = 0

           # PMTs OFF
           if runtypemask & PMT_OFF_MASK == PMT_OFF_MASK:

              sys.stdout.write("%s - rscheck():INFO: run %i has PMTs OFF\n"
                                   % (datetime.datetime.now().replace(microsecond = 0),run))

              runtype = 0

           # SLAssay 
           if runtypemask & SLASSAY_MASK == SLASSAY_MASK:

              sys.stdout.write("%s - rscheck():INFO: run %i has SLAssay\n"
                                   % (datetime.datetime.now().replace(microsecond = 0),run))

              runtype = 0

	   # Unusual Activity
           if runtypemask & UNUSUAL_ACTIVITY_MASK == UNUSUAL_ACTIVITY_MASK:

              sys.stdout.write("%s - rscheck():INFO: run %i has Unusual Activity bit set\n"
                                   % (datetime.datetime.now().replace(microsecond = 0),run))

              runtype = 0

        else:

           runtype = 9


	runduration = 1

	cratestatus = 1

	cratedac = 1
	
        # Read DQLL.ratdb
	dqlldatatuple = ratdbtools.get_table(run, "DQLL", settings.RATDB_ADDRESS, settings.RATDB_HOST,
                                             settings.RATDB_READ_USER, settings.RATDB_READ_PASSWORD,
                                             settings.RATDB_NAME,settings.RATDB_PORT)

        dqlldata = dqlldatatuple[1]

        if dqlldatatuple[0]:

           duration = dqlldata['duration_seconds']

           # Duration < 30 minutes
           if duration < 1800:
           
	      sys.stdout.write("%s - rscheck():INFO: run %i duration is less than 30 minutes\n"
                                   % (datetime.datetime.now().replace(microsecond = 0),run))

              runduration = 0

	   crates_status_a = dqlldata['crate_hv_status_a']

	   # At least one crate HV is OFF (A supply)
	   for i in range(len(crates_status_a)):
   
	       if crates_status_a[i] == False:

	          sys.stdout.write("%s - rscheck():INFO: run %i crate %i HV is off\n"
                  	               % (datetime.datetime.now().replace(microsecond = 0),run,i))

		  cratestatus = 0

           # OWLs are OFF (16B supply)
           crate_16B_status = dqlldata['crate_16_hv_status_b']

           if crate_16B_status == False:

	      sys.stdout.write("%s - rscheck():INFO: run %i OWLs HV is off\n"
                                       % (datetime.datetime.now().replace(microsecond = 0),run))

              cratestatus = 0

	   # At least one DAC value is 0 (power supply A)
           crates_dac_a = dqlldata['crate_hv_dac_a']

	   for i in range(len(crates_dac_a)):

	       if crates_dac_a[i] == 0:

	          sys.stdout.write("%s - rscheck():INFO: run %i crate %i DAC value is 0\n"
                                       % (datetime.datetime.now().replace(microsecond = 0),run,i))

                  cratedac = 0

	   # OWLs DAC value is 0 (power supply B)
           crate_16B_dac = dqlldata['crate_16_hv_status_b']

	   if crate_16B_dac == 0:

              sys.stdout.write("%s - rscheck():INFO: run %i crate %i DAC value is 0\n"
	                           % (datetime.datetime.now().replace(microsecond = 0),run,i))
           
              cratedac = 0
        
        else:

           runduration = 9
	   cratestatus = 9
           cratedac = 9

	# Write run info in run list
        runlist.write(str(run) + ' | ')
	runlist.write(str(runtype) + '        | ')
	runlist.write(str(runduration) + '        | ')
	runlist.write(str(cratestatus) + '         | ')
	runlist.write(str(cratedac) + '         | ')

        # Perform the HL checks
        dqhltools.dqhlPassFailList(run,runlist)

    runlist.close()	

    return 0  # Success!

if __name__ == '__main__':
    print sys.exit(main())
