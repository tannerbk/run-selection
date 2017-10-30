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

    # Variables for statistics calculation
    nruns = 0
    nnotphys = 0
    ntypeok = 0
    ndurationok = 0
    ncratehvok = 0
    nnohvalarm = 0

    nphysruns = 0

    # Create run list
    runlistname = "runlist_{0}-{1}.txt".format(args.firstrun,args.lastrun)

    runlist = open(runlistname,'w')

    # Write run list header
    rstools.write_header(runlist)

    # Counter for some cosmetics below
    p = 0

    # Loop over runs from <firstrun> to <lastrun>  
    for run in range(args.firstrun,args.lastrun + 1):	

    	sys.stdout.write("%s - rscheck():INFO: preparing the Run Selection checks for run %s\n" 
     		             % (datetime.datetime.now().replace(microsecond = 0),run))

        # Skipping non-existing runs in ORCA
        if run == 100259 or run == 101112 or run == 101347 or run == 101650 \
	or (run > 101857 and run < 101887) or run == 102924 or run == 103166 \
      	or run == 103351 or run == 103376 or run == 103644 or run == 103822 \
        or run == 103850 or run == 103924 or run == 104392 or run == 104427 \
        or run == 106359:
	    sys.stdout.write("%s - rscheck():INFO: run %s does not exist in ORCA/detector state database - skipping\n"
                            	% (datetime.datetime.now().replace(microsecond = 0),run)) 
            continue
	
        nruns += 1

	# Some cosmetics taken from E. Falk
        if ((run % 10 == 0) and (p != 0)):

            runlist.write("-------||-------|------|" + \
                          "------|-------|" + \
                          "-------|--------||----"
                          "-----|--------|" + \
                          "-------------------------|-----------------------------------------|--------------------|" + \
                          "--------------------\n")
						
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
     
              nnotphys += 1

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

        if runtype == 1: ntypeok += 1

	runduration = 1

	cratestatus = 1

	cratedac = 1

        crateovercurrent = 1

        cratecurrentnearzero = 1

        cratesetpointdiscrepancy = 1

        cratealarmok = 1
	
        # Read DQLL.ratdb
	dqlldatatuple = ratdbtools.get_table(run, "DQLL", settings.RATDB_ADDRESS, settings.RATDB_HOST,
                                             settings.RATDB_READ_USER, settings.RATDB_READ_PASSWORD,
                                             settings.RATDB_NAME,settings.RATDB_PORT)

        dqlldata = dqlldatatuple[1]

        if dqlldatatuple[0]:

           duration = dqlldata['duration_seconds']

           version = dqlldata['version']

           # Duration < 30 minutes
           if duration < 1800:
           
	      sys.stdout.write("%s - rscheck():INFO: run %i duration is less than 30 minutes\n"
                                   % (datetime.datetime.now().replace(microsecond = 0),run))

              runduration = 0

           if runtype == 1 and runduration == 1: ndurationok += 1

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

	          sys.stdout.write("%s - rscheck():INFO: run %i crate %i power supply A DAC value is 0\n"
                                       % (datetime.datetime.now().replace(microsecond = 0),run,i))

                  cratedac = 0

	   # OWLs DAC value is 0 (power supply B)
           crate_16B_dac = dqlldata['crate_16_hv_dac_b']

	   if crate_16B_dac == 0:

              sys.stdout.write("%s - rscheck():INFO: run %i crate 16 power supply B DAC value is 0\n"
	                           % (datetime.datetime.now().replace(microsecond = 0),run))
           
              cratedac = 0
        
           # HV alarms only exist for version 4 and later
           if version > 3:

               # Crate HV alarms
               detectordbalarms = dqlldata['detector_db_alarms']

               # At least one crate has a current near zero alarm (power supply A)
               hvcurrentnearzero_a = detectordbalarms['HV_current_near_zero_A']

               for i in range(len(hvcurrentnearzero_a)):

                   if hvcurrentnearzero_a[i] == 1:

                       sys.stdout.write("%s - rscheck():INFO: run %i crate %i has a current near zero alarm\n"
                                        % (datetime.datetime.now().replace(microsecond = 0),run,i))

                       cratecurrentnearzero = 0

               # OWLs crate with current near zero (power supply B)
               hvcurrentnearzero_16b = detectordbalarms['HV_current_near_zero_B']

               if hvcurrentnearzero_16b == 1: 
               
                   sys.stdout.write("%s - rscheck():INFO: run %i crate 16 power supply B has a current near zero alarm\n"
                                    % (datetime.datetime.now().replace(microsecond = 0),run))
                   
                   cratecurrentnearzero = 0

               # At least one crate has an over-current alarm (power supply A)
               hvovercurrent_a = detectordbalarms['HV_over_current_A']

               for i in range(len(hvovercurrent_a)):

                   if hvovercurrent_a[i] == 1:

                       sys.stdout.write("%s - rscheck():INFO: run %i crate %i has an over current alarm\n"
                                        % (datetime.datetime.now().replace(microsecond = 0),run,i))
                       
                        
                       crateovercurrent = 0

               # OWLs crate with an over current alarm (power supply B)
               hvovercurrent_16b = detectordbalarms['HV_over_current_B']

               if hvovercurrent_16b == 1:

                   sys.stdout.write("%s - rscheck():INFO: run %i crate 16 power supply B has an over current alarm\n"
                                    % (datetime.datetime.now().replace(microsecond = 0),run))

                   crateovercurrent = 0

               # At least one crate with has a HV setpoint discrepancy alarm (power supply A)
               hvsetpointdiscrepancy_a = detectordbalarms['HV_setpoint_discrepancy_A']

               for i in range(len(hvsetpointdiscrepancy_a)):

                   if hvsetpointdiscrepancy_a[i] == 1:
                        
                       sys.stdout.write("%s - rscheck():INFO: run %i crate %i has a setpoint discrepancy alarm\n"
                                        % (datetime.datetime.now().replace(microsecond = 0),run,i))
                        
                       cratesetpointdiscrepancy = 0
                        
               # OWls crate with a setpoint discrepancy alarm (power supply B)
               hvsetpointdiscrepancy_16b = detectordbalarms['HV_setpoint_discrepancy_B']

               if hvsetpointdiscrepancy_16b == 1:

                   sys.stdout.write("%s - rscheck():INFO: run %i crate 16 power supply B has a setpoint discrepancy alarm\n"
                                     % (datetime.datetime.now().replace(microsecond = 0),run))
                   
                   cratesetpointdiscrepancy = 0

               if cratecurrentnearzero == 0 or crateovercurrent == 0 or cratesetpointdiscrepancy == 0: cratealarmok = 0

           else:

               cratealarmok = 9

               sys.stdout.write("%s - rscheck():INFO: run %i HV alarms not saved in the DQLL table - please check the detector state page on snopl.us\n"
                                % (datetime.datetime.now().replace(microsecond = 0),run))

        else:

           runduration = 9
	   cratestatus = 9
           cratedac = 9
           cratealarmok = 9

        if runtype == 1 and runduration == 1 and cratestatus == 1 and cratedac == 1: ncratehvok += 1

        if runtype == 1 and runduration == 1 and cratestatus == 1 and cratedac == 1 and cratealarmok == 1: nnohvalarm += 1

	# Write run info in run list
        runlist.write(str(run) + ' || ')
        runlist.write(str(runtype) + str(runduration) + str(cratestatus) + str(cratedac) + str(cratealarmok) + ' | ')
	runlist.write(str(runtype) + '    | ')
	runlist.write(str(runduration) + '    | ')
	runlist.write(str(cratestatus) + '     | ')
	runlist.write(str(cratedac) + '     | ')
        runlist.write(str(cratealarmok) + '      ||')

        # Perform the HL checks
        dqhltools.dqhlPassFailList(run,runlist)

	# Increment p
	p += 1

    runlist.close()

    nphysruns = nruns - nnotphys

    sys.stdout.write("%s - rscheck():INFO: number of runs %i - physics runs %i - with typeok %i - with durationok %i - with cratehvok %i with nohvalarm %i\n"
                         % (datetime.datetime.now().replace(microsecond = 0),nruns,nphysruns,ntypeok,ndurationok,ncratehvok,nnohvalarm))	

    return 0  # Success!

if __name__ == '__main__':
    print sys.exit(main())
