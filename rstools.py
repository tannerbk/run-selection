#!/usr/bin/env python
"""rstools.py
Tools to write the run selection run list

Authors: Gersende Prior, Elisabeth Falk
        <gersende@lip.pt>, <E.Falk@sussex.ac.uk>
"""


def write_header(file):

    # Print list header:
    file.write("\n")
    file.write("Run no || DQLL Checks                                  || DQHL Checks      |" + \
     	       " Trigger Processor | Time Processor                          |" + \
               " Run Processor      | PMT Processor  \n")
    file.write("----------------------------------------------------------------------" + \
               "--------------------------" + \
               "------------------------------------------" + \
               "-----------------------------------------\n")
    file.write("       || ALL   | Run  | Run  | Crate | Crate | Crate  || TTRP    | TTRP   |" + \
               " N100L ESUMH Miss  |" + \
               " Event GT in  Re-   1st ev 10 MHz  Event |" + \
               " Physics Monte Trig | Ov'all Crate Panel\n")
    file.write("       ||       | Type | Time | HV    | DAC   | alarms || (modif) | (orig) |"+ \
               " rate  rate  GTID  |" + \
               " rate  oth ev trigs time   UT comp order |" + \
               " run     Carlo mask | covg   covg  covg\n")
    file.write("----------------------------------------------------------------------" + \
               "--------------------------" + \
               "------------------------------------------" + \
               "-----------------------------------------\n")
