#!/usr/bin/env python

#==================================================================
# 
# dqhlProcChecks.py
# E. Falk (E.Falk@sussex.ac.uk)
# 2017-05-15 
#
# Library of functions to determine the overall Pass/Fail result, 
# by processor, of the DQHL checks
# 
#==================================================================

kAmendedMaxEventRate = 1200

# --- From dqtriggerproc: ---

def triggerProcChecksOK(triggerProc):
    passChecks = 0
    if ((triggerProc['n100l_trigger_rate'] == 1) and
        (triggerProc['esumh_trigger_rate'] == 1) and
        (triggerProc['triggerProcMissingGTID'] == 1) and 
        (triggerProc['triggerProcBitFlipGTID'] == 1)):
        passChecks = 1
    return passChecks

def modifTriggerProcChecksOK(triggerProc):
    passChecks = 0
    if ((triggerProc['n100l_trigger_rate'] == 1) and
        (triggerProc['esumh_trigger_rate'] == 1) and
        # (triggerProc['triggerProcMissingGTID'] == 1) and 
        (triggerProc['triggerProcBitFlipGTID'] == 1)):
        passChecks = 1
    return passChecks

# --- From dqtimeproc: ---

def getEventRateFix(timeProc):
    eventRate = timeProc['event_rate']
    if (timeProc['event_rate'] == 0):
        minEventRate = timeProc['criteria']['min_event_rate']
        # meanEventRate = timeProc['check_params']['mean_event_rate']
        deltaTEventRate = timeProc['check_params']['delta_t_event_rate']
        if ((deltaTEventRate <= kAmendedMaxEventRate) and
            (deltaTEventRate >= minEventRate)):
            eventRate = 1
    return eventRate

def timeProcChecksOK(timeProc):
    passChecks = 0
    if ((timeProc['event_rate'] == 1) and 
        (timeProc['event_separation'] == 1) and
        (timeProc['retriggers'] == 1) and
        (timeProc['run_header'] == 1) and
        (timeProc['10Mhz_UT_comparrison'] == 1) and
        (timeProc['clock_forward'] == 1) and
        (timeProc['delta_t_comparison'] == 1)):
        passChecks = 1
    return passChecks

def modifTimeProcChecksOK(timeProc):
    passChecks = 0
    # if ((timeProc['event_rate'] == 1) and 
    if ((getEventRateFix(timeProc) == 1) and 
        (timeProc['event_separation'] == 1) and
        (timeProc['retriggers'] == 1) and
        (timeProc['run_header'] == 1) and
        (timeProc['10Mhz_UT_comparrison'] == 1) and
        # (timeProc['clock_forward'] == 1) and
        # (timeProc['delta_t_comparison'] == 1)):
        (timeProc['clock_forward'] == 1)):
        passChecks = 1
    return passChecks

# --- From dqrunproc: ---

def runProcChecksOK(runProc):
    passChecks = 0
    if ((runProc['run_type'] == 1) and
        (runProc['mc_flag'] == 1) and
        (runProc['run_length'] == 1) and 
        (runProc['trigger'] == 1)): 
        passChecks = 1
    return passChecks

def modifRunProcChecksOK(runProc):
    passChecks = 0
    if ((runProc['run_type'] == 1) and
        (runProc['mc_flag'] == 1) and
        # (runProc['run_length'] == 1) and 
        # (runProc['trigger'] == 1)): 
        (runProc['run_length'] == 1)):
        passChecks = 1
    return passChecks

# --- From dqpmtproc: ---

def pmtProcChecksOK(pmtProc):
    passChecks = 0
    if ((pmtProc['general_coverage'] == 1) and
        (pmtProc['crate_coverage'] == 1) and
        (pmtProc['panel_coverage'] == 1)):
        passChecks = 1
    return passChecks

