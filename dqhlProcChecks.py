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

# kAmendedMaxEventRate = 1200
kAmendedMaxEventRate = 7000  # Agreed at RS/DQ phone mtg 19/06/2017
kAmendedMaxBitFlipCount = 0
kAmendedMAXMissGTIDCount = 10

def isPhysicsRun(data):
    physicsRun = False
    if (data):
        checks = data['checks']
        if (('dqtellieproc' not in checks) and \
            ('dqsmellieprocproc' not in checks) and \
            ('dqtriggerproc' in checks) and \
            ('dqtimeproc' in checks) and \
            ('dqrunproc' in checks) and \
            ('dqpmtproc' in checks)):
            physicsRun = True

    return physicsRun

# --- Overall DQHL Pass/Fail:

def dqhlChecksOK(data):
    passChecks = 0
    checks = data['checks']
    if ((triggerProcChecksOK(checks['dqtriggerproc']) == 1) and
        (timeProcChecksOK(checks['dqtimeproc']) == 1) and
        (runProcChecksOK(checks['dqrunproc']) == 1) and
        (pmtProcChecksOK(checks['dqpmtproc']) == 1)):
        passChecks = 1
    return passChecks

def modifDqhlChecksOK(runNumber, data):
    passChecks = 0
    checks = data['checks']
    if ((modifTriggerProcChecksOK(checks['dqtriggerproc']) == 1) and
        (modifTimeProcChecksOK(checks['dqtimeproc']) == 1) and
        (modifRunProcChecksOK(runNumber, checks['dqrunproc']) == 1) and
        (pmtProcChecksOK(checks['dqpmtproc']) == 1)):
        passChecks = 1
    return passChecks

# --- From dqtriggerproc: ---

def rsMissingGTIDChecksOK(triggerProc):
    passChecks = 0
    if len(triggerProc['check_params']['missing_gtids']) <= kAmendedMAXMissGTIDCount:
        passChecks = 1
    return passChecks

# Last updated version for all runs
def rsTriggerProcChecksOK(triggerProc):
    passChecks = 0
    if ((triggerProc['n100l_trigger_rate'] == 1) and
        (triggerProc['esumh_trigger_rate'] == 1) and
        (rsMissingGTIDCheckOK(triggerProc) == 1)):
        passChecks = 1
    return passChecks

def modifBitFlipGTIDCountOK(triggerProc):
    bitFlipCountCheck = triggerProc['triggerProcBitFlipGTID']
    bitFlipCountCheck = 0
    bitFlipCount = len(triggerProc['check_params']['bitflip_gtids'])
    if ((bitFlipCount <= kAmendedMaxBitFlipCount) and
        (bitFlipCount >= 0)):
        bitFlipCountCheck = 1
    return bitFlipCountCheck

# This is only for compatibility with the Physics Data Quality web page:
def nominalTriggerProcChecksOK(triggerProc):
    passChecks = 0
    if ((triggerProc['n100l_trigger_rate'] == 1) and
        (triggerProc['esumh_trigger_rate'] == 1) and
        (triggerProc['triggerProcMissingGTID'] == 1) and 
        (triggerProc['triggerProcBitFlipGTID'] == 1)):
        passChecks = 1
    return passChecks

def triggerProcChecksOK(triggerProc):
    passChecks = 0
    if ((triggerProc['n100l_trigger_rate'] == 1) and
        (triggerProc['esumh_trigger_rate'] == 1) and
        (triggerProc['triggerProcMissingGTID'] == 1) and 
        (triggerProc['triggerProcBitFlipGTID'] == 1)):
        passChecks = 1
    return passChecks

# Use this version for RS if processing v1 of runs < 100600:
def modifTriggerProcChecksOK(runNumber, triggerProc):
    passChecks = 0
    if (runNumber >= 101266): 
        passChecks = triggerProcChecksOK(triggerProc)
    else:
        if ((triggerProc['n100l_trigger_rate'] == 1) and
            (triggerProc['esumh_trigger_rate'] == 1) and
            # (triggerProc['triggerProcMissingGTID'] == 1) and 
            # (triggerProc['triggerProcBitFlipGTID'] == 1)):
            (modifBitFlipGTIDCountOK(triggerProc) == 1)):
            passChecks = 1
    return passChecks

# --- From dqtimeproc: ---

def modifEventRateCheckOK(timeProc):
    eventRateCheck = timeProc['event_rate']
    if (eventRateCheck == 0):
        minEventRate = timeProc['criteria']['min_event_rate']
        meanEventRate = timeProc['check_params']['mean_event_rate']
        # deltaTEventRate = timeProc['check_params']['delta_t_event_rate']
        if ((meanEventRate <= kAmendedMaxEventRate) and
            (meanEventRate >= minEventRate)):
            eventRateCheck = 1
    return eventRateCheck

# This version is fine for RS for runs 100600 onwards; if processing v1 of earlier runs,
# use modif version:
def timeProcChecksOK(timeProc):
    passChecks = 0
    if ((timeProc['event_rate'] == 1) and 
        (timeProc['event_separation'] == 1) and
        (timeProc['retriggers'] == 1) and
        (timeProc['run_header'] == 1) and
        (timeProc['10Mhz_UT_comparrison'] == 1) and
        # (timeProc['clock_forward'] == 1) and
        # (timeProc['delta_t_comparison'] == 1)):     # 21 May 2017: No longer used
        (timeProc['clock_forward'] == 1)):
        passChecks = 1
    return passChecks

# Use this version for RS if processing v1 of runs < 100600:
def modifTimeProcChecksOK(runNumber, timeProc):
    passChecks = 0
    # if ((timeProc['event_rate'] == 1) and 
    if ((modifEventRateCheckOK(timeProc) == 1) and 
        (timeProc['event_separation'] == 1) and
        (timeProc['retriggers'] == 1) and
        (timeProc['run_header'] == 1) and
        (timeProc['10Mhz_UT_comparrison'] == 1) and
        # (timeProc['clock_forward'] == 1) and
        # (timeProc['delta_t_comparison'] == 1)):     # 21 May 2017: No longer used
        (timeProc['clock_forward'] == 1)):
        passChecks = 1
    return passChecks

# --- From dqrunproc: ---

# This version is fine for RS for runs 100600 onwards; if processing v1 of earlier runs,
# use modif version:
def runProcChecksOK(runProc):
    passChecks = 0
    if ((runProc['run_type'] == 1) and
        (runProc['mc_flag'] == 1) and
        # (runProc['run_length'] == 1) and            # Ca 18 May 2017: No longer used 
        (runProc['trigger'] == 1)): 
        passChecks = 1
    return passChecks

# Use this version for RS if processing v1 of runs < 100600:
def modifRunProcChecksOK(runNumber, runProc):
    passChecks = 0
    if (runNumber >= 100600):
        passChecks = runProcChecksOK(runProc)
    else:
        if ((runProc['run_type'] == 1) and
            # (runProc['mc_flag'] == 1) and
            # (runProc['run_length'] == 1) and        # Ca 18 May 2017: No longer used
            # (runProc['trigger'] == 1)):             # Pre-ca 18 May 2017: Ignore due to bug 
            (runProc['mc_flag'] == 1)):
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

