#/usr/bin/env python

""" couchdbtools.py
Tools to read the data-quality table from couchdb

Author: Mark Stringer
        <ms711@sussex.ac.uk>

Last update: 2017-05-23
"""

import couchdb
import json
import os

def getCouchDBDict(server,runNumber):
    dqDB = server["data-quality"]
    data = None
    for row in dqDB.view('_design/data-quality/_view/runs'):
        if(int(row.key) == runNumber):
            runDocId = row['id']
            data = dqDB.get(runDocId)
            return data

