Link records in the Supreme Court Database and documents in CourtListener
=========================================================================

This repository contains a script that will link records in the Supreme Court
Database (SCDB) with CourtListener *documents*. See `README-data.md` for
further details about retrieving the required raw data (not included in the
repository).

Once the required data is available, the following commands should succeed:

    pip install -r requirements.txt
    python3 merge-scdb-courtlistener.py

And you should see something like:

    84 SCDB records have no matches in CourtListener, saving them to /tmp/scdb-unmerged.csv
    7529 SCDB records have unique matches in CourtListener, saving them to /tmp/scdb-courtlistener.csv
