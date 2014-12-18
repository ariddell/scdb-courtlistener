#!/usr/bin/env python3
import datetime
import json
import os
import re
import tarfile
import tempfile

import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__name__), 'data')
SCDB_FILENAME = os.path.join(DATA_DIR, 'SCDB_2014_01_caseCentered_Citation.csv')
# only match cases between these terms (inclusive)
SCDB_TERM_BEGIN = 1950
SCDB_TERM_END = 2008
SCDB_DECISION_TYPES = {1, 2, 5, 6, 7}

# citation regular expressions
us_cite_re = re.compile(r'^[0-9]{1,3} U\.S\. [0-9]{1,4}')
assert us_cite_re.match('342 U.S. 76')
assert us_cite_re.match('37 U.S. 1189')
sct_cite_re = re.compile(r'^[0-9]{1,4} S\. Ct\. [0-9]{1,4}')
assert sct_cite_re.match('127 S. Ct. 2301')

# load SCDB to double check all cases are present
scdb = pd.read_csv(SCDB_FILENAME, index_col='caseId', encoding='latin1')
scdb = scdb[(scdb.term >= SCDB_TERM_BEGIN) & (scdb.term <= SCDB_TERM_END)]
# Limit our cases of interest to the following decisionTypes
# See http://scdb.wustl.edu/documentation.php?var=decisionType
# 1: opinion of the court (orally argued)
# 2: per curiam (no oral argument)
# 5: equally divided vote
# 6: per curiam (orally argued)
# 7: judgment of the Court (orally argued)
scdb = scdb[scdb.decisionType.isin(SCDB_DECISION_TYPES)]
# we will use dateDecision, so we need to parse it
scdb['dateDecision'] = pd.to_datetime(scdb['dateDecision'], format='%m/%d/%Y')
assert scdb.index.is_unique

# Build flat dataset of CourtListener documents to facilitate document id lookups
courtlistener_records = []
with tarfile.open(os.path.join(DATA_DIR, 'scotus.tar.gz')) as tar:
    for tarinfo in tar:
        f = tar.extractfile(tarinfo)
        record = json.loads(f.read().decode('utf8'))
        date_filed = datetime.datetime.strptime(record['date_filed'], "%Y-%m-%d")
        # ignore cases that are chronologically distant from our time frame
        if date_filed.year < scdb.term.min() - 1 or date_filed.year > scdb.term.max() + 1:
            continue
        document_id = int(record['id'])
        citation_count = int(record['citation_count'])
        document_uris = tuple(record['citation']['document_uris'])
        docket_number = str(record['citation']['docket_number'])
        case_name = str(record['citation']['case_name'])
        us_citations = set(v.strip() for v in record['citation'].values() if isinstance(v, str) and us_cite_re.match(v.strip()))
        if len(us_citations) == 0:
            us_citations.add(float('nan'))
        elif len(us_citations) > 1:
            print(record['citation'])
            raise ValueError("Found more than one US Reports citation for {}".format(document_id))
        sct_citations = set(v for v in record['citation'].values() if isinstance(v, str) and sct_cite_re.match(v))
        if len(sct_citations) == 0:
            sct_citations.add(float('nan'))
        elif len(sct_citations) > 1:
            print(record['citation'])
            raise ValueError("Found more than one Supreme Court Reporter citation for {}".format(document_id))
        courtlistener_records.append((document_id, date_filed, docket_number, us_citations.pop(), sct_citations.pop(), case_name, citation_count, document_uris))
courtlistener = pd.DataFrame.from_records(
    courtlistener_records,
    columns=['document_id', 'date_filed', 'docket_number', 'us_cite', 'sct_cite', 'case_name', 'citation_count', 'document_uris']
).set_index('document_id')
assert courtlistener.index.is_unique

############################################################################
# Incrementally merge the two datasets
############################################################################
print("Goal is to match {} opinions in the SCDB".format(len(scdb)))

# to find matches incrementally create copies of datasets and remove entries as matches are found
scdb_unmerged = scdb.copy()
cl_unmerged = courtlistener.copy()
partial_merges = []

# harmonize format of CourtListener's `docket_number` to SCDB's `docket`
cl_unmerged['docket_number'] = [dn.replace(', Original', ' ORIG') for dn in cl_unmerged['docket_number']]
cl_unmerged['docket_number'] = [dn.replace('___, ORIGINAL', 'ORIG') for dn in cl_unmerged['docket_number']]
cl_unmerged['docket_number'] = [dn.replace(', Orig', ' ORIG') for dn in cl_unmerged['docket_number']]
cl_unmerged['docket_number'] = [dn.replace(', Misc', ' M') for dn in cl_unmerged['docket_number']]
cl_unmerged['docket_number'] = [dn.replace(' Misc', ' M') for dn in cl_unmerged['docket_number']]
cl_unmerged['docket_number'] = [dn.replace('NO. ', '') for dn in cl_unmerged['docket_number']]
cl_unmerged.loc[107757, 'docket_number'] = '1133'  # was '1133, October Term, 1967'
cl_unmerged.loc[109805, 'docket_number'] = '77-88'  # was "Nos. 77-88, 77-126"
cl_unmerged.loc[145898, 'docket_number'] = '105 ORIG'  # was '105 ORIG.'
cl_unmerged.loc[2510329, 'docket_number'] = '6'  # was "NOS. 6 AND 11"

# manually provide a US Reports citation for CourtListener cases where it is missing
cl_unmerged.loc[145898, 'us_cite'] = '556 U.S. 98'  # Kansas v. Colorado

# fix minor idiosyncrasies in SCDB `docket`
scdb_unmerged.loc['1951-018', 'docket'] = '71 M'  # was "71M"
scdb_unmerged.loc['2008-033', 'docket_number'] = '105 ORIG'  # was '105, Orig.'
scdb_unmerged.loc['1953-054', 'docket'] = scdb_unmerged.loc['1953-054', 'docket'].strip()  # whitespace

assert sum(scdb_unmerged['docket'].isnull()) < 10
scdb_unmerged['docket'] = scdb_unmerged['docket'].fillna('___')  # matches "___" used in CourtListener

# where CourtListener has multiple entries with same docket number and US Reports citation, use document
# with highest `citation_count`.
assert sum(cl_unmerged[['us_cite', 'docket_number']].duplicated()) < 400
cl_unmerged = cl_unmerged.sort('citation_count', ascending=False).drop_duplicates(['us_cite', 'docket_number'])


# helper function to grab unambiguous matches
def merge_unambiguous(df1, df2, left_on, right_on):
    """Return matching indexes where an unambigious match from df1 -> df2 is found."""
    if not (df1.index.is_unique and df2.index.is_unique):
        raise ValueError("Both datasets must have unique indexes")
    df1_index_name = df1.index.name
    df2_index_name = df2.index.name
    merged = pd.merge(df1.reset_index(), df2.reset_index(), left_on=left_on, right_on=right_on, how='inner')
    # drop records where a unique match was not found
    merged_nonunique = merged[merged[df1_index_name].duplicated()]
    merged = merged[~merged[df1_index_name].isin(merged_nonunique[df1_index_name])]
    merged = merged[[df1_index_name, df2_index_name]]
    if not (merged.set_index(df1_index_name).index.is_unique and
            merged.set_index(df2_index_name).index.is_unique):
        raise ValueError("Unable to unambiguously match records in datasets.")
    return merged

# match on US Reports citation AND docket number
merged = merge_unambiguous(scdb_unmerged, cl_unmerged, ['usCite', 'docket'], ['us_cite', 'docket_number'])
partial_merges.append(merged)
# remove records from SCDB and CourtListener where we have a match
scdb_unmerged.drop(merged['caseId'], inplace=True)
cl_unmerged.drop(merged['document_id'], inplace=True)
print("Merged {} opinions, {} remain".format(len(scdb) - len(scdb_unmerged), len(scdb_unmerged)))

# match on Supreme Court Reporter citation AND docket number
merged = merge_unambiguous(scdb_unmerged, cl_unmerged, ['sctCite', 'docket'], ['sct_cite', 'docket_number'])
partial_merges.append(merged)
# remove records from SCDB and CourtListener where we have a match
scdb_unmerged.drop(merged['caseId'], inplace=True)
cl_unmerged.drop(merged['document_id'], inplace=True)
print("Merged {} opinions, {} remain".format(len(scdb) - len(scdb_unmerged), len(scdb_unmerged)))

# match on US Reports citation alone
merged = merge_unambiguous(scdb_unmerged, cl_unmerged, ['usCite'], ['us_cite'])
partial_merges.append(merged)
# remove records from SCDB and CourtListener where we have a match
scdb_unmerged.drop(merged['caseId'], inplace=True)
cl_unmerged.drop(merged['document_id'], inplace=True)
print("Merged {} opinions, {} remain".format(len(scdb) - len(scdb_unmerged), len(scdb_unmerged)))

# match on Supreme Court Reporter citation alone
merged = merge_unambiguous(scdb_unmerged, cl_unmerged, ['sctCite'], ['sct_cite'])
partial_merges.append(merged)
# remove records from SCDB and CourtListener where we have a match
scdb_unmerged.drop(merged['caseId'], inplace=True)
cl_unmerged.drop(merged['document_id'], inplace=True)
print("Merged {} opinions, {} remain".format(len(scdb) - len(scdb_unmerged), len(scdb_unmerged)))

# match on decision date/filing date and docket number
merged = merge_unambiguous(scdb_unmerged, cl_unmerged, ['dateDecision', 'docket'], ['date_filed', 'docket_number'])
partial_merges.append(merged)
# remove records from SCDB and CourtListener where we have a match
scdb_unmerged.drop(merged['caseId'], inplace=True)
cl_unmerged.drop(merged['document_id'], inplace=True)
print("Merged {} opinions, {} remain".format(len(scdb) - len(scdb_unmerged), len(scdb_unmerged)))

# save results to disk
scdb_unmerged_fn = os.path.join(tempfile.gettempdir(), 'scdb-unmerged.csv')
print("{} SCDB records have no matches in CourtListener, saving them to {}".format(len(scdb_unmerged), scdb_unmerged_fn))
scdb_unmerged.to_csv(scdb_unmerged_fn)

df = pd.concat(partial_merges)
assert df.set_index('caseId').index.is_unique
assert df.set_index('document_id').index.is_unique

# recover document ids where the opinion spans more than one document,
# in these rare cases one row from the SCDB needs to match multiple document ids
cl_multi = courtlistener.loc[df.document_id]
cl_multi = cl_multi[cl_multi.document_uris.apply(len) > 1]
cl_multi = cl_multi.join(df.set_index('document_id'))
# add new records to df via expansion (needs recent verison of pandas)
df = df.set_index('document_id')
for document_id, record in cl_multi[['caseId', 'document_uris']].iterrows():
    document_ids = {int(re.search(r'/([0-9]+)/', uri).groups()[0]) for uri in record['document_uris']}
    document_ids -= {document_id}
    for i in document_ids:
        df.loc[i] = record['caseId']
# for some reason, the index name gets lost
df.index.name = 'document_id'
df = df.reset_index().set_index('caseId').sort(axis=0)

scdb_courtlistener_fn = os.path.join(tempfile.gettempdir(), 'scdb-courtlistener.csv')
print("{} SCDB records have unique matches in CourtListener, saving them to {}".format(len(scdb) - len(scdb_unmerged), scdb_courtlistener_fn))
df.to_csv(scdb_courtlistener_fn)
