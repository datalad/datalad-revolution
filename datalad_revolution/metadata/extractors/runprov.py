# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for provenance information in DataLad's `run` records

Concept
-------

- Find all the commits with a run-record encoded in them
- the commit SHA provides @id for the "activity"
- pull out the author/date info for annotation purposes
- pull out the run record (at the very least to report it straight
  up, but there can be more analysis of the input/output specs in
  the context of the repo state at that point)
- pull out the diff: this gives us the filenames and shasums of
  everything touched by the "activity". This info can then be used
  to look up which file was created by which activity and report
  that in the content metadata

- each run-record is expressed as a PROV bundle, which is stored in the
  'bundles' dict of the PROV record, its key is the recorded commit SHA
  (think: 'run' observed a command do its thing

- for each file there should be a relation declaration (e.g. wasGeneratedBy)
  that links it with an activity in a bundle
  it is still unclear to me how to best compose the per-file record


Make any extractor be able to return a list of any number of documents
(for dataset metadata) to be able to describe more than files and datasets
as entities. Examples are people, run records, measurements...
Give each of them a '@type' by which we can filter.

Enhance extract_metadata/query_metadata to return an @graph list of nodes/documents
with some kind of 'describedBy' properties that encode what extractor was responsible

Here is a draft of a meaningful structure
{
  # prov terms
  "@context": "http://openprovenance.org/prov.jsonld",
  # every record needs an ID, for us this is the dataset ID (or refcommit SHA?)
  # or a file key/sha
  "@id": "ex:uuid",
  # we know datasets and files, and have to say which one this is about
  "@type": "ex:dataset",
  # the rest a PROV concepts via @reverse properties
  # this needs to be read from inside to outside, hence here we declare that
  # some person is attributed to the thing described in this document (a dataset)
  # by means of being an author.
  "entity_attributed": [
    {"@id": "email", "@type": "ex:person", "hadRole": "ex:author"}
  ],
  # each run command is an activity that is known to have "influenced"
  # this dataset (or a file). This is the most generic association, as it is
  # harder to be more specific (generation/derivation) at the level of an
  # entire dataset
  "influencee": [
    { "@id": "ex:sha1", "@type": "prov:Activity",
 "startedAtTime": "2012-03-31T09:21:00.000+01:00",
 "endedAtTime": "2012-04-01T15:21:00.000+01:00"
 },
    { "@id": "ex:sha2", "@type": "prov:Activity",
 "startedAtTime": "2012-03-31T09:21:00.000+01:00",
 "endedAtTime": "2012-04-01T15:21:00.000+01:00"
 }
]
}


{
  "@context": {
    "@base": "http://dx.datalad.org/",
    "@vocab": "http://schema.org/",
      "hasContributors": {"@reverse": "contributor"}
  },
  "@id": "7bec74da-6bf1-11e9-bb11-f0d5bf7b5561",
  "@type": "Dataset",
  "hasContributors": {
    "@id": "michael.hanke@gmail.com",
    "@type": "Person",
    "name": "Michael Hanke",
    "email": "michael.hanke@gmail.com",
    "contributor": {"@id": "7bec74da-6bf1-11e9-bb11-f0d5bf7b5561"}
  }
}
"""


from .base import MetadataExtractor
from six import (
    text_type,
)
from datalad.support.json_py import (
    loads as jsonloads,
    load as jsonload,
)
from datalad.utils import (
    Path,
)

import logging
lgr = logging.getLogger('datalad.metadata.extractors.runprov')


class RunProvenanceExtractor(MetadataExtractor):
    def __call__(self, dataset, process_type, status):
        # shortcut
        ds = dataset

        # lookup dict to find an activity that generated a file at a particular
        # path
        path_db = {}
        # all discovered activities indexed by their commit sha
        activities = {}

        for rec in yield_run_records(ds):
            # run records are coming in latest first
            for d in rec.pop('diff', []):
                if d['path'] in path_db:
                    # records are latest first, if we have an entry, we already
                    # know about the latest change
                    continue
                if d['mode'] == '000000':
                    # this file was deleted, hence it cannot possibly be part
                    # of the to-be-described set of files
                    continue
                # record which activity generated this file
                path_db[d['path']] = dict(
                    activity=rec['gitshasum'],
                    # we need to capture the gitshasum of the file as generated
                    # by the activity to be able to discover later modification
                    # between this state and the to-be-described state
                    gitshasum=d['gitshasum'],
                )
            activities[rec['gitshasum']] = rec

        if process_type in ('all', 'content'):
            for rec in status:
                # see if we have any knowledge about this entry
                # from any of the activity change logs
                dbrec = path_db.get(
                    Path(rec['path']).relative_to(ds.pathobj).as_posix(),
                    {})
                if dbrec.get('gitshasum', None) == rec.get('gitshasum', ''):
                    # the file at this path was generated by a recorded
                    # activity
                    yield dict(
                        rec,
                        metadata=dbrec,
                        type=rec['type'],
                        status='ok',
                    )
                else:
                    # we don't know an activity that made this file, but we
                    # could still report who has last modified it
                    # no we should not, this is the RUN provenance extractor
                    # this stuff can be done by the core extractor
                    pass

        if process_type in ('all', 'dataset'):
            yield dict(
                metadata={
                    '@context': 'https://openprovenance.org/prov.jsonld',
                    'influencee': activities,
                },
                type='dataset',
                status='ok',
            )


def yield_run_records(ds):
    stdout, stderr = ds.repo._git_custom_command(
        None,
        ['git', 'log', '-F',
         '--grep', '=== Do not change lines below ===',
         "--pretty=tformat:%x00%x00record%x00%n%H%x00%aN%x00%aE%x00%aI%n%B%x00%x00diff%x00",
         "--raw", "--no-abbrev",
        ]
    )

    def _finalize_record(r):
        msg, rec = _split_record_message(r.pop('body', []))
        r['message'] = msg
        # TODO this can also just be a runrecord ID in which case we need
        # to load the file and report its content
        rec = jsonloads(rec)
        if not isinstance(rec, dict):
            # this is a runinfo file name
            rec = jsonload(
                text_type(ds.pathobj / '.datalad' / 'runinfo' / rec),
                # TODO this should not be necessary, instead jsonload()
                # should be left on auto, and `run` should save compressed
                # files with an appropriate extension
                compressed=True,
            )
        r['run_record'] = rec
        return r

    record = None
    indiff = False
    for line in stdout.splitlines():
        if line == '\0\0record\0':
            indiff = False
            # fresh record
            if record:
                yield _finalize_record(record)
            record = None
        elif record is None:
            record = dict(zip(
                ('gitshasum', 'author_name', 'author_email', 'commit_date'),
                line.split('\0')
            ))
            record['body'] = []
            record['diff'] = []
        elif line == '\0\0diff\0':
            indiff = True
        elif indiff:
            if not line.startswith(':'):
                continue
            diff = line[1:].split(' ')[:4]
            diff.append(line[line.index('\t') + 1:])
            record['diff'].append(
                dict(zip(
                    ('prev_mode', 'mode', 'prev_gitshasum', 'gitshasum',
                     'path'),
                    diff
                ))
            )
        else:
            record['body'].append(line)
    if record:
        yield _finalize_record(record)


def _split_record_message(lines):
    msg = []
    run = []
    inrec = False
    for line in lines:
        if line == "=== Do not change lines below ===":
            inrec = True
        elif line == "^^^ Do not change lines above ^^^":
            inrec = False
        elif inrec:
            run.append(line)
        else:
            msg.append(line)
    return '\n'.join(msg).strip(), ''.join(run)


# TODO report runrecord directory as content-needed

"""
General PROV document structure

{
    "entity": { // Map of entities by entities' IDs
    },
    "activity": { // Map of activities by IDs
    },
    "agent": { // Map of agents by IDs
    },
    <relationName>: { // A map of relations of type relationName by their IDs
    },
    ...
    "bundle": { // Map of named bundles by IDs
    }
}
"""
