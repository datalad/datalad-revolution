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
"""


from .base import MetadataExtractor
from datalad.support.json_py import loads as jsonloads

import logging
lgr = logging.getLogger('datalad.metadata.extractors.runprov')


class RunProvenanceExtractor(MetadataExtractor):
    def __call__(self, dataset, process_type, status):
        # shortcut
        ds = dataset

        stdout, stderr = ds.repo._git_custom_command(
            None,
            ['git', 'log', '-F',
             '--grep', '=== Do not change lines below ===',
             "--pretty=tformat:%x00%x00record%x00%n%H%x00%aN%x00%aE%x00%aI%n%B%x00%x00diff%x00",
             "--raw", "--no-abbrev",
            ]
        )
        records = []
        record = None
        indiff = False
        for line in stdout.splitlines():
            if line == '\0\0record\0':
                indiff = False
                # fresh record
                if record:
                    records.append(record)
                record = None
            elif record is None:
                record = dict(zip(
                    ('@id', 'author', 'email', 'date'),
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
                record['diff'].append(diff)
            else:
                record['body'].append(line)
        if record:
            records.append(record)

        for r in records:
            msg, rec = _split_record_message(r.pop('body', []))
            r['msg'] = msg
            # TODO this can also just be a runrecord ID in which case we need
            # to load the file and report its content
            r['run'] = jsonloads(rec)
        yield dict(
            metadata=dict(records=records),
            type='dataset',
            status='ok',
        )


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
