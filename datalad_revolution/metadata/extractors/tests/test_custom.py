# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test custom metadata extractor"""

from six import text_type

from datalad.distribution.dataset import Dataset
# API commands needed
from datalad.api import (
    rev_save,
    query_metadata,
    rev_aggregate_metadata,
)
from datalad.tests.utils import (
    with_tree,
    eq_,
    assert_status,
    assert_result_count,
    assert_in,
    assert_not_in,
    assert_repo_status,
)
from simplejson import dumps as jsondumps

# some metadata to play with, taken from the examples of the google dataset
# search
# this is JSON+LD with plain schema.org terms
sample_jsonld = \
{
    "@context": "https://schema.org/",
    "@type": "Dataset",
    "name": "NCDC Storm Events Database",
    "description": "Storm Data is provided by the National Weather Service (NWS) and contain statistics on...",
    "url": "https://catalog.data.gov/dataset/ncdc-storm-events-database",
    "sameAs": "https://gis.ncdc.noaa.gov/geoportal/catalog/search/resource/details.page?id=gov.noaa.ncdc:C00510",
    "keywords": [
        "ATMOSPHERE > ATMOSPHERIC PHENOMENA > CYCLONES",
        "ATMOSPHERE > ATMOSPHERIC PHENOMENA > DROUGHT",
        "ATMOSPHERE > ATMOSPHERIC PHENOMENA > FOG",
        "ATMOSPHERE > ATMOSPHERIC PHENOMENA > FREEZE"
    ],
    "creator": {
        "@type": "Organization",
        "url":  "https://www.ncei.noaa.gov/",
        "name": "OC/NOAA/NESDIS/NCEI > National Centers for Environmental Information, NESDIS, NOAA, U.S. Department of Commerce",
        "contactPoint": {
            "@type": "ContactPoint",
            "contactType": "customer service",
            "telephone": "+1-828-271-4800",
            "email": "ncei.orders@noaa.gov"
        }
    },
    "includedInDataCatalog": {
        "@type": "DataCatalog",
        "name": "data.gov"
    },
    "distribution": [
        {
            "@type": "DataDownload",
            "encodingFormat": "CSV",
            "contentUrl": "http://www.ncdc.noaa.gov/stormevents/ftp.jsp"
        },
        {
            "@type": "DataDownload",
            "encodingFormat": "XML",
            "contentUrl": "http://gis.ncdc.noaa.gov/all-records/catalog/search/resource/details.page?id=gov.noaa.ncdc:C00510"
        }
    ],
    "temporalCoverage": "1950-01-01/2013-12-18",
    "spatialCoverage": {
        "@type": "Place",
        "geo": {
            "@type": "GeoShape",
            "box": "18.0 -65.0 72.0 172.0"
        }
    }
}

testmeta = {
    "@id": "magic",
    "name": "silence"
}


@with_tree(
    tree={
        '.metadata': {
            'dataset.json': jsondumps(sample_jsonld)},
        'down': {
            'customloc': jsondumps(testmeta)}})
def test_custom_dsmeta(path):
    ds = Dataset(path).create(force=True)
    # enable custom extractor
    # use default location
    ds.config.add('datalad.metadata.nativetype', 'custom', where='dataset')
    ds.rev_save()
    assert_repo_status(ds.path)
    res = ds.rev_aggregate_metadata()
    assert_status('ok', res)
    res = ds.query_metadata(reporton='datasets')
    assert_result_count(res, 1)
    dsmeta = res[0]['metadata']
    assert_in('custom', dsmeta)
    eq_(sample_jsonld, dsmeta['custom'])
    assert_not_in('@id', dsmeta['custom'])

    # overwrite default source location within something non-exiting
    # extraction does not blow up, but no metadata is reported
    ds.config.add(
        'datalad.metadata.custom-dataset-source',
        'nothere',
        where='dataset')
    ds.rev_save()
    # we could argue that any config change should lead
    # to a reaggregation automatically, but that would mean
    # that we are willing to pay a hefty performance price
    # in many situation that do not need re-aggregation
    res = ds.rev_aggregate_metadata(
        force='fromscratch',
        on_failure='ignore')
    assert_result_count(
        res, 1, action='extract_metadata', type='dataset', status='impossible',
        path=ds.path,
        message=(
            'configured custom metadata source is not available in %s: %s',
            ds, 'nothere'),
    )

    res = ds.query_metadata(reporton='datasets')
    assert_result_count(res, 1)
    eq_(res[0]['metadata'].get('custom', {}), {})

    # overwrite default source location within something existing
    ds.config.set(
        'datalad.metadata.custom-dataset-source',
        # always POSIX!
        'down/customloc',
        where='dataset')
    ds.rev_save()
    ds.rev_aggregate_metadata(force='fromscratch')
    res = ds.query_metadata(reporton='datasets')
    assert_result_count(res, 1)
    eq_(testmeta, res[0]['metadata']['custom'])

    # multiple source locations
    ds.config.add(
        'datalad.metadata.custom-dataset-source',
        # put back default
        '.metadata/dataset.json',
        where='dataset')
    ds.rev_save()
    ds.rev_aggregate_metadata(force='fromscratch')
    res = ds.query_metadata(reporton='datasets')
    assert_result_count(res, 1)
    eq_(
        # merge order: testmeta <- sample_jsonld
        dict(testmeta, **sample_jsonld),
        res[0]['metadata']['custom']
    )


@with_tree(
    tree={
        'sub': {
            'one': '1',
            '_one.dl.json': '{"some":"thing"}',
        }
    })
def test_custom_contentmeta(path):
    ds = Dataset(path).create(force=True)
    ds.config.add('datalad.metadata.nativetype', 'custom', where='dataset')
    # use custom location
    ds.config.add('datalad.metadata.custom-content-source',
                  '{freldir}/_{fname}.dl.json',
                  where='dataset')
    ds.rev_save()
    res = ds.rev_extract_metadata(sources=['custom'], process_type='content')
    assert_result_count(
        res, 1,
        path=text_type(ds.pathobj / 'sub' / 'one'),
        type='file',
        status='ok',
        metadata={'custom': {'some': 'thing'}},
        action='extract_metadata'
    )


@with_tree(
    tree={
        '.metadata': {
            'content': {
                'sub': {
                    'one.json': 'not JSON',
                },
            },
        },
        'sub': {
            'one': '1',
        }
    })
def test_custom_content_broken(path):
    ds = Dataset(path).create(force=True)
    ds.config.add('datalad.metadata.nativetype', 'custom', where='dataset')
    ds.rev_save()
    res = ds.rev_extract_metadata(sources=['custom'], process_type='content',
                                  on_failure='ignore')
    assert_result_count(res, 1)
    assert_result_count(
        res, 1,
        path=text_type(ds.pathobj / 'sub' / 'one'),
        type='file',
        # specific message does vary a lot across platforms
        #message=
        status='error',
        action='extract_metadata'
    )
