# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""

"""

__docformat__ = 'restructuredtext'


import io
import codecs
import logging
import os.path as op
from six import iteritems
from hashlib import md5
from pkg_resources import resource_filename
import shutil

import xml.etree.ElementTree as ET

from datalad.distribution.get import Get

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.support.constraints import (
    EnsureStr,
    EnsureNone,
)
from .dataset import (
    RevolutionDataset as Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.support.param import Parameter
from datalad.support.json_py import (
    load as jsonpyload,
    jsondump as jsonpydump,
)
from datalad.interface.utils import (
    eval_results,
)
from datalad.metadata.metadata import load_ds_aggregate_db
from . import utils as ut

lgr = logging.getLogger('datalad.revolution.web_catalog')

"""

required
description	Text
A short summary describing a dataset.

name	Text
A descriptive name of a dataset. For example, "Snow depth in Northern Hemisphere".

citation	Text or CreativeWork
A citation for a publication that describes the dataset. For example, "J.Smith 'How I created an awesome dataset', Journal of Data Science, 1966".

identifier	URL, Text, or PropertyValue
An identifier for the dataset, such as a DOI.

keywords	Text
Keywords summarizing the dataset.

license	URL, Text
A license under which the dataset is distributed.

sameAs	URL
A link to a page that provides more information about the same dataset, usually in a different repository.

spatialCoverage	Text, Place
You can provide a single point that describes the spatial aspect of the dataset. Only include this property if the dataset has a spatial dimension. For example, a single point where all the measurements were collected, or the coordinates of a bounding box for an area.

Points

"spatialCoverage:" {
  "@type": "Place",
  "geo": {
    "@type": "GeoCoordinates",
    "latitude": 39.3280,
    "longitude": 120.1633
  }
}
Coordinates

Use GeoShape to describe areas of different shapes. For example, to specify a bounding box.

"spatialCoverage:" {
  "@type": "Place",
  "geo": {
    "@type": "GeoShape",
    "box": "39.3280 120.1633 40.445 123.7878"
  }
}
Named locations

"spatialCoverage:" "Tahoe City, CA"
temporalCoverage	Text
The data in the dataset covers a specific time interval. Only include this property if the dataset has a temporal dimension. Schema.org uses the ISO 8601 standard to describe time intervals and time points. You can describe dates differently depending upon the dataset interval. Indicate open-ended intervals with two decimal points (..).

Single date

"temporalCoverage" : "2008"
Time period

"temporalCoverage" : "1950-01-01/2013-12-18"
Open-ended time period

"temporalCoverage" : "2013-12-19/.."
variableMeasured	Text, PropertyValue
The variable that this dataset measures. For example, temperature or pressure.

The variableMeasured property is proposed and pending standardization at schema.org. We encourage publishers to share any feedback on this property with the schema.org community.
version	Text, Number
The version number for the dataset.

url	URL
Location of a page describing the dataset.

>>> autoset >>>
includedInDataCatalog	DataCatalog

>>> tricky, likely must come from parameter >>>
distribution	DataDownload
The description of the location for download of the dataset and the file format for download.

distribution.fileFormat	Text
The file format of the distribution.



<script type="application/ld+json">
{
  "@context":"https://schema.org/",
  "@type":"Dataset",
  "name":"NCDC Storm Events Database",
  "description":"Storm Data is provided by the National Weather Service (NWS) and contain statistics on...",
  "url":"https://catalog.data.gov/dataset/ncdc-storm-events-database",
  "sameAs":"https://gis.ncdc.noaa.gov/geoportal/catalog/search/resource/details.page?id=gov.noaa.ncdc:C00510",
  "keywords":[
     "ATMOSPHERE > ATMOSPHERIC PHENOMENA > CYCLONES",
     "ATMOSPHERE > ATMOSPHERIC PHENOMENA > DROUGHT",
     "ATMOSPHERE > ATMOSPHERIC PHENOMENA > FOG",
     "ATMOSPHERE > ATMOSPHERIC PHENOMENA > FREEZE"
  ],
  "creator":{
     "@type":"Organization",
     "url": "https://www.ncei.noaa.gov/",
     "name":"OC/NOAA/NESDIS/NCEI > National Centers for Environmental Information, NESDIS, NOAA, U.S. Department of Commerce",
     "contactPoint":{
        "@type":"ContactPoint",
        "contactType": "customer service",
        "telephone":"+1-828-271-4800",
        "email":"ncei.orders@noaa.gov"
     }
  },
  "includedInDataCatalog":[{
     "@type":"DataCatalog",
     "name":"data.gov"
  }, {"@type":"DataCatalog", "name":"studyforrest.org"}],
  "distribution":[
     {
        "@type":"DataDownload",
        "encodingFormat":"CSV",
        "contentUrl":"http://www.ncdc.noaa.gov/stormevents/ftp.jsp"
     },
     {
        "@type":"DataDownload",
        "encodingFormat":"XML",
        "contentUrl":"http://gis.ncdc.noaa.gov/all-records/catalog/search/resource/details.page?id=gov.noaa.ncdc:C00510"
     }
  ],
  "temporalCoverage":"1950-01-01/2013-12-18",
  "spatialCoverage":{
     "@type":"Place",
     "geo":{
        "@type":"GeoShape",
        "box":"18.0 -65.0 72.0 172.0"
     }
  }
}
</script>
"""

@build_doc
class ExportWebCatalog(Interface):
    """
    """
    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""specify the dataset to """,
            constraints=EnsureDataset() | EnsureNone()),
        baseurl=Parameter(
            args=("baseurl",),
            doc="""""",
            constraints=EnsureStr()),
        destination=Parameter(
            args=("destination",),
            metavar='PATH',
            doc="""""",
            constraints=EnsureStr() | EnsureNone()),
        homogenization=Parameter(
            args=("--homogenization",),
            doc="""Metadata homogenization mode to produce standardized
            metadata records for the catalog. 'custom' performs no
            homogenization, and takes the content of the 'custom'
            metadata extractor verbatim.""",
            constraints=EnsureStr() | EnsureNone()),
    )

    @staticmethod
    @datasetmethod(name='rev_save')
    @eval_results
    def __call__(baseurl,
                 destination,
                 dataset=None,
                 homogenization='custom',
                 ):
        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='export web catalog')

        destination = ut.Path(destination)
        # make sure it is around
        if not destination.exists():
            destination.mkdir(parents=True)
        elif not destination.is_dir():
            raise ValueError('{} is not a directory'.format(destination))

        obj_dir = destination / 'objs'
        obj_dir.mkdir(exist_ok=True)

        aggdb = load_ds_aggregate_db(ds, abspath=True)
        ds.get(
            a['dataset_info'] for d, a in iteritems(aggdb)
        )

        lookups = {
            # 1:1 mapping, paths are unique in a dataset
            'by_path': {},
            # this is a 1:many mapping, a single dataset can be present
            # in multiple versions
            'by_id': {},
        }

        # sitemap
        sitemap = ET.Element(
            'urlset',
            attrib={
                'xmlns': "http://www.sitemaps.org/schemas/sitemap/0.9",
                'xmlns:xsi': "http://www.w3.org/2001/XMLSchema-instance",
                'xsi:schemaLocation': "http://www.sitemaps.org/schemas/sitemap/0.9 http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd"
            }
        )

        for dsname, dsprops in iteritems(aggdb):
            if 'id' not in dsprops or 'refcommit' not in dsprops:
                lgr.error(
                    'skipped dataset record without an ID or refcommit: %s',
                    dsname)
                continue
            # only the path to a dataset is guaranteed to be unique in a
            # superdataset -> use a hash of it as identifier
            path_md5 = md5(dsname.encode('utf-8')).hexdigest()
            obj_loc = ut.Path(path_md5[:2]) / path_md5[2:]
            obj_destination = obj_dir / obj_loc
            obj_destination.parent.mkdir(exist_ok=True)
            # load dataset metadata
            dsmeta = jsonpyload(dsprops['dataset_info'])
            # tailor the content to match the purpose of data discover
            # via universal terms
            if homogenization == 'custom':
                dsmeta = dsmeta['custom']
            else:
                # TODO implement (configurable) homogenization heuristics
                raise NotImplementedError
            jsondump(
                dsmeta,
                str(obj_destination),
            )
            # in the lookups we always want POSIX convention paths
            # TODO verify that on windows the aggdb paths would be native
            rpath = str(
                ut.PurePosixPath(ut.Path(dsname).relative_to(ds.pathobj))
            )
            lookups['by_path'][rpath] = str(obj_loc)
            byid = lookups['by_id'].get(dsprops['id'], [])
            byid.append(str(obj_loc))
            lookups['by_id'][dsprops['id']] = byid
            # sitemap entry
            url_el = ET.SubElement(sitemap, 'url')
            loc_el = ET.SubElement(url_el, 'loc')
            loc_el.text = '{}?p={}'.format(
                baseurl, rpath)

        # dump the lookup dicts
        for lname, lookup in iteritems(lookups):
            jsondump(
                lookup,
                str(destination / '{}.json'.format(lname)),
            )
        # dump the sitemap
        with open(str(destination / 'catalog.xml'), 'w') as f:
            # TODO 'unicode' may need to be 'utf-8' in PY2
            f.write(ET.tostring(sitemap, encoding='unicode'))
        # common files
        resource_dir = \
            ut.Path(resource_filename('datalad_revolution', '')) / \
            'resources' / 'web_catalog'
        for f in ('catalog.css',
                  'catalog.js',
                  ):
            shutil.copy(str(resource_dir / f), str(destination)
        )
        # embed JS code in catalog page template and add to output
        (destination / 'dataset.html').write_text(
            (resource_dir / 'dataset.html').read_text().replace(
                '<!-- ### INSERT JS CODE HERE ### -->',
                (resource_dir / 'inject.js').read_text()))
        yield {}


def jsondump(data, target):
    """ """
    with io.open(target, 'wb') as f:
        return jsonpydump(
            data,
            codecs.getwriter('utf-8')(f),
            indent=None,
            separators=(',', ':'),
            ensure_ascii=False,
            encoding='utf-8',
        )

