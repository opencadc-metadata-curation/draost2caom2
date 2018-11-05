# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import importlib
import logging
import os
import sys
import traceback

from caom2 import Observation
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser, gen_proc
from caom2pipe import manage_composable as mc
from caom2pipe import execute_composable as ec


__all__ = ['main_app', 'update', 'DraoSTName', 'COLLECTION', 'APPLICATION']


APPLICATION = 'draost2caom2'
COLLECTION = 'DRAO'


class DraoSTName(ec.StorageName):
    """DRAO-ST naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support uncompressed files in storage
    """

    DRAOST_NAME_PATTERN = '*'

    def __init__(self, obs_id=None, fname_on_disk=None, file_name=None):
        self.fname_in_ad = file_name
        super(DraoSTName, self).__init__(
            obs_id, COLLECTION, DraoSTName.DRAOST_NAME_PATTERN, fname_on_disk)

    def is_valid(self):
        return True


def accumulate_bp(bp, uri):
    """Configure the DRAO-ST-specific ObsBlueprint at the CAOM model Observation
    level."""
    logging.debug('Begin accumulate_bp.')
    # bp.configure_position_axes((1, 2))
    # bp.configure_time_axis(3)
    # bp.configure_energy_axis(4)
    # bp.configure_polarization_axis(5)
    # bp.configure_observable_axis(6)
    bp.set('Observation.observationID', 'abc')
    bp.set('Observation.metaRelease', '1990-01-01')
    bp.set('Observation.type', 'TBD')
    bp.set('Observation.intent', 'science')

    bp.set('Observation.algorithm.name', 'exposure')

    bp.set('Observation.telescope.name', 'Synthesis Telescope')
    bp.set('Observation.telescope.geoLocationX', -2100330.8751700274)
    bp.set('Observation.telescope.geoLocationY', -3694247.8244465934)
    bp.set('Observation.telescope.geoLocationZ', 4741018.330967472)

    bp.set('Observation.instrument.name', 'TBD')
    bp.set('Observation.instrument.keywords', 'TBD')

    bp.set('Observation.environment.ambientTemp', -2.5)

    bp.set('Observation.proposal.id', 'proposal id')
    bp.set('Observation.proposal.pi', 'Dr Drao')
    bp.set('Observation.proposal.project', 'Proposal Project')
    bp.set('Observation.proposal.title', 'Proposal Title')
    bp.set('Observation.proposal.keywords', 'Proposal Keywords, comma separated')

    bp.set('Observation.target.name', 'Target Name')
    bp.set('Observation.target.type', 'Target Type')
    bp.set('Observation.target.standard', 'True or False')
    bp.set('Observation.target.redshift', 'TBD')
    bp.set('Observation.target.keywords', 'comma separated')
    bp.set('Observation.target.moving', 'True or False')

    bp.set('Observation.target_position.point.cval1', '-1.0')
    bp.set('Observation.target_position.point.cval2', '-1.0')
    bp.set('Observation.target_position.coordsys', 'FK5')
    bp.set('Observation.target_position.equinox', '2000.0')

    bp.set('Plane.productID', 'TBD')
    bp.set('Plane.metaRelease', '1990-01-01')
    bp.set('Plane.dataRelease', '2030-01-01')
    bp.set('Plane.dataProductType', 'image')
    bp.set('Plane.calibrationLevel', '2')

    bp.set('Plane.provenance.name', 'TBD')
    bp.set('Plane.provenance.project', 'TBD')
    bp.set('Plane.provenance.producer', 'DRAO')
    bp.set('Plane.provenance.reference', 'DRAO')
    bp.set('Plane.provenance.lastExecuted', '2018-10-31 01:23:45')

    bp.set('Artifact.productType', 'science')
    bp.set('Artifact.releaseType', 'data')
    bp.set('Artifact.contentChecksum', 'md5:01234567890abcdef0123456789bcdef')
    bp.set('Artifact.contentLength', 42)
    bp.set('Artifact.contentType', 'application/gzip')
    bp.set('Artifact.uri', 'ad:DRAO/abc.tar.gz')

    import yaml
    with open('bp.yml', 'w') as outfile:
        yaml.dump(bp._plan, outfile, default_flow_style=False)
    logging.debug('Done accumulate_bp.')


def update(observation, **kwargs):
    """Called to fill multiple CAOM model elements and/or attributes, must
    have this signature for import_module loading and execution.

    :param observation A CAOM Observation model instance.
    :param **kwargs Everything else."""
    logging.debug('Begin update.')
    mc.check_param(observation, Observation)

    headers = None
    if 'headers' in kwargs:
        headers = kwargs['headers']
    fqn = None
    if 'fqn' in kwargs:
        fqn = kwargs['fqn']

    logging.debug('Done update.')
    return True


def _update_typed_set(typed_set, new_set):
    # remove the previous values
    while len(typed_set) > 0:
        typed_set.pop()
    typed_set.update(new_set)


def _build_blueprints(uri):
    """This application relies on the caom2utils fits2caom2 ObsBlueprint
    definition for mapping FITS file values to CAOM model element
    attributes. This method builds the DRAO-ST blueprint for a single
    artifact.

    The blueprint handles the mapping of values with cardinality of 1:1
    between the blueprint entries and the model attributes.

    :param uri The artifact URI for the file to be processed."""
    module = importlib.import_module(__name__)
    blueprint = ObsBlueprint(module=module)
    accumulate_bp(blueprint, uri)
    blueprints = {uri: blueprint}
    return blueprints


def _get_uri(args):
    result = None
    if args.observation:
        result = DraoSTName(args.observation[1]).file_uri
    elif args.local:
        obs_id = DraoSTName.remove_extensions(os.path.basename(args.local[0]))
        result = DraoSTName(obs_id).get_file_uri()
    elif args.lineage:
        result = args.lineage[0].split('/', 1)[1]
    else:
        raise mc.CadcException(
            'Could not define uri from these args {}'.format(args))
    return result


def _build_observation():
    """An example how to use the python caom2 library to create a
    Synthesis Telescope observation.
    """
    from astropy.time import Time
    from caom2pipe import astro_composable as ac
    from datetime import datetime
    from caom2 import SimpleObservation, Algorithm, ObservationIntentType
    from caom2 import Instrument, Proposal, Target, TargetType
    from caom2 import TargetPosition, Telescope, Environment, Plane
    from caom2 import DataProductType, CalibrationLevel, Provenance, Metrics
    from caom2 import DataQuality, Quality, Artifact, ProductType
    from caom2 import ReleaseType, ChecksumURI, SegmentType, Point, Vertex
    from caom2 import shape, Position, Energy, EnergyBand, Polarization
    from caom2 import PolarizationState, Requirements, Status
    from caom2 import Time as caom_Time

    # collection 'DRAO' is a constant
    #
    # observation_id - this plus collection is the unique identifier for an
    # CAOM2 instance, so it must be unique within all the DRAO CAOM2
    # instances.
    #
    # Simple Observations have an algorithm of 'exposure'
    # Composite Observations have an algorithm of 'composite'
    #
    # create Composite Observations if Simple Observations already exist,
    # that can be named as members of the Composite Observation
    obs = SimpleObservation(collection='DRAO',
                            observation_id='sample_observation_id',
                            algorithm=Algorithm(name='exposure'))
    obs.sequence_number = 1
    # calibration or science
    obs.intent = ObservationIntentType.SCIENCE
    # in FITS, the value of 'OBSTYPE' or equivalent keyword
    obs.type = 'FIELD'
    # TODO - what effect does this have on observation visibility in AS?
    obs.meta_release = datetime(1990, 1, 1)
    obs.instrument = Instrument(name='DRAO-ST')
    obs.instrument.keywords.add('eg')
    obs.instrument.keywords.add('shutter state')
    obs.proposal = Proposal(id='proposal id',
                            pi_name='John A Galt',
                            project='Survey Name',
                            title='Proposal ID Title')
    obs.proposal.keywords.add('eg')
    obs.proposal.keywords.add('low mass star formation')
    obs.target = Target(name='MC2',
                        target_type=TargetType.FIELD,  # or OBJECT
                        standard=False,
                        redshift=1.2,
                        moving=False)
    obs.target_position = TargetPosition(coordinates=None,
                                         coordsys=None,
                                         equinox=None)
    # telescope location must be in geo-centric coordinates
    # this is the same for all CAOM2 instances.
    x, y, z = ac.get_location(latitude=48.320000,
                              longitude=-119.620000,
                              elevation=545.0)
    obs.telescope = Telescope(name='DRAO-ST',
                              geo_location_x=x,
                              geo_location_y=y,
                              geo_location_z=z)

    obs.environment = Environment()
    obs.environment.seeing = None
    obs.environment.humidity = None
    obs.environment.elevation = None
    obs.environment.tau = None
    obs.environment.wavelength_tau = None
    obs.environment.ambient_temp = None
    obs.environment.photometric = None
    # do not set this field if the PI's observing goal has been met
    obs.requirements = Requirements(flag=Status.FAIL)

    # create one plane per unique thing obtained from the same photons
    # product id must be unique within a CAOM2 instance
    plane = Plane(product_id='1420MHz-QU',
                  creator_id=None,
                  meta_release=datetime(2011, 1, 1),  # public metadata
                  data_release=datetime(2030, 1, 1),  # proprietary data
                  data_product_type=DataProductType.IMAGE,
                  calibration_level=CalibrationLevel.CALIBRATED)
    plane.provenance = Provenance(name='CGPS MOSAIC',
                                  version='42.43.44567',
                                  project='CGPS',
                                  producer='CGPS Consortium',
                                  run_id='1',
                                  reference='http://dx.doi.org/10.1086/375301',
                                  last_executed=datetime.utcnow())
    plane.metrics = Metrics()
    plane.metrics.source_number_density = None
    plane.metrics.background = None
    plane.metrics.background_std_dev = None
    plane.flux_density_limit = None
    plane.mag_limit = None
    # do not set this field if the telescope's QA assessment has passed
    plane.quality = DataQuality(Quality.JUNK)

    # build a plane.position from a CD matrix - this makes
    # the CAOM2 instance findable from a spatial search
    #
    # default units are degrees
    #   - resolution: arcsec
    points = [Point(cval1=155.125320, cval2=15.553371),
              Point(154.546128, 15.556190),
              Point(154.548561, 16.114706),
              Point(155.129357, 16.111878)]
    vertices = [Vertex(cval1=points[0].cval1,
                       cval2=points[0].cval2,
                       type=SegmentType.MOVE),
                Vertex(points[1].cval1, points[1].cval2, SegmentType.LINE),
                Vertex(points[2].cval1, points[2].cval2, SegmentType.LINE),
                Vertex(points[3].cval1, points[3].cval2, SegmentType.LINE),
                Vertex(points[0].cval1, points[0].cval2, SegmentType.CLOSE)]
    polygon = shape.Polygon(
        points=points, samples=shape.MultiPolygon(vertices))
    position = Position(time_dependent=False,
                        bounds=polygon)
    plane.position = position

    # build plane.energy - makes the CAOM2 instance findable from an
    # energy search
    #
    # default units are m (barycentric wavelength)
    #   - frequency: width Hz
    #   - frequency: sample size Hz
    samples = [shape.SubInterval(lower=0.07427706352989176,
                                 upper=0.15265452235324573)]
    bounds = shape.Interval(lower=0.07427706352989176,
                            upper=0.15265452235324573,
                            samples=samples)
    energy = Energy(bounds=bounds,
                    dimension=1,  # number of pixels
                    resolving_power=None,
                    sample_size=0.07837745882335397,
                    bandpass_name='S-band',
                    em_band=EnergyBand.RADIO,
                    transition=None,
                    restwav=None)
    plane.energy = energy

    # build plane.polarization - makes the CAOM2 instance findable from a
    # polarization search
    polarization_states = [PolarizationState.I]
    polarization = Polarization(dimension=1,
                                polarization_states=polarization_states)
    plane.polarization = polarization

    # build plane.time - makes the CAOM2 instance findable from a time search
    # default units are days
    #   - exposure: seconds
    time_data = [[Time('2009-09-07'), Time('2009-09-21')],
                 [Time('2009-11-30'), Time('2009-12-09')],
                 [Time('2010-02-23'), Time('2010-03-09')]]
    for ii in range(0, 3):
        time_data[ii][0].format = 'mjd'
        time_data[ii][1].format = 'mjd'
    exposure_time = time_data[2][1] - time_data[0][0]
    samples = [shape.SubInterval(lower=float(time_data[0][0].value),
                                 upper=float(time_data[0][1].value)),
               shape.SubInterval(float(time_data[1][0].value),
                                 float(time_data[1][1].value)),
               shape.SubInterval(float(time_data[2][0].value),
                                 float(time_data[2][1].value))]
    bounds = shape.Interval(lower=float(time_data[0][0].value),
                            upper=float(time_data[2][1].value),
                            samples=samples)
    caom_time = caom_Time(bounds=bounds,
                          dimension=len(samples),
                          resolution=exposure_time.to('second').value,
                          sample_size=exposure_time.to('day').value,
                          exposure=exposure_time.to('second').value)
    plane.time = caom_time

    # create one artifact per file
    #
    # ReleaseType.META means the plane.meta_release value will be used
    # to govern the visibility of the CAOM2 instance in Advanced Search.
    # ReleaseType.DATA means the plane.data_release value will be used
    # to govern the visibility of the CAOM2 instance and the associated file.
    # In the DATA case, the metadata is public.
    #
    # parts=None because there will be no cutout support from the tar file
    meta = mc.get_file_meta('/tmp/sample_observation.tar.gz')
    artifact = Artifact(uri='ad:DRAO/sample_observation.tar.gz',
                        product_type=ProductType.SCIENCE,
                        release_type=ReleaseType.META,
                        content_type='application/gzip',
                        content_length=meta['size'],
                        content_checksum=ChecksumURI(
                            'md5:{}'.format(meta['md5sum'])),
                        parts=None)

    plane.artifacts.add(artifact)
    obs.planes.add(plane)

    # write the observation to a file on disk in XML format, which
    # is what the /ams service requires as input
    mc.write_obs_to_file(obs=obs,
                         fqn='/tmp/sample_observation.xml')

    # mandatory fields are:
    #
    # obs.collection
    # obs.observation_id
    # obs.intent
    # obs.algorithm.name
    # obs.instrument.name
    #
    # obs.planes[0].product_id
    # obs.planes[0].quality
    #
    # obs.planes[0].artifacts[0].uri
    # obs.planes[0].artifacts[0].product_type
    # obs.planes[0].artifacts[0].release_type
    #
    # obs.proposal is not mandatory, but if one is provided, mandatory fields
    # are:
    # obs.proposal.id
    #
    # obs.target is not mandatory, but if one is provided, mandatory fields
    # are:
    # obs.target.name
    #
    # obs.target_position is not mandatory, but if one is provided,
    # mandatory fields are:
    # obs.target_position.coordsys
    # obs.target_position.coordinates
    #
    # obs.telescope is not mandatory, but if one is provided, mandatory fields
    # are:
    # obs.telescope.name
    #
    # obs.requirements is not mandatory, but if one is provided, mandatory
    # fields are:
    # obs.requirements.flag
    #
    # plane.provenance is not mandatory, but if one is provided, mandatory
    # fields are:
    # plane.provenance.name
    #


def main_app():
    args = get_gen_proc_arg_parser().parse_args()
    try:
        uri = _get_uri(args)
        blueprints = _build_blueprints(uri)
        gen_proc(args, blueprints)
    except Exception as e:
        logging.error('Failed {} execution for {}.'.format(APPLICATION, args))
        tb = traceback.format_exc()
        logging.error(tb)
        sys.exit(-1)

    _build_observation()
    logging.debug('Done {} processing.'.format(APPLICATION))
