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

"""Keep the structure of something that executes as main_app, so it can
be called from the pipeline composable code, but until another archive
loves it some JSON, leave all the JSON-specific implementation here."""

import importlib
import jsonpickle
import logging
import os
import sys
import traceback

from datetime import datetime

from caom2 import TypedOrderedDict, AbstractCaomEntity
from caom2 import TypedSet, PlaneURI, Part
from caom2utils import get_gen_proc_arg_parser
from caom2pipe import manage_composable as mc
from draost2caom2 import draost_name


__all__ = ['draost_main_app', 'APPLICATION', 'to_caom2']


APPLICATION = 'draost2caom2'


class TypedOrderedDictHandler(jsonpickle.handlers.BaseHandler):
    """The class that handles unpickling of the peculiarly CAOM-specific
    TypedOrderedDicts"""

    def restore(self, data):
        result = None
        unpickler = self.context
        restore = unpickler.restore
        if data['__dict__'] is not None:
            if data['__dict__']['_oktypes'] is not None:
                class_name = data['__dict__']['_oktypes']['py/type']
                if class_name is not None:
                    parts = class_name.split('.')
                    if len(parts) != 3:
                        raise mc.CadcException(
                            f'Unexpected class {class_name}')
                    m = importlib.import_module(parts[0], parts[1])
                    klass = getattr(m, parts[2].strip())
                    result = TypedOrderedDict(klass)
                    for k, v in data.items():
                        if k != '__dict__' and k != 'py/object':
                            value = restore(v, reset=False)
                            value.key = k
                            result.add(value)
        return result


class DateTimeHandler(jsonpickle.handlers.BaseHandler):
    """Class that handles the unpickling of datetime.datetime"""

    def flatten(self, obj, data):
        return obj.isoformat()

    def restore(self, data):
        cls, args = data['__reduce__']
        unpickler = self.context
        restore = unpickler.restore
        cls = restore(cls, reset=False)
        value = datetime.strptime(args[0], '%Y-%m-%dT%H:%M:%S.%f')
        return value


def _set_common(entity, existing):
    """Attributes that aren't in the JSON file."""
    if existing is None:
        entity._id = AbstractCaomEntity._gen_id(fulluuid=False)
    else:
        # handle updates - need the existing id
        entity._id = existing._id
    entity._last_modified = datetime.utcnow()
    entity._max_last_modified = datetime.utcnow()
    entity._meta_checksum = None


def _build_observation(args):
    config = mc.Config()
    config.get_executors()

    existing = None
    if args.in_obs_xml:
        existing = mc.read_obs_from_file(args.in_obs_xml.name)

    drao_name, drao_dir = _get_name(args)
    json_fqn = f'{drao_dir}/{drao_name.obs_id}.json'
    logging.error(f'Looking for metadata in {json_fqn}')
    if not os.path.exists(json_fqn):
        raise mc.CadcException(
            f'Could not find {json_fqn}. Cannot continue without it.')

    with open(json_fqn) as f:
        js = f.read()

    # get the skeleton of the CAOM2 observation
    jsonpickle.handlers.register(TypedOrderedDict, TypedOrderedDictHandler)
    jsonpickle.handlers.register(datetime, DateTimeHandler)
    obs = jsonpickle.decode(js)

    # add the bits of the CAOM2 observation that are required for a
    # structure that's acceptable to /ams - this mostly amounts to
    # ensuring that attributes have been defined on the 'un-pickled'

    _set_common(obs, existing)

    if obs._proposal is not None:
        if not hasattr(obs._proposal, '_project'):
            obs._proposal._project = None
        if not hasattr(obs._proposal, '_name'):
            obs._proposal._name = None
        if not hasattr(obs._proposal, '_keywords'):
            obs._proposal._keywords = set()
        if not hasattr(obs._proposal, '_title'):
            obs._proposal._title = None
    if obs._target is not None:
        if not hasattr(obs._target, '_target_type'):
            obs._target._target_type = None
        if not hasattr(obs._target, '_standard'):
            obs._target._standard = None
        if not hasattr(obs._target, '_redshift'):
            obs._target._redshift = None
        if not hasattr(obs._target, '_moving'):
            obs._target._moving = None
        if not hasattr(obs._target, '_target_id'):
            obs._target._target_id = None
        obs._target._keywords = set()
    obs._requirements = None
    if obs._telescope is not None:
        obs._telescope._keywords = set()
    if obs._instrument is not None:
        obs._instrument._keywords = set()
    obs._environment = None

    if not hasattr(obs, '_meta_read_groups'):
        obs._meta_read_groups = None

    for plane in obs.planes.values():
        if existing is not None:
            _set_common(plane, existing.planes[plane.product_id])
        else:
            _set_common(plane, None)
        plane._acc_meta_checksum = None

        if not hasattr(plane, '_data_read_groups'):
            plane._data_read_groups = None
        if not hasattr(plane, '_meta_read_groups'):
            plane._meta_read_groups = None

        plane._metrics = None
        plane._quality = None
        if plane._provenance is not None:
            plane._provenance._keywords = set()
            plane._provenance._inputs = TypedSet(PlaneURI, )
            if not hasattr(plane._provenance, '_run_id'):
                plane._provenance._run_id = None
            # plane._provenance._last_executed = None
        if hasattr(plane, '_position'):
            if plane._position is not None:
                plane._position._dimension = None
                plane._position._resolution = None
                plane._position._sample_size = None

            if not hasattr(plane._position, '_resolution_bounds'):
                plane._position._resolution_bounds = None
        else:
            plane._position = None
        if hasattr(plane, '_energy'):
            if plane._energy is not None:
                if not hasattr(plane._energy, '_sample_size'):
                    plane._energy._sample_size = None
                if not hasattr(plane._energy, '_bandpass_name'):
                    plane._energy._bandpass_name = None
                if not hasattr(plane._energy, '_transition'):
                    plane._energy._transition = None
                if not hasattr(plane._energy, '_resolving_power'):
                    plane._energy._resolving_power = None
                if not hasattr(plane._energy, '_resolving_power_bounds'):
                    plane._energy._resolving_power_bounds = None

                if hasattr(plane._energy, '_em_band'):
                    plane._energy.energy_bands = None
                    plane._energy.energy_bands.add(plane._energy._em_band)
        else:
            plane._energy = None
        if not hasattr(plane, '_polarization'):
            plane._polarization = None
        if hasattr(plane, '_time'):
            if plane._time is not None:
                if not hasattr(plane._time, '_resolution'):
                    plane._time._resolution = None
                if not hasattr(plane._time, '_resolution_bounds'):
                    plane._time._resolution_bounds = None
        else:
            plane._time = None
        if not hasattr(plane, '_position'):
            plane._position = None
        if not hasattr(plane, '_custom'):
            plane._custom = None

        for artifact in plane.artifacts.values():
            if existing is not None:
                _set_common(
                    artifact,
                    existing.planes[plane.product_id].artifacts[artifact.uri])
            else:
                _set_common(artifact, None)
            artifact._acc_meta_checksum = None
            artifact.parts = TypedOrderedDict(Part,)
            if not hasattr(artifact, '_content_release'):
                artifact._content_release = None
            if not hasattr(artifact, '_content_read_groups'):
                artifact._content_read_groups = None

    if args.out_obs_xml:
        mc.write_obs_to_file(obs, args.out_obs_xml)
    else:
        raise mc.CadcException(f'No where to write for {obs.observation_id}')
    return 0


def _get_name(args):
    if args.local:
        drao_name = draost_name.DraoSTName(
            fname_on_disk=os.path.basename(args.local[0]))
        drao_dir = os.path.dirname(args.local[0])
    else:
        raise mc.CadcException(f'Could not define uri from these args {args}')
    return drao_name, drao_dir


def to_caom2():
    args = get_gen_proc_arg_parser().parse_args()
    result = _build_observation(args)
    logging.debug(f'Done {APPLICATION} processing.')
    return result


def draost_main_app():
    args = get_gen_proc_arg_parser().parse_args()
    try:
        result = to_caom2()
        sys.exit(result)
    except Exception as e:
        logging.error(
            f'Failed {APPLICATION} execution for {args} with {str(e)}.')
        tb = traceback.format_exc()
        logging.error(tb)
        sys.exit(-1)
