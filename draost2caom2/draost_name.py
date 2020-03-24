# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

import glob
import logging
import os

from caom2pipe import manage_composable as mc

__all__ = ['COLLECTION', 'ARCHIVE', 'DraoSTName']

ARCHIVE = 'DRAO'
COLLECTION = 'DRAO'


class DraoSTName(mc.StorageName):
    """DRAO-ST naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support compressed files in storage

    Remove the majority of the naming handling, because that all arrives
    via the json file from DRAO.

    From Pat, slack 29-01-20:
    As for which: in the past we left it up to the operation to figure out
    and do handle decompression as necessary... I would go with
    application/x-tar in CAOM and when interacting with storage I would
    expect content-type=application/x-tar and content-encoding=gzip to
    provide the correct details in a consistent way.... I cannot recommend
    relying on [libmagic].
    """

    DRAOST_NAME_PATTERN = '*'

    def __init__(self, obs_id=None, fname_on_disk=None, file_name=None):
        obs_id = DraoSTName.get_obs_id(fname_on_disk)
        super(DraoSTName, self).__init__(
            obs_id, ARCHIVE, DraoSTName.DRAOST_NAME_PATTERN, fname_on_disk,
            mime_encoding='gzip', mime_type='application/x-tar')
        self.fname_on_disk = fname_on_disk
        self._f_names_on_disk = None
        self._logger = logging.getLogger(__name__)
        self._logger.debug(self)

    def __str__(self):
        return f'obs id {self.obs_id} file names {self._f_names_on_disk}'

    @property
    def file_uri(self):
        """The ad URI for the file. Assumes compression."""
        return None

    @property
    def product_id(self):
        return None

    @property
    def file_name(self):
        return None

    @property
    def lineage(self):
        return None

    def is_valid(self):
        return self.fname_on_disk.endswith('.json')

    def multiple_files(self, work_dir_fqn):
        self._f_names_on_disk = DraoSTName.get_f_names(
            self.obs_id, work_dir_fqn)
        self._logger.debug(self)
        return self._f_names_on_disk

    @staticmethod
    def get_f_names(obs_id, work_dir_fqn):
        # pattern agreed on with DDR on slack, 29-01-20
        temp = glob.glob(
            f'{work_dir_fqn}/DRAO_ST_*_{obs_id}_*.tar.gz')
        return sorted([os.path.basename(ii) for ii in temp])

    @staticmethod
    def get_obs_id(f_name):
        return DraoSTName.remove_extensions(f_name)

    @staticmethod
    def remove_extensions(f_name):
        return f_name.replace('.gz', '').replace('.tar', '').replace(
            '.json', '')
