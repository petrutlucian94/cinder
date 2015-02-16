#  Copyright 2013 Cloudbase Solutions Srl
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os

from oslo_config import cfg

from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder.openstack.common import fileutils
from cinder.volume.drivers import imagecache
from cinder.volume.drivers.windows import constants
from cinder.volume.drivers.windows import vhdutils

image_cache_opts = [
    cfg.StrOpt('image_cache_dir',
               default='$state_path/image_cache',
               help='Directory where base images are cached.'),
    cfg.BoolOpt('cache_fetched_images',
                default=False,
                help=('Caching fetched images can greatly reduce the time '
                      'required to create volumes from images.')),
]

CONF = cfg.CONF
CONF.register_opts(image_cache_opts, 'imagecache')


class WindowsImageCache(imagecache.ImageCache):
    _SUPPORTED_IMAGE_FORMATS = ('vhd', 'vpc', 'vhdx')

    def __init__(self, block_size=None):
        self._vhdutils = vhdutils.VHDUtils()

    def _fetch_image(self, context, image_service, image_id, fetch_path,
                     image_format, image_subformat):
        fileutils.ensure_tree(os.path.dirname(fetch_path))
        image_utils.fetch_verify_image(context, image_service,
                                       image_id, fetch_path)

        self._verify_image_format(fetch_path, image_format, image_subformat)

    def _convert_image(self, image_path, destination_path,
                       image_format, image_subformat):
        if image_subformat:
            vhd_type = constants.VHD_TYPE_MAP[image_subformat]
        else:
            # Keep the same image subtype if not specified otherwise.
            vhd_info = self._vhdutils.get_vhd_info(image_path)
            vhd_type = vhd_info['ProviderSubtype']

        self._vhdutils.convert_vhd(image_path, destination_path,
                                   vhd_type)

    def _verify_image_format(self, image_path, requested_format,
                             requested_subformat):
        info = image_utils.qemu_img_info(image_path)
        image_format = info.file_format

        if image_format not in self._SUPPORTED_IMAGE_FORMATS:
            raise exception.ImageUnacceptable(
                _("Unsupported image format: %s") % image_format)

        vhd_type = self._vhdutils.get_vhd_info(image_path)['ProviderSubtype']
        image_subformat = constants.VHD_SUBFORMAT_MAP[vhd_type]

        wrong_format = image_format != requested_format
        wrong_subformat = (requested_subformat
                           and requested_subformat != image_subformat)

        if wrong_format or wrong_subformat:
            base_path, ext = os.path.splitext(image_path)
            tmp_image_path = base_path + "-tmp." + requested_format
            self._convert_image(image_path, tmp_image_path,
                                requested_format, requested_subformat)
            os.unlink(image_path)
            os.rename(tmp_image_path, image_path)

    def _resize_image(self, image_path, size_gb):
        self._vhdutils.resize_vhd(image_path, size_gb << 30)

    def _get_image_size(self, image_path):
        return self._vhdutils.get_vhd_info(image_path)['VirtualSize']
