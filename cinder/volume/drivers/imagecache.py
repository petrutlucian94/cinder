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

import glob
import os
import shutil

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import fileutils

from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import utils

LOG = logging.getLogger(__name__)

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


class ImageCache(object):
    """Common base class used for caching images"""
    _CACHED_IMAGE_NAME_TEMPLATE = "%s%s.%s"

    def __init__(self, block_size):
        self._block_size = block_size
        self._cache_at_destination = False

    def get_image(self, context, image_service, image_id, destination_path,
                  image_format, image_size, image_subformat=None):
        @utils.synchronized(image_id)
        def _get_image():
            image_cache_dir = self._get_image_cache_dir(destination_path)
            fetch_path = self._get_fetch_path(image_id, destination_path,
                                              image_format, image_subformat,
                                              image_cache_dir)
            if not os.path.exists(fetch_path):
                # Check for cached images having a different format
                cached_images = self._get_cached_images(image_id,
                                                        image_cache_dir)
                if cached_images:
                    self._convert_image(cached_images[0],
                                        fetch_path,
                                        image_format,
                                        image_subformat)
                else:
                    self._fetch_image(context, image_service, image_id,
                                      fetch_path, image_format,
                                      image_subformat)

            self._handle_requested_image(fetch_path,
                                         destination_path,
                                         image_size)
        _get_image()

    def _fetch_image(self, context, image_service, image_id, fetch_path,
                     image_format, image_subformat):
        fileutils.ensure_tree(os.path.dirname(fetch_path))
        image_utils.fetch_to_volume_format(
            context, image_service, image_id, fetch_path, image_format,
            blocksize=self._block_size, volume_subformat=image_subformat)

    def _convert_image(self, image_path, destination_path,
                       image_format, image_subformat):
        image_utils.convert_image(image_path, destination_path,
                                  image_format,
                                  out_subformat=image_subformat)

    def _handle_requested_image(self, fetch_path, destination_path,
                                image_size):
        if destination_path != fetch_path:
            shutil.copyfile(fetch_path, destination_path)

        if self._is_resize_needed(destination_path, image_size):
            self._resize_image(destination_path, image_size)

    def _get_fetch_path(self, image_id, destination_path, image_format,
                        image_subformat, image_cache_dir):
        if CONF.imagecache.cache_fetched_images:
            return self._get_cached_image_path(image_id,
                                               image_format,
                                               image_subformat,
                                               image_cache_dir)
        else:
            return destination_path

    def _get_cached_image_path(self, image_id, image_format,
                               image_subformat, image_cache_dir):
        image_subformat = "-" + image_subformat if image_subformat else ''
        image_file_name = self._CACHED_IMAGE_NAME_TEMPLATE % (
            image_id, image_subformat, image_format)
        return os.path.join(image_cache_dir, image_file_name)

    def _get_cached_images(self, image_id, image_cache_dir):
        pattern = os.path.join(image_cache_dir, image_id) + '*'
        return glob.glob(pattern)

    def _is_resize_needed(self, image_path, requested_size_gb):
        image_size = self._get_image_size(image_path)
        requested_size_bytes = requested_size_gb << 30

        if requested_size_bytes < image_size:
            error_msg = _("Cannot resize image to a smaller size. "
                          "Image size: %(image_size)s, "
                          "requested_size: %(requested_size)s") % {
                              "image_size": image_size,
                              "requested_size": requested_size_bytes}
            raise exception.VolumeBackendAPIException(error_msg)
        elif requested_size_bytes > image_size:
            return True
        return False

    def _resize_image(self, image_path, size_gb):
        image_utils.resize_image(image_path, size_gb)

    def _get_image_size(self, image_path):
        image_info = image_utils.qemu_img_info(image_path)
        return image_info.virtual_size

    def _get_image_cache_dir(self, destination_path):
        if self._cache_at_destination:
            return os.path.dirname(destination_path)
        else:
            return CONF.imagecache.image_cache_dir
