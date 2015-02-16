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

import mock

import os

from cinder import exception
from cinder.image import image_utils
from cinder.openstack.common import fileutils
from cinder import test
from cinder.volume.drivers import imagecache


class ImageCacheTestCase(test.TestCase):
    _FAKE_IMAGE_SIZE_GB = 2

    def setUp(self):
        super(ImageCacheTestCase, self).setUp()
        self._imagecache = imagecache.ImageCache(mock.sentinel.block_size)

    @mock.patch('os.path.exists')
    @mock.patch.object(imagecache.ImageCache, '_get_fetch_path')
    @mock.patch.object(imagecache.ImageCache, '_get_cached_images')
    @mock.patch.object(imagecache.ImageCache, '_convert_image')
    @mock.patch.object(imagecache.ImageCache, '_fetch_image')
    @mock.patch.object(imagecache.ImageCache, '_handle_requested_image')
    def _test_get_image(self, mock_handle_image, mock_fetch,
                        mock_convert_image, mock_get_cached_images,
                        mock_get_fetch_path, mock_exists,
                        cached_images=False):
        mock_get_fetch_path.return_value = mock.sentinel.fetch_path
        mock_exists.return_value = False
        fake_cached_images = []
        if cached_images:
            fake_cached_images.append(mock.sentinel.cached_image_path)
        mock_get_cached_images.return_value = fake_cached_images

        # with mock.patch.object(utils, 'synchronized', fake_synchronized):
        self._imagecache.get_image(mock.sentinel.context,
                                   mock.sentinel.image_service,
                                   mock.sentinel.image_id,
                                   mock.sentinel.destination_path,
                                   mock.sentinel.image_format,
                                   mock.sentinel.image_size,
                                   mock.sentinel.image_subformat)

        if cached_images:
            mock_convert_image.assert_called_once_with(
                mock.sentinel.cached_image_path,
                mock.sentinel.fetch_path,
                mock.sentinel.image_format,
                mock.sentinel.image_subformat)
        else:
            mock_fetch.assert_called_once_with(mock.sentinel.context,
                                               mock.sentinel.image_service,
                                               mock.sentinel.image_id,
                                               mock.sentinel.fetch_path,
                                               mock.sentinel.image_format,
                                               mock.sentinel.image_subformat)
        mock_handle_image.assert_called_once_with(
            mock.sentinel.fetch_path, mock.sentinel.destination_path,
            mock.sentinel.image_size)

    def test_get_uncached_image(self):
        self._test_get_image()

    def test_get_cached_image_wrong_format(self):
        self._test_get_image(cached_images=True)

    @mock.patch.object(fileutils, 'ensure_tree')
    @mock.patch.object(image_utils, 'fetch_to_volume_format')
    @mock.patch('os.path.dirname')
    def test_fetch_image(self, mock_dirname, mock_fetch, mock_ensure_tree):
        mock_dirname.return_value = mock.sentinel.fetch_path_dir_name

        self._imagecache._fetch_image(mock.sentinel.context,
                                      mock.sentinel.image_service,
                                      mock.sentinel.image_id,
                                      mock.sentinel.fetch_path,
                                      mock.sentinel.image_format,
                                      mock.sentinel.image_subformat)

        mock_ensure_tree.assert_called_once_with(
            mock.sentinel.fetch_path_dir_name)
        mock_fetch.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.image_service,
            mock.sentinel.image_id, mock.sentinel.fetch_path,
            mock.sentinel.image_format,
            volume_subformat=mock.sentinel.image_subformat,
            blocksize=mock.sentinel.block_size)

    @mock.patch.object(image_utils, 'convert_image')
    def test_convert_image(self, mock_convert_image):
        self._imagecache._convert_image(mock.sentinel.image_path,
                                        mock.sentinel.destination_path,
                                        mock.sentinel.image_format,
                                        mock.sentinel.image_subformat)
        mock_convert_image.assert_called_once_with(
            mock.sentinel.image_path,
            mock.sentinel.destination_path,
            mock.sentinel.image_format,
            out_subformat=mock.sentinel.image_subformat)

    @mock.patch.object(imagecache.ImageCache, '_get_cached_image_path')
    def _test_get_fetch_path(self, mock_get_cached_image_path,
                             cache_images=False):
        self.flags(cache_fetched_images=cache_images, group='imagecache')

        fetch_path = self._imagecache._get_fetch_path(
            mock.sentinel.image_id, mock.sentinel.destination_path,
            mock.sentinel.image_format, mock.sentinel.image_subformat,
            mock.sentinel.image_cache_dir)

        if cache_images:
            mock_get_cached_image_path.assert_called_once_with(
                mock.sentinel.image_id, mock.sentinel.image_format,
                mock.sentinel.image_subformat,
                mock.sentinel.image_cache_dir)
            expected_path = mock_get_cached_image_path.return_value
        else:
            expected_path = mock.sentinel.destination_path

        self.assertEqual(expected_path, fetch_path)

    def test_get_fetch_path_without_caching(self):
        self._test_get_fetch_path()

    def test_get_fetch_path_caching_enabled(self):
        self._test_get_fetch_path(cache_images=True)

    def test_get_cached_image_path(self):
        fake_image_id = 'fake_image_id'
        fake_image_format = 'vhd'
        fake_image_subformat = 'fixed'
        fake_cache_dir = 'fake_cache_dir'

        image_path = self._imagecache._get_cached_image_path(
            fake_image_id, fake_image_format, fake_image_subformat,
            fake_cache_dir)
        expected_file_name = '%s-%s.%s' % (fake_image_id,
                                           fake_image_subformat,
                                           fake_image_format)
        expected_path = os.path.join(fake_cache_dir, expected_file_name)
        self.assertEqual(expected_path, image_path)

    @mock.patch.object(imagecache.ImageCache, '_get_image_size')
    def _test_check_if_resize_needed(self, mock_get_size,
                                     requested_size_gb):
        mock_get_size.return_value = self._FAKE_IMAGE_SIZE_GB << 30

        if requested_size_gb < self._FAKE_IMAGE_SIZE_GB:
            self.assertRaises(exception.VolumeBackendAPIException,
                              self._imagecache._is_resize_needed,
                              mock.sentinel.image_path,
                              requested_size_gb)
        else:
            resize_needed = self._imagecache._is_resize_needed(
                mock.sentinel.image_path, requested_size_gb)
            self.assertEqual(requested_size_gb > self._FAKE_IMAGE_SIZE_GB,
                             resize_needed)
        mock_get_size.assert_called_once_with(mock.sentinel.image_path)

    def test_check_if_resize_needed_bigger_size(self):
        self._test_check_if_resize_needed(
            requested_size_gb=self._FAKE_IMAGE_SIZE_GB + 1)

    def test_check_if_resize_needed_smaller_size(self):
        self._test_check_if_resize_needed(
            requested_size_gb=self._FAKE_IMAGE_SIZE_GB - 1)

    def test_check_if_resize_needed_same_size(self):
        self._test_check_if_resize_needed(
            requested_size_gb=self._FAKE_IMAGE_SIZE_GB)
