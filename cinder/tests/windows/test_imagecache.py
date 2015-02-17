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
from cinder.volume.drivers.windows import constants
from cinder.volume.drivers.windows import imagecache
from cinder.volume.drivers.windows import vhdutils
from cinder.volume.drivers.windows import windows_utils


class WindowsImageCacheTestCase(test.TestCase):
    @mock.patch.object(vhdutils, 'VHDUtils')
    @mock.patch.object(windows_utils, 'WindowsUtils')
    def setUp(self, mock_windows_utils, mock_vhdutils):
        super(WindowsImageCacheTestCase, self).setUp()
        self._imagecache = imagecache.WindowsImageCache()

    @mock.patch.object(imagecache.WindowsImageCache, '_verify_image_format')
    @mock.patch.object(fileutils, 'ensure_tree')
    @mock.patch.object(image_utils, 'fetch_verify_image')
    @mock.patch('os.path.dirname')
    def test_fetch_image(self, mock_dirname, mock_fetch, mock_ensure_tree,
                         mock_verifiy_image_format):
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
            mock.sentinel.image_id, mock.sentinel.fetch_path)
        mock_verifiy_image_format.assert_called_once_with(
            mock.sentinel.fetch_path, mock.sentinel.image_format,
            mock.sentinel.image_subformat)

    @mock.patch.object(imagecache.WindowsImageCache, '_is_resize_needed')
    @mock.patch.object(imagecache.WindowsImageCache, '_resize_image')
    def _test_handle_requested_image(self, mock_resize,
                                     mock_is_resize_needed,
                                     disk_format='vhd',
                                     use_cow_images=False):
        mock_is_resize_needed.return_value = True
        mock_create_diff = self._imagecache._vhdutils.create_differencing_vhd
        self.flags(use_cow_images=use_cow_images, group='imagecache')

        fake_image_name = 'fake_image_name'
        fake_fetch_path = '%s.%s' % (fake_image_name, disk_format)
        requested_size = 1

        self._imagecache._handle_requested_image(
            fake_fetch_path, mock.sentinel.dest_path, requested_size)

        expected_base_path = fake_fetch_path
        expected_resized_image = mock.sentinel.dest_path
        if use_cow_images:
            if disk_format == 'vhd':
                expected_base_path = '%s_%s.%s' % (fake_image_name,
                                                   requested_size,
                                                   disk_format)
                expected_resized_image = expected_base_path
                self._imagecache._utils.copy.assert_called_once_with(
                    fake_fetch_path, expected_resized_image)

            mock_create_diff.assert_called_once_with(
                path=mock.sentinel.dest_path,
                parent_path=expected_base_path)
        else:
            self._imagecache._utils.copy.assert_called_once_with(
                fake_fetch_path, mock.sentinel.dest_path)

        mock_resize.assert_called_once_with(expected_resized_image,
                                            requested_size)

    def test_handle_requested_image_cow_images_disabled(self):
        self._test_handle_requested_image()

    def test_handle_requested_image_using_vhd_cow_images(self):
        self._test_handle_requested_image(use_cow_images=True)

    def test_handle_requested_image_using_vhdx_cow_images(self):
        self._test_handle_requested_image(use_cow_images=True,
                                          disk_format='vhdx')

    def _test_convert_image(self, image_subformat=None):
        if image_subformat:
            fake_vhd_type = constants.VHD_TYPE_MAP[image_subformat]
        else:
            fake_vhd_type = constants.VHD_TYPE_DYNAMIC
        fake_vhd_info = {'ProviderSubtype': fake_vhd_type}
        self._imagecache._vhdutils.get_vhd_info.return_value = fake_vhd_info

        self._imagecache._convert_image(mock.sentinel.image_path,
                                        mock.sentinel.destination_path,
                                        mock.sentinel.image_format,
                                        image_subformat)
        if not image_subformat:
            self._imagecache._vhdutils.get_vhd_info.assert_called_once_with(
                mock.sentinel.image_path)
        self._imagecache._vhdutils.convert_vhd.assert_called_once_with(
            mock.sentinel.image_path,
            mock.sentinel.destination_path,
            fake_vhd_type)

    def test_convert_image_no_subformat_specified(self):
        self._test_convert_image()

    def test_convert_image_to_dynamic_subtype(self):
        self._test_convert_image(
            image_subformat=constants.VHD_SUBFORMAT_DYNAMIC)

    @mock.patch.object(imagecache.WindowsImageCache, '_convert_image')
    @mock.patch.object(image_utils, 'qemu_img_info')
    @mock.patch.object(os, 'unlink')
    @mock.patch.object(os, 'rename')
    def _test_verify_image_format(self, mock_rename, mock_unlink,
                                  mock_qemu_img_info, mock_convert_image,
                                  requested_format='vhd',
                                  requested_subformat=None):
        fake_image_format = 'vhd'
        fake_image_subformat = constants.VHD_TYPE_DYNAMIC
        fake_vhd_info = {'ProviderSubtype': fake_image_subformat}
        fake_image_cache_dir = 'fake_image_cache_dir'
        fake_image_id = 'fake_image_id'
        fake_ext = '.' + fake_image_format
        fake_image_path = os.path.join(fake_image_cache_dir,
                                       fake_image_id + fake_ext)

        self._imagecache._vhdutils.get_vhd_info.return_value = fake_vhd_info
        mock_qemu_img_info.return_value.file_format = fake_image_format

        self._imagecache._verify_image_format(fake_image_path,
                                              requested_format,
                                              requested_subformat)

        wrong_format = fake_image_format != requested_format
        wrong_subformat = (requested_subformat
                           and requested_subformat != fake_image_subformat)

        if wrong_format or wrong_subformat:
            expected_image_name = fake_image_id + '-tmp.' + requested_format
            expected_temp_path = os.path.join(fake_image_cache_dir,
                                              expected_image_name)
            mock_convert_image.assert_called_once_with(
                fake_image_path, expected_temp_path, requested_format,
                requested_subformat)
            mock_unlink.assert_called_once_with(fake_image_path)
            mock_rename.assert_called_once_with(expected_temp_path,
                                                fake_image_path)
        else:
            self.assertFalse(mock_convert_image.called)
            self.assertFalse(mock_unlink.called)
            self.assertFalse(mock_rename.called)

    def test_verify_image_requested_subformat_not_specified(self):
        self._test_verify_image_format()

    def test_verify_image_different_format(self):
        # Test that if the fetched image has a different format than
        # the one requested, the image will be converted accordingly.
        self._test_verify_image_format(requested_format='vhdx')

    def test_verify_image_different_subformat(self):
        self._test_verify_image_format(
            requested_subformat=constants.VHD_SUBFORMAT_FIXED)

    @mock.patch.object(image_utils, 'qemu_img_info')
    def test_verify_image_unsupported_format(self, mock_qemu_img_info):
        mock_qemu_img_info.return_value.file_format = 'fake_image_format'
        self.assertRaises(exception.ImageUnacceptable,
                          self._imagecache._verify_image_format,
                          mock.sentinel.image_path,
                          mock.sentinel.requested_format,
                          mock.sentinel.requested_subformat)
