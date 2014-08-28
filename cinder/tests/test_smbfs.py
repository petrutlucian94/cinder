#  Copyright 2014 Cloudbase Solutions Srl
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

import contextlib
import copy
import mock
import os

from cinder import exception
from cinder import test

from cinder.image import image_utils
from cinder.volume.drivers import smbfs


class SmbFsTestCase(test.TestCase):

    _FAKE_SHARE = '//1.2.3.4/share1'
    _FAKE_MNT_BASE = '/mnt'
    _FAKE_HASH = 'db0bf952c1734092b83e8990bd321131'
    _FAKE_VOLUME_NAME = 'volume-4f711859-4928-4cb7-801a-a50c37ceaccc'
    _FAKE_TOTAL_SIZE = '2048'
    _FAKE_TOTAL_AVAILABLE = '1024'
    _FAKE_TOTAL_ALLOCATED = 1024
    _FAKE_VOLUME = {'id': '4f711859-4928-4cb7-801a-a50c37ceaccc',
                    'size': 1,
                    'provider_location': _FAKE_SHARE,
                    'name': _FAKE_VOLUME_NAME,
                    'status': 'available'}
    _FAKE_MNT_POINT = os.path.join(_FAKE_MNT_BASE, _FAKE_HASH)
    _FAKE_VOLUME_PATH = os.path.join(_FAKE_MNT_POINT, _FAKE_VOLUME_NAME)
    _FAKE_SNAPSHOT_ID = '5g811859-4928-4cb7-801a-a50c37ceacba'
    _FAKE_SNAPSHOT = {'id': _FAKE_SNAPSHOT_ID,
                      'volume': _FAKE_VOLUME,
                      'status': 'available',
                      'volume_size': 1}
    _FAKE_SNAPSHOT_PATH = (
        _FAKE_VOLUME_PATH + '-snapshot' + _FAKE_SNAPSHOT_ID)
    _FAKE_SHARE_OPTS = '-o username=Administrator,password=12345'
    _FAKE_OPTIONS_DICT = {'username': 'Administrator',
                          'password': '12345'}

    _FAKE_LISTDIR = [_FAKE_VOLUME_NAME, _FAKE_VOLUME_NAME + '.vhd',
                     _FAKE_VOLUME_NAME + '.vhdx', 'fake_folder']
    _FAKE_SMBFS_CONFIG = mock.MagicMock()
    _FAKE_SMBFS_CONFIG.smbfs_oversub_ratio = 2
    _FAKE_SMBFS_CONFIG.smbfs_used_ratio = 0.5
    _FAKE_SMBFS_CONFIG.smbfs_shares_config = '/fake/config/path'
    _FAKE_SMBFS_CONFIG.smbfs_default_volume_format = 'raw'
    _FAKE_SMBFS_CONFIG.smbfs_sparsed_volumes = False

    def setUp(self):
        super(SmbFsTestCase, self).setUp()
        smbfs.SmbfsDriver.__init__ = lambda x: None
        self._smbfs_driver = smbfs.SmbfsDriver()
        self._smbfs_driver._remotefsclient = mock.MagicMock()
        self._smbfs_driver.local_volume_dir = mock.Mock(
            return_value=self._FAKE_MNT_POINT)
        self._smbfs_driver._execute = mock.MagicMock()

    def test_delete_volume(self):
        fake_vol_info = self._FAKE_VOLUME_PATH + '.info'

        fake_unlink = mock.MagicMock()
        self._smbfs_driver._ensure_share_mounted = mock.MagicMock()
        fake_ensure_mounted = self._smbfs_driver._ensure_share_mounted

        self._smbfs_driver.local_volume_dir = mock.Mock(
            return_value=self._FAKE_MNT_POINT)
        self._smbfs_driver.get_active_image_from_info = mock.Mock(
            return_value=self._FAKE_VOLUME_NAME)
        self._smbfs_driver._delete_volume = mock.MagicMock()
        self._smbfs_driver._local_path_volume_info = mock.Mock(
            return_value=fake_vol_info)

        with contextlib.nested(
                mock.patch('os.unlink', fake_unlink),
                mock.patch('os.path.exists', lambda x: True)):

            self._smbfs_driver.delete_volume(self._FAKE_VOLUME)

            fake_ensure_mounted.assert_called_once_with(self._FAKE_SHARE)
            self._smbfs_driver._delete_volume.assert_called_once_with(
                self._FAKE_VOLUME_PATH)
            fake_unlink.assert_called_once_with(fake_vol_info)

    def _test_setup(self, config, share_config_exists=True):
        fake_exists = mock.Mock(return_value=share_config_exists)
        fake_ensure_mounted = mock.MagicMock()
        self._smbfs_driver._ensure_shares_mounted = fake_ensure_mounted
        self._smbfs_driver.configuration = config

        with mock.patch('os.path.exists', fake_exists):
            if not (config.smbfs_shares_config and share_config_exists and
                    config.smbfs_oversub_ratio > 0 and
                    0 <= config.smbfs_used_ratio <= 1):
                self.assertRaises(exception.SmbfsException,
                                  self._smbfs_driver.do_setup,
                                  None)
            else:
                self._smbfs_driver.do_setup(None)
                self.assertEqual(self._smbfs_driver.shares, {})
                fake_ensure_mounted.assert_called_once()

    def test_setup_missing_shares_config_option(self):
        fake_config = copy.copy(self._FAKE_SMBFS_CONFIG)
        fake_config.smbfs_shares_config = None
        self._test_setup(fake_config, None)

    def test_setup_missing_shares_config_file(self):
        self._test_setup(self._FAKE_SMBFS_CONFIG, False)

    def test_setup_invlid_oversub_ratio(self):
        fake_config = copy.copy(self._FAKE_SMBFS_CONFIG)
        fake_config.smbfs_oversub_ratio = -1
        self._test_setup(fake_config)

    def test_setup_invalid_used_ratio(self):
        fake_config = copy.copy(self._FAKE_SMBFS_CONFIG)
        fake_config.smbfs_used_ratio = -1
        self._test_setup(fake_config)

    def _test_create_volume(self, volume_exists=False, volume_format=None):
        fake_method = mock.MagicMock()
        self._smbfs_driver.configuration = copy.copy(self._FAKE_SMBFS_CONFIG)
        self._smbfs_driver._set_rw_permissions_for_all = mock.MagicMock()
        fake_set_permissions = self._smbfs_driver._set_rw_permissions_for_all
        self._smbfs_driver.get_volume_format = mock.MagicMock()

        windows_image_format = False
        fake_vol_path = self._FAKE_VOLUME_PATH
        self._smbfs_driver.get_volume_format.return_value = volume_format

        if volume_format:
            if volume_format in ('vhd', 'vhdx'):
                windows_image_format = volume_format
                if volume_format == 'vhd':
                    windows_image_format = 'vpc'
                method = '_create_windows_image'
                fake_vol_path += '.' + volume_format
            else:
                method = '_create_%s_file' % volume_format
                if volume_format == 'sparsed':
                    self._smbfs_driver.configuration.smbfs_sparsed_volumes = (
                        True)
        else:
            method = '_create_regular_file'

        setattr(self._smbfs_driver, method, fake_method)

        with mock.patch('os.path.exists', new=lambda x: volume_exists):
            if volume_exists:
                self.assertRaises(exception.InvalidVolume,
                                  self._smbfs_driver._do_create_volume,
                                  self._FAKE_VOLUME)
                return

            self._smbfs_driver._do_create_volume(self._FAKE_VOLUME)
            if windows_image_format:
                fake_method.assert_called_once_with(
                    fake_vol_path,
                    self._FAKE_VOLUME['size'],
                    windows_image_format)
            else:
                fake_method.assert_called_once_with(
                    fake_vol_path, self._FAKE_VOLUME['size'])
            fake_set_permissions.assert_called_once_with(fake_vol_path)

    def test_create_existing_volume(self):
        self._test_create_volume(volume_exists=True)

    def test_create_vhdx(self):
        self._test_create_volume(volume_format='vhdx')

    def test_create_qcow2(self):
        self._test_create_volume(volume_format='qcow2')

    def test_create_sparsed(self):
        self._test_create_volume(volume_format='sparsed')

    def test_create_regular(self):
        self._test_create_volume()

    def _test_find_share(self, existing_mounted_shares=True,
                         eligible_shares=True):
        if existing_mounted_shares:
            mounted_shares = ('fake_share1', 'fake_share2', 'fake_share3')
        else:
            mounted_shares = None

        self._smbfs_driver._mounted_shares = mounted_shares
        self._smbfs_driver._is_share_eligible = mock.Mock(
            return_value=eligible_shares)
        fake_capacity_info = ((2, 1, 5), (2, 1, 4), (2, 1, 1))
        self._smbfs_driver._get_capacity_info = mock.Mock(
            side_effect=fake_capacity_info)

        if not mounted_shares:
            self.assertRaises(exception.SmbfsNoSharesMounted,
                              self._smbfs_driver._find_share,
                              self._FAKE_VOLUME['size'])
        elif not eligible_shares:
            self.assertRaises(exception.SmbfsNoSuitableShareFound,
                              self._smbfs_driver._find_share,
                              self._FAKE_VOLUME['size'])
        else:
            ret_value = self._smbfs_driver._find_share(
                self._FAKE_VOLUME['size'])
            # The eligible share with the minimum allocated space
            # will be selected
            self.assertEqual(ret_value, 'fake_share3')

    def test_find_share(self):
        self._test_find_share()

    def test_find_share_missing_mounted_shares(self):
        self._test_find_share(existing_mounted_shares=False)

    def test_find_share_missing_eligible_shares(self):
        self._test_find_share(eligible_shares=False)

    def _test_is_share_eligible(self, capacity_info, volume_size):
        self._smbfs_driver._get_capacity_info = mock.Mock(
            return_value=[float(x << 30) for x in capacity_info])
        self._smbfs_driver.configuration = self._FAKE_SMBFS_CONFIG
        return self._smbfs_driver._is_share_eligible(self._FAKE_SHARE,
                                                     volume_size)

    def test_share_volume_above_used_ratio(self):
        fake_capacity_info = (4, 1, 1)
        fake_volume_size = 2
        ret_value = self._test_is_share_eligible(fake_capacity_info,
                                                 fake_volume_size)
        self.assertEqual(ret_value, False)

    def test_eligible_share(self):
        fake_capacity_info = (4, 4, 0)
        fake_volume_size = 1
        ret_value = self._test_is_share_eligible(fake_capacity_info,
                                                 fake_volume_size)
        self.assertEqual(ret_value, True)

    def test_share_volume_above_oversub_ratio(self):
        fake_capacity_info = (4, 4, 7)
        fake_volume_size = 2
        ret_value = self._test_is_share_eligible(fake_capacity_info,
                                                 fake_volume_size)
        self.assertEqual(ret_value, False)

    def test_share_reserved_above_oversub_ratio(self):
        fake_capacity_info = (4, 4, 10)
        fake_volume_size = 1
        ret_value = self._test_is_share_eligible(fake_capacity_info,
                                                 fake_volume_size)
        self.assertEqual(ret_value, False)

    def test_parse_options(self):
        (opt_list,
         opt_dict) = self._smbfs_driver.parse_options(
            self._FAKE_SHARE_OPTS)
        expected_ret = ([], self._FAKE_OPTIONS_DICT)
        self.assertEqual(expected_ret, (opt_list, opt_dict))

    def test_parse_credentials(self):
        fake_smb_options = r'-o user=MyDomain\Administrator,noperm'
        expected_flags = '-o username=Administrator,noperm'
        flags = self._smbfs_driver.parse_credentials(fake_smb_options)
        self.assertEqual(expected_flags, flags)

    def test_get_volume_path(self):
        self._smbfs_driver.get_volume_format = mock.Mock(
            return_value='vhd')
        self._smbfs_driver.local_volume_dir = mock.Mock(
            return_value=self._FAKE_MNT_POINT)

        expected = self._FAKE_VOLUME_PATH + '.vhd'

        ret_val = self._smbfs_driver.local_path(self._FAKE_VOLUME)
        self.assertEqual(expected, ret_val)

    def test_initialize_connection(self):
        self._smbfs_driver.get_active_image_from_info = mock.Mock(
            return_value=self._FAKE_VOLUME_NAME)
        self._smbfs_driver._get_mount_point_base = mock.Mock(
            return_value=self._FAKE_MNT_BASE)
        self._smbfs_driver.shares = {self._FAKE_SHARE: self._FAKE_SHARE_OPTS}

        fake_data = {'export': self._FAKE_SHARE,
                     'name': self._FAKE_VOLUME_NAME,
                     'options': self._FAKE_SHARE_OPTS}
        expected = {
            'driver_volume_type': 'smbfs',
            'data': fake_data,
            'mount_point_base': self._FAKE_MNT_BASE}
        ret_val = self._smbfs_driver.initialize_connection(
            self._FAKE_VOLUME, None)

        self.assertEqual(expected, ret_val)

    def test_create_snapshot(self):
        fake_volume_info_path = self._FAKE_VOLUME_PATH + '.info'
        fake_snapshot_name = os.path.basename(self._FAKE_SNAPSHOT_PATH)
        fake_volume_info = {'active': fake_snapshot_name,
                            self._FAKE_SNAPSHOT['id']: fake_snapshot_name}

        self._smbfs_driver.local_path = mock.Mock(
            return_value=self._FAKE_VOLUME_PATH)
        self._smbfs_driver.get_active_image_from_info = mock.Mock(
            return_value=self._FAKE_VOLUME_NAME)
        self._smbfs_driver.local_volume_dir = mock.Mock(
            return_value=self._FAKE_MNT_POINT)
        self._smbfs_driver._do_create_snapshot = mock.MagicMock()
        self._smbfs_driver._read_info_file = mock.Mock(
            return_value={})
        self._smbfs_driver._write_info_file = mock.MagicMock()
        self._smbfs_driver._local_path_volume_info = mock.Mock(
            return_value=fake_volume_info_path)

        self._smbfs_driver._create_snapshot(self._FAKE_SNAPSHOT)

        self._smbfs_driver._do_create_snapshot.assert_called_once_with(
            self._FAKE_SNAPSHOT, self._FAKE_VOLUME_PATH,
            self._FAKE_SNAPSHOT_PATH)
        self._smbfs_driver._write_info_file.assert_called_once_with(
            fake_volume_info_path, fake_volume_info)

    def test_create_snapshot_volume_in_use(self):
        fake_snapshot = {'volume': {'status': 'in_use'}}
        self.assertRaises(exception.InvalidVolume,
                          self._smbfs_driver.create_snapshot,
                          fake_snapshot)

    def _test_do_create_snapshot(self, unsupported_format=False):
        fake_img_info = mock.MagicMock()
        fake_img_info.file_format = 'raw'
        self._smbfs_driver._img_info = mock.Mock(
            return_value=fake_img_info)
        self._smbfs_driver._set_rw_permissions_for_all = mock.MagicMock()
        self._smbfs_driver.get_volume_format = mock.MagicMock()

        if unsupported_format:
            self._smbfs_driver.get_volume_format.return_value = 'vhdx'
            self.assertRaises(exception.InvalidVolume,
                              self._smbfs_driver._do_create_snapshot,
                              self._FAKE_SNAPSHOT,
                              self._FAKE_VOLUME_PATH,
                              self._FAKE_SNAPSHOT_PATH)
        else:
            self._smbfs_driver._do_create_snapshot(self._FAKE_SNAPSHOT,
                                                   self._FAKE_VOLUME_PATH,
                                                   self._FAKE_SNAPSHOT_PATH)

            call_list = self._smbfs_driver._execute.call_args_list
            all_call_args = [arg for call in call_list for arg in call[0]]

            self.assertIn('create', all_call_args)
            self.assertIn('rebase', all_call_args)

    def test_do_create_snapshot(self):
        self._test_do_create_snapshot()

    def test_do_create_snapshot_vhdx(self):
        self._test_do_create_snapshot(True)

    def _test_delete_snapshot(self, is_active_image=True,
                              highest_file_exists=False):
        # If the snapshot is not the active image, it is guaranteed that
        # another snapshot exists having it as backing file.
        # If yet another file is backed by the file from the next level,
        # it means that the 'highest file' exists and it needs to be rebased.
        fake_volume_info_path = self._FAKE_VOLUME_PATH + '.info'
        fake_snapshot_name = os.path.basename(self._FAKE_SNAPSHOT_PATH)
        fake_info = {'active': fake_snapshot_name,
                     self._FAKE_SNAPSHOT['id']: fake_snapshot_name}
        fake_img_info = mock.MagicMock()
        fake_img_info.backing_file = self._FAKE_VOLUME_NAME
        fake_img_info.file_format = 'qcow2'

        self._smbfs_driver._local_path_volume_info = mock.Mock(
            return_value=fake_volume_info_path)
        self._smbfs_driver._write_info_file = mock.MagicMock()
        self._smbfs_driver._img_info = mock.Mock(
            return_value=fake_img_info)
        self._smbfs_driver.local_volume_dir = mock.Mock(
            return_value=self._FAKE_MNT_POINT)
        self._smbfs_driver._img_commit = mock.MagicMock()
        self._smbfs_driver._rebase_img = mock.MagicMock()

        expected_info = {
            'active': fake_snapshot_name,
            self._FAKE_SNAPSHOT_ID: fake_snapshot_name
        }

        if is_active_image:
            self._smbfs_driver._read_info_file = mock.Mock(
                return_value=fake_info)
            self._smbfs_driver.get_active_image_from_info = mock.Mock(
                return_value=fake_snapshot_name)

            self._smbfs_driver._delete_snapshot(self._FAKE_SNAPSHOT)

            self._smbfs_driver._img_commit.assert_called_once_with(
                self._FAKE_SNAPSHOT_PATH)
            self._smbfs_driver._write_info_file.assert_called_once_with(
                fake_volume_info_path, fake_info)
        else:
            fake_upper_snap_id = 'fake_upper_snap_id'
            fake_upper_snap_path = (
                self._FAKE_VOLUME_PATH + '-snapshot' + fake_upper_snap_id)
            fake_upper_snap_name = os.path.basename(fake_upper_snap_path)

            fake_backing_chain = [
                {'filename': fake_upper_snap_name,
                 'backing-filename': fake_snapshot_name},
                {'filename': fake_snapshot_name,
                 'backing-filename': self._FAKE_VOLUME_NAME},
                {'filename': self._FAKE_VOLUME_NAME,
                 'backing-filename': None}]

            fake_info[fake_upper_snap_id] = fake_upper_snap_name
            fake_info[self._FAKE_SNAPSHOT_ID] = fake_snapshot_name

            if highest_file_exists:
                fake_highest_snap_id = 'fake_highest_snap_id'
                fake_highest_snap_path = (
                    self._FAKE_VOLUME_PATH + '-snapshot' +
                    fake_highest_snap_id)
                fake_highest_snap_name = os.path.basename(
                    fake_highest_snap_path)

                fake_highest_snap_info = {
                    'filename': fake_highest_snap_name,
                    'backing-filename': fake_upper_snap_name,
                }
                fake_backing_chain.insert(0, fake_highest_snap_info)

                fake_info['active'] = fake_highest_snap_name
                fake_info[fake_highest_snap_id] = fake_highest_snap_name
            else:
                fake_info['active'] = fake_upper_snap_name

            expected_info = copy.deepcopy(fake_info)
            expected_info[fake_upper_snap_id] = fake_snapshot_name
            del expected_info[self._FAKE_SNAPSHOT_ID]
            if not highest_file_exists:
                expected_info['active'] = fake_snapshot_name

            self._smbfs_driver._read_info_file = mock.Mock(
                return_value=fake_info)
            self._smbfs_driver._get_backing_chain_for_path = mock.Mock(
                return_value=fake_backing_chain)

            self._smbfs_driver._delete_snapshot(self._FAKE_SNAPSHOT)

            self._smbfs_driver._img_commit.assert_any_call(
                fake_upper_snap_path)
            if highest_file_exists:
                self._smbfs_driver._rebase_img.assert_called_once_with(
                    fake_highest_snap_path, fake_snapshot_name, 'qcow2')

            self._smbfs_driver._write_info_file.assert_called_once_with(
                fake_volume_info_path, expected_info)

    def test_delete_snapshot_when_active_file(self):
        self._test_delete_snapshot()

    def test_delete_snapshot_with_one_upper_file(self):
        self._test_delete_snapshot(is_active_image=False)

    def test_delete_snapshot_with_two_or_more_upper_files(self):
        self._test_delete_snapshot(is_active_image=False,
                                   highest_file_exists=True)

    def _test_extend_volume(self, extend_failed=False):
        self._smbfs_driver.local_path = mock.Mock(
            return_value=self._FAKE_VOLUME_PATH)
        self._smbfs_driver._extend_volume = mock.MagicMock()
        self._smbfs_driver._check_extend_volume_support = mock.Mock(
            return_value=True)
        self._smbfs_driver._is_file_size_equal = mock.Mock(
            return_value=not extend_failed)

        if extend_failed:
            self.assertRaises(exception.ExtendVolumeError,
                              self._smbfs_driver.extend_volume,
                              self._FAKE_VOLUME, 2)
        else:
            self._smbfs_driver.extend_volume(self._FAKE_VOLUME, 2)
            self._smbfs_driver._extend_volume.assert_called_once_with(
                self._FAKE_VOLUME_PATH, 2)

    def test_extend_volume(self):
        self._test_extend_volume()

    def test_extend_volume_failed(self):
        self._test_extend_volume(True)

    def _test_check_extend_support(self, has_snapshots=False,
                                   is_eligible=True):
        fake_img_info = mock.MagicMock()
        self._smbfs_driver.local_path = mock.Mock(
            return_value=self._FAKE_VOLUME_PATH)

        if has_snapshots:
            fake_img_info.backing_file = self._FAKE_SNAPSHOT_PATH
        else:
            fake_img_info.backing_file = None

        self._smbfs_driver._img_info = mock.Mock(
            return_value=fake_img_info)
        self._smbfs_driver._is_share_eligible = mock.Mock(
            return_value=is_eligible)

        if has_snapshots:
            self.assertRaises(exception.InvalidVolume,
                              self._smbfs_driver._check_extend_volume_support,
                              self._FAKE_VOLUME, 2)
        elif not is_eligible:
            self.assertRaises(exception.ExtendVolumeError,
                              self._smbfs_driver._check_extend_volume_support,
                              self._FAKE_VOLUME, 2)
        else:
            self._smbfs_driver._check_extend_volume_support(
                self._FAKE_VOLUME, 2)
            self._smbfs_driver._is_share_eligible.assert_called_once_with(
                self._FAKE_SHARE, 1)

    def test_check_extend_support(self):
        self._test_check_extend_support()

    def test_check_extend_volume_with_snapshots(self):
        self._test_check_extend_support(has_snapshots=True)

    def test_check_extend_volume_uneligible_share(self):
        self._test_check_extend_support(is_eligible=False)

    def _test_copy_volume_to_image(self, has_snapshots=False):
        fake_img_info = mock.MagicMock()
        fake_img_info.file_format = 'qcow2'
        fake_image_meta = {'id': 'fake_image_id'}

        if has_snapshots:
            fake_img_info.backing_file = self._FAKE_VOLUME_NAME
            active_image = os.path.basename(self._FAKE_SNAPSHOT_PATH)
            fake_temp_path = '%s/%s.temp_image.%s' % (
                self._FAKE_MNT_POINT, self._FAKE_VOLUME['id'],
                fake_image_meta['id'])
        else:
            active_image = self._FAKE_VOLUME_NAME
            fake_img_info.backing_file = None

        self._smbfs_driver.get_active_image_from_info = mock.Mock(
            return_value=active_image)
        self._smbfs_driver.local_volume_dir = mock.Mock(
            return_value=self._FAKE_MNT_POINT)
        self._smbfs_driver._img_info = mock.Mock(
            return_value=fake_img_info)

        with contextlib.nested(
            mock.patch.object(image_utils, 'upload_volume'),
            mock.patch.object(image_utils, 'convert_image')) as (
                fake_upload_volume,
                fake_convert_image):

            self._smbfs_driver.copy_volume_to_image(
                None, self._FAKE_VOLUME, None, fake_image_meta)

            self.assertTrue(fake_upload_volume.called)
            if has_snapshots:
                self.assertTrue(fake_convert_image.called)
                self._smbfs_driver._execute.assert_called_once_with(
                    'rm', '-f', fake_temp_path)

    def test_copy_volume_to_image(self):
        self._test_copy_volume_to_image()

    def test_copy_volume_to_image_having_snapshots(self):
        self._test_copy_volume_to_image(True)

    def test_create_volume_from_in_use_snapshot(self):
        fake_snapshot = {'status': 'in-use'}
        self.assertRaises(
            exception.InvalidSnapshot,
            self._smbfs_driver.create_volume_from_snapshot,
            self._FAKE_VOLUME, fake_snapshot)

    def test_create_volume_from_snapshot(self):
        self._smbfs_driver._ensure_shares_mounted = mock.MagicMock()
        self._smbfs_driver._find_share = mock.Mock(
            return_value=self._FAKE_SHARE)
        self._smbfs_driver._do_create_volume = mock.MagicMock()
        self._smbfs_driver._copy_volume_from_snapshot = mock.MagicMock()

        ret_val = self._smbfs_driver.create_volume_from_snapshot(
            self._FAKE_VOLUME, self._FAKE_SNAPSHOT)
        expected = {'provider_location': self._FAKE_SHARE}

        self.assertEqual(expected, ret_val)
        self._smbfs_driver._do_create_volume.assert_called_once_with(
            self._FAKE_VOLUME)
        self._smbfs_driver._copy_volume_from_snapshot.assert_called_once_with(
            self._FAKE_SNAPSHOT, self._FAKE_VOLUME)

    def test_copy_volume_from_snapshot(self):
        fake_volume_info = {self._FAKE_SNAPSHOT_ID: 'fake_snapshot_file_name'}
        fake_img_info = mock.MagicMock()
        fake_img_info.backing_file = self._FAKE_VOLUME_NAME

        self._smbfs_driver.get_volume_format = mock.Mock(
            return_value='raw')
        self._smbfs_driver._local_path_volume_info = mock.Mock(
            return_value=self._FAKE_VOLUME_PATH + '.info')
        self._smbfs_driver.local_volume_dir = mock.Mock(
            return_value=self._FAKE_MNT_POINT)
        self._smbfs_driver._read_info_file = mock.Mock(
            return_value=fake_volume_info)
        self._smbfs_driver._img_info = mock.Mock(
            return_value=fake_img_info)
        self._smbfs_driver.local_path = mock.Mock(
            return_value=self._FAKE_VOLUME_PATH[:-1])
        self._smbfs_driver._set_rw_permissions_for_all = mock.MagicMock()

        with mock.patch.object(image_utils, 'convert_image') as (
                fake_convert_image):
            self._smbfs_driver._copy_volume_from_snapshot(
                self._FAKE_SNAPSHOT, self._FAKE_VOLUME)
            fake_convert_image.assert_called_once_with(
                self._FAKE_VOLUME_PATH, self._FAKE_VOLUME_PATH[:-1], 'raw')

    def _test_create_cloned_volume(self, is_available=True):
        if is_available:
            self._smbfs_driver._create_snapshot = mock.MagicMock()
            self._smbfs_driver._copy_volume_from_snapshot = mock.MagicMock()
            self._smbfs_driver._delete_snapshot = mock.MagicMock()
            expected = {'provider_location': self._FAKE_SHARE}

            ret_val = self._smbfs_driver.create_cloned_volume(
                self._FAKE_VOLUME, self._FAKE_VOLUME)
            self.assertTrue(self._smbfs_driver._create_snapshot.called)
            self.assertTrue(self._smbfs_driver._delete_snapshot.called)
            self.assertEqual(expected, ret_val)
        else:
            fake_src = {'status': 'in-use', 'id': 'fake_id'}
            self.assertRaises(exception.InvalidVolume,
                              self._smbfs_driver.create_cloned_volume,
                              self._FAKE_VOLUME,
                              fake_src)

    def test_create_clone_volume(self):
        self._test_create_cloned_volume()

    def test_create_clone_volume_in_use(self):
        self._test_create_cloned_volume(False)

    def test_ensure_mounted(self):
        self._smbfs_driver.shares = {self._FAKE_SHARE: self._FAKE_SHARE_OPTS}

        self._smbfs_driver._ensure_share_mounted(self._FAKE_SHARE)
        self._smbfs_driver._remotefsclient.mount.assert_called_once_with(
            self._FAKE_SHARE, self._FAKE_SHARE_OPTS.split())

    def _test_copy_image_to_volume(self, unsupported_qemu_version=False,
                                   wrong_size_after_fetch=False):
        fake_image_service = mock.MagicMock()
        fake_image_service.show.return_value = (
            {'id': 'fake_image_id', 'disk_format': 'raw'})

        fake_img_info = mock.MagicMock()
        if wrong_size_after_fetch:
            fake_img_info.virtual_size = 2 << 30
        else:
            fake_img_info.virtual_size = self._FAKE_VOLUME['size'] << 30

        if unsupported_qemu_version:
            qemu_version = [1, 5]
        else:
            qemu_version = [1, 7]

        self._smbfs_driver.get_volume_format = mock.Mock(
            return_value='vhdx')
        self._smbfs_driver.local_path = mock.Mock(
            return_value=self._FAKE_VOLUME_PATH)
        self._smbfs_driver.get_qemu_version = mock.Mock(
            return_value=qemu_version)
        self._smbfs_driver.configuration = mock.MagicMock()
        self._smbfs_driver.configuration.volume_dd_blocksize = 4096

        exc = None
        with contextlib.nested(
            mock.patch.object(image_utils,
                              'fetch_to_volume_format'),
            mock.patch.object(image_utils,
                              'resize_image'),
            mock.patch.object(image_utils,
                              'qemu_img_info')) as (
                fake_fetch, fake_resize,
                fake_qemu_img_info):

            if wrong_size_after_fetch:
                exc = exception.ImageUnacceptable
            elif unsupported_qemu_version:
                exc = exception.InvalidVolume

            fake_qemu_img_info.return_value = fake_img_info

            if exc:
                self.assertRaises(
                    exc, self._smbfs_driver.copy_image_to_volume, None,
                    self._FAKE_VOLUME, fake_image_service,
                    'fake_image_id')
            else:
                self._smbfs_driver.copy_image_to_volume(
                    None, self._FAKE_VOLUME, fake_image_service,
                    'fake_image_id')
                self.assertTrue(fake_fetch.called)
                self.assertTrue(fake_resize.called)

    def test_copy_image_to_volume(self):
        self._test_copy_image_to_volume()

    def test_copy_image_to_volume_wrong_size_after_fetch(self):
        self._test_copy_image_to_volume(wrong_size_after_fetch=True)

    def test_copy_image_to_volume_unsupported_qemu_version(self):
        self._test_copy_image_to_volume(unsupported_qemu_version=True)

    def test_get_capacity_info(self):
        fake_block_size = 4096.0
        fake_total_blocks = 1024
        fake_avail_blocks = 512
        fake_total_allocated = fake_total_blocks * fake_block_size

        fake_df = ('%s %s %s' % (fake_block_size, fake_total_blocks,
                                 fake_avail_blocks), None)
        fake_du = (str(fake_total_allocated), None)

        self._smbfs_driver._get_mount_point_for_share = mock.Mock(
            return_value=self._FAKE_MNT_POINT)
        self._smbfs_driver._execute = mock.Mock(
            side_effect=(fake_df, fake_du))

        ret_val = self._smbfs_driver._get_capacity_info(self._FAKE_SHARE)
        expected = (fake_block_size * fake_total_blocks,
                    fake_block_size * fake_avail_blocks,
                    fake_total_allocated)
        self.assertEqual(expected, ret_val)
