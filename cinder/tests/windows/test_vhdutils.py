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

import importlib
import mock

from cinder import exception
from cinder import test
from cinder.volume.drivers.windows import constants


class VHDUtilsTestCase(test.TestCase):

    _FAKE_FORMAT = 2
    _FAKE_TYPE = constants.VHD_TYPE_DYNAMIC
    _FAKE_JOB_PATH = 'fake_job_path'
    _FAKE_VHD_PATH = r'C:\fake\vhd.vhd'
    _FAKE_DEST_PATH = r'C:\fake\destination.vhdx'
    _FAKE_RET_VAL = 0
    _FAKE_VHD_SIZE = 1024

    def setUp(self):
        super(VHDUtilsTestCase, self).setUp()

        self._fake_ctypes = mock.MagicMock()
        mock.patch.dict('sys.modules', ctypes=self._fake_ctypes).start()
        mock.patch('os.name', 'nt').start()
        self.addCleanup(mock.patch.stopall)

        self._vhdutils_module = importlib.import_module(
            'cinder.volume.drivers.windows.vhdutils')
        self._mock_win32_structures()
        self._vhdutils = self._vhdutils_module.VHDUtils()

    def _mock_win32_structures(self):
        self._vhdutils_module.Win32_GUID = mock.Mock()
        self._vhdutils_module.Win32_RESIZE_VIRTUAL_DISK_PARAMETERS = (
            mock.Mock())
        self._vhdutils_module.Win32_CREATE_VIRTUAL_DISK_PARAMETERS = (
            mock.Mock())

    def _test_convert_vhd(self, convertion_failed=False):
        # self._vhdutils_module.ctypes.windll.virtdisk = mock.Mock()
        vhdutils = self._vhdutils_module
        fake_virtdisk = vhdutils.ctypes.windll.virtdisk

        self._vhdutils._get_device_id_by_path = mock.Mock(
            side_effect=(vhdutils.VIRTUAL_STORAGE_TYPE_DEVICE_VHD,
                         vhdutils.VIRTUAL_STORAGE_TYPE_DEVICE_VHDX))
        self._vhdutils._close = mock.Mock()

        fake_params = mock.Mock()
        fake_vst = mock.Mock()
        fake_source_vst = mock.Mock()

        vhdutils.Win32_CREATE_VIRTUAL_DISK_PARAMETERS.return_value = (
            fake_params)
        vhdutils.Win32_VIRTUAL_STORAGE_TYPE.side_effect = [
            fake_vst, None, fake_source_vst]
        fake_virtdisk.CreateVirtualDisk.return_value = int(convertion_failed)

	# Use this in order to make assertions on the variables parsed by
        # references.
        vhdutils.ctypes.byref = lambda x: x
        vhdutils.ctypes.c_wchar_p = lambda x: x

        if convertion_failed:
            self.assertRaises(exception.VolumeBackendAPIException,
                              self._vhdutils.convert_vhd,
                              self._FAKE_VHD_PATH, self._FAKE_DEST_PATH,
                              self._FAKE_TYPE)
        else:
            self._vhdutils.convert_vhd(self._FAKE_VHD_PATH,
                                       self._FAKE_DEST_PATH,
                                       self._FAKE_TYPE)
            self.assertTrue(self._vhdutils._close.called)

        self.assertEquals(vhdutils.VIRTUAL_STORAGE_TYPE_DEVICE_VHDX,
                          fake_vst.DeviceId)
        self.assertEquals(vhdutils.VIRTUAL_STORAGE_TYPE_DEVICE_VHD,
                          fake_source_vst.DeviceId)
        print fake_virtdisk.CreateVirtualDisk.call_args_list

        fake_virtdisk.CreateVirtualDisk.assert_called_with(
            vhdutils.ctypes.byref(fake_vst),
            vhdutils.ctypes.c_wchar_p(self._FAKE_DEST_PATH),
            vhdutils.VIRTUAL_DISK_ACCESS_NONE, None,
            vhdutils.CREATE_VIRTUAL_DISK_FLAG_NONE, 0,
            vhdutils.ctypes.byref(fake_params), None,
            vhdutils.ctypes.byref(vhdutils.ctypes.wintypes.HANDLE()))

    def test_convert_vhd_successfully(self):
        self._test_convert_vhd()

    def test_convert_vhd_exception(self):
        self._test_convert_vhd(True)
