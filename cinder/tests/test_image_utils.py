
# Copyright (c) 2013 eNovance , Inc.
# All Rights Reserved.
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
"""Unit tests for image utils."""

import contextlib
import tempfile

import mock
from oslo.config import cfg

from cinder import context
from cinder import exception
from cinder.image import image_utils
from cinder.openstack.common import fileutils
from cinder.openstack.common import processutils
from cinder.openstack.common import units
from cinder import test
from cinder import utils
from cinder.volume import utils as volume_utils

CONF = cfg.CONF


class FakeImageService:
    def __init__(self):
        self._imagedata = {}

    def download(self, context, image_id, data):
        self.show(context, image_id)
        data.write(self._imagedata.get(image_id, ''))

    def show(self, context, image_id):
        return {'size': 2 * units.Gi,
                'disk_format': 'qcow2',
                'container_format': 'bare'}

    def update(self, context, image_id, metadata, path):
        pass


class TestQemuImgInfo(test.TestCase):
    @mock.patch('cinder.image.image_utils.os')
    @mock.patch.object(image_utils, '_is_vhd')
    @mock.patch('cinder.openstack.common.imageutils.QemuImgInfo')
    @mock.patch('cinder.utils.execute')
    def _test_qemu_img_info(self, mock_exec, mock_info, mock_is_vhd, mock_os,
                            is_vhd=False, os_name='posix', run_as_root=False):
        mock_out = mock.sentinel.out
        mock_err = mock.sentinel.err
        test_path = mock.sentinel.path
        mock_exec.return_value = (mock_out, mock_err)
        mock_is_vhd.return_value = is_vhd
        mock_os.name = os_name

        output = image_utils.qemu_img_info(test_path, run_as_root=run_as_root)

        expected_args = ('qemu-img', 'info', test_path)
        if os_name != 'nt':
            expected_args = ('env', 'LC_ALL=C') + expected_args
        mock_exec.assert_called_once_with(*expected_args,
                                          run_as_root=run_as_root)
        self.assertEqual(mock_info.return_value, output)

    def test_qemu_img_info_not_root(self):
        self._test_qemu_img_info()

    def test_qemu_img_info_on_nt(self):
        self._test_qemu_img_info(os_name='nt')

    def test_qemu_img_info_vhd_image(self):
        self._test_qemu_img_info(is_vhd=True)

    @mock.patch('cinder.utils.execute')
    def test_get_qemu_img_version(self, mock_exec):
        mock_out = "qemu-img version 2.0.0"
        mock_err = mock.sentinel.err
        mock_exec.return_value = (mock_out, mock_err)

        expected_version = [2, 0, 0]
        version = image_utils.get_qemu_img_version()

        mock_exec.assert_called_once_with('qemu-img', check_exit_code=False)
        self.assertEqual(expected_version, version)

    @mock.patch.object(image_utils, 'get_qemu_img_version')
    def test_validate_qemu_img_version(self, mock_get_qemu_img_version):
        fake_current_version = [1, 8]
        mock_get_qemu_img_version.return_value = fake_current_version
        minimum_version = '1.8'

        image_utils.check_qemu_img_version(minimum_version)

        mock_get_qemu_img_version.assert_called_once_with()

    @mock.patch.object(image_utils, 'get_qemu_img_version')
    def test_validate_unsupported_qemu_img_version(self,
                                                   mock_get_qemu_img_version):
        fake_current_version = [1, 8]
        mock_get_qemu_img_version.return_value = fake_current_version
        minimum_version = '2.0'

        self.assertRaises(exception.VolumeBackendAPIException,
                          image_utils.check_qemu_img_version,
                          minimum_version)

        mock_get_qemu_img_version.assert_called_once_with()


class TestConvertImage(test.TestCase):
    @mock.patch('cinder.image.image_utils.os.stat')
    @mock.patch('cinder.utils.execute')
    @mock.patch('cinder.volume.utils.setup_blkio_cgroup',
                return_value=(mock.sentinel.cgcmd, ))
    @mock.patch('cinder.utils.is_blk_device', return_value=True)
    def test_defaults_block_dev(self, mock_isblk, mock_cgroup, mock_exec,
                                mock_stat):
        source = mock.sentinel.source
        dest = mock.sentinel.dest
        out_format = mock.sentinel.out_format
        cgcmd = mock.sentinel.cgcmd
        mock_stat.return_value.st_size = 1048576

        with mock.patch('cinder.volume.utils.check_for_odirect_support',
                        return_value=True):
            output = image_utils.convert_image(source, dest, out_format)

            self.assertIsNone(output)
            mock_exec.assert_called_once_with(cgcmd, 'qemu-img', 'convert',
                                              '-t', 'none', '-O', out_format,
                                              source, dest, run_as_root=True)

        mock_exec.reset_mock()

        with mock.patch('cinder.volume.utils.check_for_odirect_support',
                        return_value=False):
            output = image_utils.convert_image(source, dest, out_format)

            self.assertIsNone(output)
            mock_exec.assert_called_once_with(cgcmd, 'qemu-img', 'convert',
                                              '-O', out_format, source, dest,
                                              run_as_root=True)

    @mock.patch('cinder.volume.utils.check_for_odirect_support',
                return_value=True)
    @mock.patch('cinder.image.image_utils.os.stat')
    @mock.patch('cinder.utils.execute')
    @mock.patch('cinder.volume.utils.setup_blkio_cgroup',
                return_value=(mock.sentinel.cgcmd, ))
    @mock.patch('cinder.utils.is_blk_device', return_value=False)
    def test_defaults_not_block_dev(self, mock_isblk, mock_cgroup, mock_exec,
                                    mock_stat, mock_odirect):
        source = mock.sentinel.source
        dest = mock.sentinel.dest
        out_format = mock.sentinel.out_format
        cgcmd = mock.sentinel.cgcmd
        mock_stat.return_value.st_size = 1048576

        output = image_utils.convert_image(source, dest, out_format)

        self.assertIsNone(output)
        mock_exec.assert_called_once_with(cgcmd, 'qemu-img', 'convert', '-O',
                                          out_format, source, dest,
                                          run_as_root=True)


class TestResizeImage(test.TestCase):
    @mock.patch('cinder.utils.execute')
    def test_defaults(self, mock_exec):
        source = mock.sentinel.source
        size = mock.sentinel.size
        output = image_utils.resize_image(source, size)
        self.assertIsNone(output)
        mock_exec.assert_called_once_with('qemu-img', 'resize', source,
                                          'sentinel.sizeG', run_as_root=False)

    @mock.patch('cinder.utils.execute')
    def test_run_as_root(self, mock_exec):
        source = mock.sentinel.source
        size = mock.sentinel.size
        output = image_utils.resize_image(source, size, run_as_root=True)
        self.assertIsNone(output)
        mock_exec.assert_called_once_with('qemu-img', 'resize', source,
                                          'sentinel.sizeG', run_as_root=True)


class TestExtractTo(test.TestCase):
    def test_extract_to_calls_tar(self):
        mox = self.mox
        mox.StubOutWithMock(utils, 'execute')

        utils.execute(
            'tar', '-xzf', 'archive.tgz', '-C', 'targetpath').AndReturn(
                ('ignored', 'ignored'))

        mox.ReplayAll()

        image_utils.extract_targz('archive.tgz', 'targetpath')
        mox.VerifyAll()


class TestSetVhdParent(test.TestCase):
    def test_vhd_util_call(self):
        mox = self.mox
        mox.StubOutWithMock(utils, 'execute')

        utils.execute(
            'vhd-util', 'modify', '-n', 'child', '-p', 'parent').AndReturn(
                ('ignored', 'ignored'))

        mox.ReplayAll()

        image_utils.set_vhd_parent('child', 'parent')
        mox.VerifyAll()


class TestFixVhdChain(test.TestCase):
    def test_empty_chain(self):
        mox = self.mox
        mox.StubOutWithMock(image_utils, 'set_vhd_parent')

        mox.ReplayAll()
        image_utils.fix_vhd_chain([])

    def test_single_vhd_file_chain(self):
        mox = self.mox
        mox.StubOutWithMock(image_utils, 'set_vhd_parent')

        mox.ReplayAll()
        image_utils.fix_vhd_chain(['0.vhd'])

    def test_chain_with_two_elements(self):
        mox = self.mox
        mox.StubOutWithMock(image_utils, 'set_vhd_parent')

        image_utils.set_vhd_parent('0.vhd', '1.vhd')

        mox.ReplayAll()
        image_utils.fix_vhd_chain(['0.vhd', '1.vhd'])


class TestGetSize(test.TestCase):
    def test_vhd_util_call(self):
        mox = self.mox
        mox.StubOutWithMock(utils, 'execute')

        utils.execute(
            'vhd-util', 'query', '-n', 'vhdfile', '-v').AndReturn(
                ('1024', 'ignored'))

        mox.ReplayAll()

        result = image_utils.get_vhd_size('vhdfile')
        mox.VerifyAll()

        self.assertEqual(1024, result)


class TestResize(test.TestCase):
    def test_vhd_util_call(self):
        mox = self.mox
        mox.StubOutWithMock(utils, 'execute')

        utils.execute(
            'vhd-util', 'resize', '-n', 'vhdfile', '-s', '1024',
            '-j', 'journal').AndReturn(('ignored', 'ignored'))

        mox.ReplayAll()

        image_utils.resize_vhd('vhdfile', 1024, 'journal')
        mox.VerifyAll()


class TestCoalesce(test.TestCase):
    def test_vhd_util_call(self):
        mox = self.mox
        mox.StubOutWithMock(utils, 'execute')

        utils.execute(
            'vhd-util', 'coalesce', '-n', 'vhdfile'
        ).AndReturn(('ignored', 'ignored'))

        mox.ReplayAll()

        image_utils.coalesce_vhd('vhdfile')
        mox.VerifyAll()


@contextlib.contextmanager
def fake_context(return_value):
    yield return_value


class TestTemporaryFile(test.TestCase):
    def test_file_unlinked(self):
        mox = self.mox
        mox.StubOutWithMock(image_utils, 'create_temporary_file')
        mox.StubOutWithMock(fileutils, 'delete_if_exists')

        image_utils.create_temporary_file().AndReturn('somefile')
        fileutils.delete_if_exists('somefile')

        mox.ReplayAll()

        with image_utils.temporary_file():
            pass

    def test_file_unlinked_on_error(self):
        mox = self.mox
        mox.StubOutWithMock(image_utils, 'create_temporary_file')
        mox.StubOutWithMock(fileutils, 'delete_if_exists')

        image_utils.create_temporary_file().AndReturn('somefile')
        fileutils.delete_if_exists('somefile')

        mox.ReplayAll()

        def sut():
            with image_utils.temporary_file():
                raise test.TestingException()

        self.assertRaises(test.TestingException, sut)


class TestCoalesceChain(test.TestCase):
    def test_single_vhd(self):
        mox = self.mox
        mox.StubOutWithMock(image_utils, 'get_vhd_size')
        mox.StubOutWithMock(image_utils, 'resize_vhd')
        mox.StubOutWithMock(image_utils, 'coalesce_vhd')

        mox.ReplayAll()

        result = image_utils.coalesce_chain(['0.vhd'])
        mox.VerifyAll()

        self.assertEqual('0.vhd', result)

    def test_chain_of_two_vhds(self):
        self.mox.StubOutWithMock(image_utils, 'get_vhd_size')
        self.mox.StubOutWithMock(image_utils, 'temporary_dir')
        self.mox.StubOutWithMock(image_utils, 'resize_vhd')
        self.mox.StubOutWithMock(image_utils, 'coalesce_vhd')
        self.mox.StubOutWithMock(image_utils, 'temporary_file')

        image_utils.get_vhd_size('0.vhd').AndReturn(1024)
        image_utils.temporary_dir().AndReturn(fake_context('tdir'))
        image_utils.resize_vhd('1.vhd', 1024, 'tdir/vhd-util-resize-journal')
        image_utils.coalesce_vhd('0.vhd')
        self.mox.ReplayAll()

        result = image_utils.coalesce_chain(['0.vhd', '1.vhd'])
        self.mox.VerifyAll()
        self.assertEqual('1.vhd', result)


class TestDiscoverChain(test.TestCase):
    def test_discovery_calls(self):
        mox = self.mox
        mox.StubOutWithMock(image_utils, 'file_exist')

        image_utils.file_exist('some/path/0.vhd').AndReturn(True)
        image_utils.file_exist('some/path/1.vhd').AndReturn(True)
        image_utils.file_exist('some/path/2.vhd').AndReturn(False)

        mox.ReplayAll()
        result = image_utils.discover_vhd_chain('some/path')
        mox.VerifyAll()

        self.assertEqual(
            ['some/path/0.vhd', 'some/path/1.vhd'], result)


class TestXenServerImageToCoalescedVhd(test.TestCase):
    def test_calls(self):
        mox = self.mox
        mox.StubOutWithMock(image_utils, 'temporary_dir')
        mox.StubOutWithMock(image_utils, 'extract_targz')
        mox.StubOutWithMock(image_utils, 'discover_vhd_chain')
        mox.StubOutWithMock(image_utils, 'fix_vhd_chain')
        mox.StubOutWithMock(image_utils, 'coalesce_chain')
        mox.StubOutWithMock(image_utils.os, 'unlink')
        mox.StubOutWithMock(fileutils, 'delete_if_exists')
        mox.StubOutWithMock(image_utils, 'rename_file')

        image_utils.temporary_dir().AndReturn(fake_context('somedir'))
        image_utils.extract_targz('image', 'somedir')
        image_utils.discover_vhd_chain('somedir').AndReturn(
            ['somedir/0.vhd', 'somedir/1.vhd'])
        image_utils.fix_vhd_chain(['somedir/0.vhd', 'somedir/1.vhd'])
        image_utils.coalesce_chain(
            ['somedir/0.vhd', 'somedir/1.vhd']).AndReturn('somedir/1.vhd')
        fileutils.delete_if_exists('image')
        image_utils.rename_file('somedir/1.vhd', 'image')

        mox.ReplayAll()
        image_utils.replace_xenserver_image_with_coalesced_vhd('image')
        mox.VerifyAll()
