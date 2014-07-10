# Copyright (c) 2014 Cloudbase Solutions SRL
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

import json
import os
import re

from oslo.config import cfg

from cinder import exception
from cinder import utils

from cinder.brick.remotefs import remotefs
from cinder.image import image_utils
from cinder.openstack.common import fileutils
from cinder.openstack.common.gettextutils import _
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils as putils
from cinder.openstack.common import units
from cinder.volume.drivers import nfs


VERSION = '1.0.0'

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.StrOpt('smbfs_shares_config',
               default='/etc/cinder/smbfs_shares',
               help='File with the list of available smbfs shares'),
    cfg.StrOpt('smbfs_default_volume_format',
               default='raw',
               help=('Default format that will be used when creating volumes '
                     'if no volume format is specified. Can be set to: '
                     'raw, qcow2, vhd or vhdx.')),
    cfg.BoolOpt('smbfs_sparsed_volumes',
                default=True,
                help=('Create volumes as sparsed files which take no space '
                      'rather than regular files when using raw format, '
                      'in which case volume creation takes lot of time.')),
    cfg.FloatOpt('smbfs_used_ratio',
                 default=0.95,
                 help=('Percent of ACTUAL usage of the underlying volume '
                       'before no new volumes can be allocated to the volume '
                       'destination.')),
    cfg.FloatOpt('smbfs_oversub_ratio',
                 default=1.0,
                 help=('This will compare the allocated to available space on '
                       'the volume destination.  If the ratio exceeds this '
                       'number, the destination will no longer be valid.')),
    cfg.StrOpt('smbfs_mount_point_base',
               default='$state_path/mnt',
               help=('Base dir containing mount points for smbfs shares.')),
    cfg.StrOpt('smbfs_mount_options',
               default='noperm,file_mode=0775,dir_mode=0775',
               help=('Mount options passed to the smbfs client. See section '
                     'of the mount.cifs man page for details.')),
]

CONF = cfg.CONF
CONF.register_opts(volume_opts)


class SmbfsDriver(nfs.RemoteFsDriver):
    """SMBFS based cinder volume driver.
    """

    driver_volume_type = 'smbfs'
    driver_prefix = 'smbfs'
    volume_backend_name = 'Generic_SMBFS'
    VERSION = VERSION

    _DISK_FORMAT_VHD = 'vhd'
    _DISK_FORMAT_VHDX = 'vhdx'
    _DISK_FORMAT_RAW = 'raw'
    _DISK_FORMAT_QCOW2 = 'qcow2'

    def __init__(self, execute=putils.execute, *args, **kwargs):
        self._remotefsclient = None
        super(SmbfsDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(volume_opts)
        root_helper = utils.get_root_helper()
        self.base = getattr(self.configuration,
                            'smbfs_mount_point_base',
                            CONF.smbfs_mount_point_base)
        opts = getattr(self.configuration,
                       'smbfs_mount_options',
                       CONF.smbfs_mount_options)
        self._remotefsclient = remotefs.RemoteFsClient(
            'cifs', root_helper, execute=execute,
            smbfs_mount_point_base=self.base,
            smbfs_mount_options=opts)
        self.img_suffix = None

    def set_execute(self, execute):
        super(SmbfsDriver, self).set_execute(execute)
        if self._remotefsclient:
            self._remotefsclient.set_execute(execute)

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info.

        :param volume: volume reference
        :param connector: connector reference
        """
        # Find active image
        active_file = self.get_active_image_from_info(volume)

        data = {'export': volume['provider_location'],
                'name': active_file}
        if volume['provider_location'] in self.shares:
            data['options'] = self.shares[volume['provider_location']]
        return {
            'driver_volume_type': self.driver_volume_type,
            'data': data,
            'mount_point_base': self._get_mount_point_base()
        }

    def do_setup(self, context):
        """Any initialization the volume driver does while starting"""

        config = self.configuration.smbfs_shares_config
        if not config:
            msg = (_("There's no SMBFS config file configured "
                     "(smbfs_shares_config)."))
            LOG.warn(msg)
            raise exception.SmbfsException(msg)
        if not os.path.exists(config):
            msg = (_("SMBFS config file at %(config)s doesn't exist") %
                   {'config': config})
            LOG.warn(msg)
            raise exception.SmbfsException(msg)
        if not self.configuration.smbfs_oversub_ratio > 0:
            msg = _(
                "SMBFS config 'smbfs_oversub_ratio' invalid.  Must be > 0: "
                "%s") % self.configuration.smbfs_oversub_ratio

            LOG.error(msg)
            raise exception.SmbfsException(msg)

        if ((not self.configuration.smbfs_used_ratio > 0) and
                (self.configuration.smbfs_used_ratio <= 1)):
            msg = _("SMBFS config 'smbfs_used_ratio' invalid.  Must be > 0 "
                    "and <= 1.0: %s") % self.configuration.smbfs_used_ratio
            LOG.error(msg)
            raise exception.SmbfsException(msg)

        self.shares = {}  # address : options
        self._ensure_shares_mounted()

    def local_volume_dir(self, volume):
        smbfs_share = volume['provider_location']
        local_dir = self._get_mount_point_for_share(smbfs_share)
        return local_dir

    def local_path(self, volume):
        """Get volume path (mounted locally fs path) for given volume.
        :param volume: volume reference
        """
        fmt = self.get_volume_format(volume)
        local_dir = self.local_volume_dir(volume)
        local_path = os.path.join(local_dir, volume['name'])
        if fmt in (self._DISK_FORMAT_VHD, self._DISK_FORMAT_VHDX):
            local_path += '.' + fmt
        return local_path

    def get_volume_format(self, volume, qemu_format=False):
        volume_format = (
            self._get_volume_format_spec(volume) or
            self.configuration.smbfs_default_volume_format)
        if qemu_format and volume_format == self._DISK_FORMAT_VHD:
            volume_format = 'vpc'
        return volume_format

    def _get_mount_point_base(self):
        return self.base

    def _get_mount_point_for_share(self, smbfs_share):
        """Needed by parent class."""
        return self._remotefsclient.get_mount_point(smbfs_share)

    def delete_volume(self, volume):
        """Deletes a logical volume."""
        if not volume['provider_location']:
            LOG.warn(_('Volume %s does not have provider_location specified, '
                       'skipping'), volume['name'])
            return

        self._ensure_share_mounted(volume['provider_location'])
        volume_dir = self.local_volume_dir(volume)
        mounted_path = os.path.join(volume_dir,
                                    self.get_active_image_from_info(volume))
        if os.path.exists(mounted_path):
            self._delete_volume(mounted_path)
        else:
            LOG.debug("Skipping deleting volume %s as it does not exist." %
                      mounted_path)

        info_path = self._local_path_volume_info(volume)
        fileutils.delete_if_exists(info_path)

    def _delete_volume(self, volume_path):
        self._execute('rm', '-f', volume_path, run_as_root=True)

    def get_qemu_version(self):
        info, _ = self._execute('qemu-img', check_exit_code=False)
        pattern = r"qemu-img version ([0-9\.]*)"
        version = re.match(pattern, info)
        if not version:
            LOG.warn(_("qemu-img is not installed"))
            return None
        return [int(x) for x in version.groups()[0].split('.')]

    def _create_windows_image(self, volume_path, volume_size, volume_format):
        """Creates a VHD or VHDX file of a given size."""
        # vhd is regarded as vpc by qemu
        if volume_format == self._DISK_FORMAT_VHD:
            volume_format = 'vpc'
        else:
            qemu_version = self.get_qemu_version()
            if qemu_version < [1, 7]:
                err_msg = ("qemu-img %s does not support vhdx images. "
                           "Please upgrade to 1.7 or greater.")
                raise exception.SmbfsException(err_msg)

        self._execute('qemu-img', 'create', '-f', volume_format,
                      volume_path, str(volume_size * units.Gi),
                      run_as_root=True)

    def _do_create_volume(self, volume):
        """Create a volume on given smbfs_share.

        :param volume: volume reference
        """
        volume_format = self.get_volume_format(volume)
        volume_path = self.local_path(volume)
        volume_size = volume['size']

        LOG.debug("Creating new volume at %s" % volume_path)

        if os.path.exists(volume_path):
            msg = _('File already exists at %s') % volume_path
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)

        if volume_format in (self._DISK_FORMAT_VHD, self._DISK_FORMAT_VHDX):
            self._create_windows_image(volume_path, volume_size,
                                       volume_format)
        else:
            self.img_suffix = None
            if volume_format == self._DISK_FORMAT_QCOW2:
                self._create_qcow2_file(volume_path, volume_size)
            elif self.configuration.smbfs_sparsed_volumes:
                self._create_sparsed_file(volume_path, volume_size)
            else:
                self._create_regular_file(volume_path, volume_size)

        self._set_rw_permissions_for_all(volume_path)

    def _get_capacity_info(self, smbfs_share):
        """Calculate available space on the SMBFS share.

        :param smbfs_share: example //172.18.194.100/share
        """

        mount_point = self._get_mount_point_for_share(smbfs_share)

        df, _ = self._execute('stat', '-f', '-c', '%S %b %a', mount_point,
                              run_as_root=True)
        block_size, blocks_total, blocks_avail = map(float, df.split())
        total_available = block_size * blocks_avail
        total_size = block_size * blocks_total

        du, _ = self._execute('du', '-sb', '--apparent-size', '--exclude',
                              '*snapshot*', mount_point, run_as_root=True)
        total_allocated = float(du.split()[0])
        return total_size, total_available, total_allocated

    def _find_share(self, volume_size_in_gib):
        """Choose SMBFS share among available ones for given volume size.

        For instances with more than one share that meets the criteria, the
        share with the least "allocated" space will be selected.

        :param volume_size_in_gib: int size in GB
        """

        if not self._mounted_shares:
            raise exception.SmbfsNoSharesMounted()

        target_share = None
        target_share_reserved = 0

        for smbfs_share in self._mounted_shares:
            if not self._is_share_eligible(smbfs_share, volume_size_in_gib):
                continue
            total_allocated = self._get_capacity_info(smbfs_share)[2]
            if target_share is not None:
                if target_share_reserved > total_allocated:
                    target_share = smbfs_share
                    target_share_reserved = total_allocated
            else:
                target_share = smbfs_share
                target_share_reserved = total_allocated

        if target_share is None:
            raise exception.SmbfsNoSuitableShareFound(
                volume_size=volume_size_in_gib)

        LOG.debug('Selected %s as target smbfs share.' % target_share)

        return target_share

    def _is_share_eligible(self, smbfs_share, volume_size_in_gib):
        """Verifies SMBFS share is eligible to host volume with given size.

        First validation step: ratio of actual space (used_space / total_space)
        is less than 'smbfs_used_ratio'. Second validation step: apparent space
        allocated (differs from actual space used when using sparse files)
        and compares the apparent available
        space (total_available * smbfs_oversub_ratio) to ensure enough space is
        available for the new volume.

        :param smbfs_share: smbfs share
        :param volume_size_in_gib: int size in GB
        """

        used_ratio = self.configuration.smbfs_used_ratio
        oversub_ratio = self.configuration.smbfs_oversub_ratio
        requested_volume_size = volume_size_in_gib * units.Gi

        total_size, total_available, total_allocated = \
            self._get_capacity_info(smbfs_share)

        apparent_size = max(0, total_size * oversub_ratio)
        apparent_available = max(0, apparent_size - total_allocated)
        used = (total_size - total_available) / total_size

        if used > used_ratio:
            LOG.debug('%s is above smbfs_used_ratio' % smbfs_share)
            return False
        if apparent_available <= requested_volume_size:
            LOG.debug('%s is above smbfs_oversub_ratio' % smbfs_share)
            return False
        if total_allocated / total_size >= oversub_ratio:
            LOG.debug('%s reserved space is above smbfs_oversub_ratio' %
                      smbfs_share)
            return False
        return True

    def _load_shares_config(self, share_file):
        self.shares = {}

        for share in self._read_config_file(share_file):
            # A configuration line may be either:
            #  //host/vol_name
            # or
            #  //host/vol_name -o username=Administrator,password=12345
            if not share.strip():
                # Skip blank or whitespace-only lines
                continue
            if share.startswith('#'):
                continue

            share_info = share.split(' ', 1)
            # results in share_info =
            #  [ '//address/vol', '-o username=Administrator,password=12345' ]

            share_address = share_info[0].strip().decode('unicode_escape')
            share_opts = share_info[1].strip() if len(share_info) > 1 else None

            if not re.match(r'//.+/.+', share_address):
                LOG.warn("Share %s ignored due to invalid format.  Must be of "
                         "form //address/export." % share_address)
                continue

            self.shares[share_address] = share_opts

    def get_active_image_from_info(self, volume):
        """Returns filename of the active image from the info file."""

        info_file = self._local_path_volume_info(volume)

        snap_info = self._read_info_file(info_file, empty_if_missing=True)

        if snap_info == {}:
            # No info file = no snapshots exist
            vol_path = os.path.basename(self.local_path(volume))
            return vol_path

        return snap_info['active']

    def _local_path_volume_info(self, volume):
        return '%s%s' % (self.local_path(volume), '.info')

    def _read_file(self, filename):
        """This method is to make it easier to stub out code for testing.

        Returns a string representing the contents of the file.
        """

        with open(filename, 'r') as f:
            return f.read()

    def _read_info_file(self, info_path, empty_if_missing=False):
        """Return dict of snapshot information."""

        if not os.path.exists(info_path):
            if empty_if_missing is True:
                return {}

        return json.loads(self._read_file(info_path))

    def _write_info_file(self, info_path, snap_info):
        if 'active' not in snap_info.keys():
            msg = _("'active' must be present when writing snap_info.")
            raise exception.SmbfsException(msg)

        with open(info_path, 'w') as f:
            json.dump(snap_info, f, indent=1, sort_keys=True)

    @utils.synchronized('smbfs', external=False)
    def create_snapshot(self, snapshot):
        """Apply locking to the create snapshot operation."""

        return self._create_snapshot(snapshot)

    def _create_snapshot(self, snapshot):
        status = snapshot['volume']['status']
        if status != 'available':
            msg = _('Volume status must be "available" for snapshot. '
                    '(is %s)') % status
            raise exception.InvalidVolume(msg)
        LOG.debug('Creating snapshot: %s' % snapshot)

        volume_path = self.local_path(snapshot['volume'])
        new_snap_path, ext = os.path.splitext(volume_path)
        new_snap_path += '-snapshot' + snapshot['id'] + ext
        backing_filename = self.get_active_image_from_info(snapshot['volume'])
        backing_file = os.path.join(self.local_volume_dir(snapshot['volume']),
                                    backing_filename)
        self._do_create_snapshot(snapshot, backing_file, new_snap_path)

        # Update info file
        info_path = self._local_path_volume_info(snapshot['volume'])
        snap_info = self._read_info_file(info_path,
                                         empty_if_missing=True)

        snap_info['active'] = os.path.basename(new_snap_path)
        snap_info[snapshot['id']] = os.path.basename(new_snap_path)
        self._write_info_file(info_path, snap_info)

    def _do_create_snapshot(self, snapshot, backing_file, new_snap_path):
        """Create a QCOW2 file backed by another file.

        :param snapshot: snapshot reference
        :param backing_filename: filename of file that will back the
        new qcow2 file
        :param new_snap_path: filename of new qcow2 file
        """
        volume_format = self.get_volume_format(snapshot['volume'])
        # qemu-img does not yet support differencing vhd/vhdx
        if volume_format in (self._DISK_FORMAT_VHD, self._DISK_FORMAT_VHDX):
            err_msg = _("Snapshots are not supported for this volume "
                        "format: %s ") % volume_format
            raise exception.InvalidVolume(err_msg)

        command = ['qemu-img', 'create', '-f', 'qcow2', '-o',
                   'backing_file=%s' % backing_file, new_snap_path]
        self._execute(*command, run_as_root=True)

        info = self._img_info(backing_file)
        backing_fmt = info.file_format

        backing_filename = os.path.basename(backing_file)
        command = ['qemu-img', 'rebase', '-u',
                   '-b', backing_filename,
                   '-F', backing_fmt,
                   new_snap_path]
        self._execute(*command, run_as_root=True)

        self._set_rw_permissions_for_all(new_snap_path)

    def _img_info(self, path):
        """Sanitize image_utils' qemu_img_info.

        This code expects to deal only with relative filenames.
        """

        info = image_utils.qemu_img_info(path)
        if info.image:
            info.image = os.path.basename(info.image)
        if info.backing_file:
            info.backing_file = os.path.basename(info.backing_file)

        return info

    @utils.synchronized('smbfs', external=False)
    def delete_snapshot(self, snapshot):
        """Apply locking to the delete snapshot operation."""

        return self._delete_snapshot(snapshot)

    def _delete_snapshot(self, snapshot):
        LOG.debug('Deleting snapshot %s' % snapshot['id'])
        volume_status = snapshot['volume']['status']
        if volume_status != 'available':
            msg = _('Volume status must be "available".')
            raise exception.InvalidVolume(msg)

        info_path = self._local_path_volume_info(snapshot['volume'])
        snap_info = self._read_info_file(info_path, empty_if_missing=True)

        if snapshot['id'] not in snap_info:
            # If snapshot info file is present, but snapshot record does not
            # exist, do not attempt to delete.
            LOG.info(_('Snapshot record for %s is not present, allowing '
                       'snapshot_delete to proceed.') % snapshot['id'])
            return

        snapshot_file = snap_info[snapshot['id']]
        snapshot_path = os.path.join(self.local_volume_dir(snapshot['volume']),
                                     snapshot_file)
        snapshot_path_img_info = self._img_info(snapshot_path)
        vol_path = self.local_volume_dir(snapshot['volume'])

        # Find what file has this as its backing file
        active_file = self.get_active_image_from_info(snapshot['volume'])
        active_file_path = os.path.join(vol_path, active_file)

        if snapshot_file == active_file:
            # Need to merge snapshot_file into its backing file
            # There is no top file
            #      T0       |       T1        |
            #     base      |  snapshot_file  | None
            # (guaranteed to| (being deleted) |
            #     exist)    |                 |

            base_file = snapshot_path_img_info.backing_file

            self._img_commit(snapshot_path)

            # Remove snapshot_file from info
            del snap_info[snapshot['id']]

            # Active file has changed
            snap_info['active'] = base_file
            self._write_info_file(info_path, snap_info)
        else:
            #    T0         |      T1        |     T2         |       T3
            #    base       |  snapshot_file |  higher_file   |  highest_file
            #(guaranteed to | (being deleted)|(guaranteed to  |  (may exist,
            #  exist, not   |                | exist, being   |needs ptr update
            #  used here)   |                | committed down)|     if so)

            backing_chain = self._get_backing_chain_for_path(
                snapshot['volume'], active_file_path)

            # This file is guaranteed to exist since we aren't operating on
            # the active file.
            higher_file = next((os.path.basename(f['filename'])
                                for f in backing_chain
                                if f.get('backing-filename', '') ==
                                snapshot_file),
                               None)
            if higher_file is None:
                msg = (_('No file found with %s as backing file.')
                       % snapshot_file)
                raise exception.SmbfsException(msg)

            higher_id = next((i for i in snap_info
                              if snap_info[i] == higher_file
                              and i != 'active'),
                             None)
            if higher_id is None:
                msg = _('No snap found with %s as backing file.') %\
                    higher_file
                raise exception.SmbfsException(msg)

            # Is there a file depending on higher_file?
            highest_file = next((os.path.basename(f['filename'])
                                for f in backing_chain
                                if f.get('backing-filename', '') ==
                                higher_file),
                                None)
            if highest_file is None:
                msg = 'No file depends on %s.' % higher_file
                LOG.debug(msg)

            # Committing higher_file into snapshot_file
            # And update pointer in highest_file
            higher_file_path = os.path.join(vol_path, higher_file)
            self._img_commit(higher_file_path)
            if highest_file is not None:
                highest_file_path = os.path.join(vol_path, highest_file)
                info = self._img_info(snapshot_path)
                snapshot_file_fmt = info.file_format

                self._rebase_img(highest_file_path, snapshot_file,
                                 snapshot_file_fmt)

            # Remove snapshot_file from info
            del snap_info[snapshot['id']]
            snap_info[higher_id] = snapshot_file
            if higher_file == active_file:
                if highest_file is not None:
                    msg = _('Check condition failed: '
                            '%s expected to be None.') % 'highest_file'
                    raise exception.SmbfsException(msg)
                # Active file has changed
                snap_info['active'] = snapshot_file
            self._write_info_file(info_path, snap_info)

    def _get_backing_chain_for_path(self, volume, path):
        """Returns list of dicts containing backing-chain information.

        Includes 'filename', and 'backing-filename' for each
        applicable entry.

        :param volume: volume reference
        :param path: path to image file at top of chain

        """

        output = []

        info = self._img_info(path)
        new_info = {}
        new_info['filename'] = os.path.basename(path)
        new_info['backing-filename'] = info.backing_file

        output.append(new_info)

        while new_info['backing-filename']:
            filename = new_info['backing-filename']
            path = os.path.join(self.local_volume_dir(volume), filename)
            info = self._img_info(path)
            backing_filename = info.backing_file
            new_info = {}
            new_info['filename'] = filename
            new_info['backing-filename'] = backing_filename

            output.append(new_info)

        return output

    def _img_commit(self, path):
        self._execute('qemu-img', 'commit', path, run_as_root=True)
        self._execute('rm', '-f', path, run_as_root=True)

    def _rebase_img(self, image, backing_file, volume_format):
        self._execute('qemu-img', 'rebase', '-u', '-b', backing_file, image,
                      '-F', volume_format, run_as_root=True)

    @utils.synchronized('smbfs', external=False)
    def extend_volume(self, volume, size_gb):
        LOG.info(_('Extending volume %s.'), volume['id'])
        volume_path = self.local_path(volume)
        volume_filename = os.path.basename(volume_path)

        self._check_extend_volume_support(volume, size_gb)
        LOG.info(_('Resizing file to %sG...') % size_gb)

        self._extend_volume(volume_path, size_gb)
        if not self._is_file_size_equal(volume_path, size_gb):
            raise exception.ExtendVolumeError(
                reason='Resizing image file failed.')

    def _extend_volume(self, volume_path, size_gb):
        image_utils.resize_image(volume_path, size_gb)

    def _check_extend_volume_support(self, volume, size_gb):
        volume_path = self.local_path(volume)
        info = self._img_info(volume_path)
        backing_file = info.backing_file

        if backing_file:
            msg = _('Extend volume is only supported for this'
                    ' driver when no snapshots exist.')
            raise exception.InvalidVolume(msg)

        extend_by = int(size_gb) - volume['size']
        if not self._is_share_eligible(volume['provider_location'],
                                       extend_by):
            raise exception.ExtendVolumeError(reason='Insufficient space to'
                                              ' extend volume %s to %sG'
                                              % (volume['id'], size_gb))

    @utils.synchronized('smbfs', external=False)
    def copy_volume_to_image(self, context, volume, image_service,
                             image_meta):
        """Copy the volume to the specified image."""

        # If snapshots exist, flatten to a temporary image, and upload it

        active_file = self.get_active_image_from_info(volume)
        active_file_path = '%s/%s' % (self.local_volume_dir(volume),
                                      active_file)
        info = self._img_info(active_file_path)
        backing_file = info.backing_file
        root_file_fmt = info.file_format

        temp_path = None

        try:
            if backing_file:
                # Snapshots exist
                temp_path = '%s/%s.temp_image.%s' % (
                    self.local_volume_dir(volume),
                    volume['id'],
                    image_meta['id'])

                image_utils.convert_image(active_file_path, temp_path,
                                          root_file_fmt)
                upload_path = temp_path
            else:
                upload_path = active_file_path

            image_utils.upload_volume(context,
                                      image_service,
                                      image_meta,
                                      upload_path,
                                      root_file_fmt)
        finally:
            if temp_path:
                self._execute('rm', '-f', temp_path)

    @utils.synchronized('smbfs', external=False)
    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot.

        Snapshot must not be the active snapshot. (offline)
        """

        if snapshot['status'] != 'available':
            msg = _('Snapshot status must be "available" to clone.')
            raise exception.InvalidSnapshot(msg)

        self._ensure_shares_mounted()

        volume['provider_location'] = self._find_share(volume['size'])

        self._do_create_volume(volume)

        self._copy_volume_from_snapshot(snapshot,
                                        volume)

        return {'provider_location': volume['provider_location']}

    def _copy_volume_from_snapshot(self, snapshot, volume):
        """Copy data from snapshot to destination volume.

        This is done with a qemu-img convert to raw/qcow2 from the snapshot
        qcow2.
        """

        LOG.debug("Snapshot: %(snap)s, volume: %(vol)s, "
                  "volume_size: %(size)s" %
                  {'snap': snapshot['id'],
                   'vol': volume['id'],
                   'size': snapshot['volume_size']})

        info_path = self._local_path_volume_info(snapshot['volume'])
        snap_info = self._read_info_file(info_path)
        vol_dir = self.local_volume_dir(snapshot['volume'])
        out_format = self.get_volume_format(volume, qemu_format=True)

        forward_file = snap_info[snapshot['id']]
        forward_path = os.path.join(vol_dir, forward_file)

        # Find the file which backs this file, which represents the point
        # when this snapshot was created.
        img_info = self._img_info(forward_path)
        path_to_snap_img = os.path.join(vol_dir, img_info.backing_file)

        LOG.debug("Will copy from snapshot at %s" % path_to_snap_img)

        image_utils.convert_image(path_to_snap_img,
                                  self.local_path(volume),
                                  out_format)

        self._set_rw_permissions_for_all(self.local_path(volume))

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """Fetch the image from image_service and write it to the volume."""
        volume_format = self.get_volume_format(volume, qemu_format=True)
        image_meta = image_service.show(context, image_id)
        qemu_version = self.get_qemu_version()

        if (qemu_version < [1, 7] and (
                volume_format == self._DISK_FORMAT_VHDX and
                image_meta['disk_format'] != volume_format)):
            err_msg = _("Unsupported volume format: vhdx. qemu-img 1.7 or "
                        "higher is required in order to properly support this "
                        "format.")
            raise exception.InvalidVolume(err_msg)

        image_utils.fetch_to_volume_format(
            context, image_service, image_id,
            self.local_path(volume), volume_format,
            self.configuration.volume_dd_blocksize)

        image_utils.resize_image(self.local_path(volume), volume['size'])

        data = image_utils.qemu_img_info(self.local_path(volume))
        virt_size = data.virtual_size / units.Gi
        if virt_size != volume['size']:
            raise exception.ImageUnacceptable(
                image_id=image_id,
                reason=(_("Expected volume size was %d") % volume['size'])
                + (_(" but size is now %d") % virt_size))

    @utils.synchronized('smbfs', external=False)
    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""

        LOG.info(_('Cloning volume %(src)s to volume %(dst)s') %
                 {'src': src_vref['id'],
                  'dst': volume['id']})

        if src_vref['status'] != 'available':
            msg = _("Volume status must be 'available'.")
            raise exception.InvalidVolume(msg)

        volume_name = CONF.volume_name_template % volume['id']

        volume_info = {'provider_location': src_vref['provider_location'],
                       'size': src_vref['size'],
                       'id': volume['id'],
                       'name': volume_name,
                       'status': src_vref['status']}
        temp_snapshot = {'volume_name': volume_name,
                         'size': src_vref['size'],
                         'volume_size': src_vref['size'],
                         'name': 'clone-snap-%s' % src_vref['id'],
                         'volume_id': src_vref['id'],
                         'id': 'tmp-snap-%s' % src_vref['id'],
                         'volume': src_vref}
        self._create_snapshot(temp_snapshot)
        try:
            self._copy_volume_from_snapshot(temp_snapshot,
                                            volume_info)
        finally:
            self._delete_snapshot(temp_snapshot)

        return {'provider_location': src_vref['provider_location']}

    def _ensure_share_mounted(self, smbfs_share):
        mnt_flags = []
        if self.shares.get(smbfs_share) is not None:
            mnt_flags = self.shares[smbfs_share]
            # The domain name must be removed from the
            # user name when using Samba.
            mnt_flags = self.parse_credentials(mnt_flags).split()
        self._remotefsclient.mount(smbfs_share, mnt_flags)

    def parse_options(self, option_str):
        opts_dict = {}
        opts_list = []
        if option_str:
            for i in option_str.split():
                if i == '-o':
                    continue
                for j in i.split(','):
                    tmp_opt = j.split('=')
                    if len(tmp_opt) > 1:
                        opts_dict[tmp_opt[0]] = tmp_opt[1]
                    else:
                        opts_list.append(tmp_opt[0])
        return opts_list, opts_dict

    def parse_credentials(self, mnt_flags):
        options_list, options_dict = self.parse_options(mnt_flags)
        username = (options_dict.pop('user', None) or
                    options_dict.pop('username', None))
        if username:
            # Remove the Domain from the user name
            options_dict['username'] = username.split('\\')[-1]
        else:
            options_dict['username'] = 'guest'
        named_options = ','.join("%s=%s" % (key, val) for (key, val)
                                 in options_dict.iteritems())
        options_list = ','.join(options_list)
        flags = '-o ' + ','.join([named_options, options_list])

        return flags.strip(',')

    def _get_volume_format_spec(self, volume):
        extra_specs = []

        metadata_specs = volume.get('volume_metadata') or []
        extra_specs += metadata_specs

        vol_type = volume.get('volume_type')
        if vol_type:
            volume_type_specs = vol_type.get('extra_specs') or []
            extra_specs += volume_type_specs

        for spec in extra_specs:
            if 'volume_format' in spec.key:
                return spec.value
        return None

    def _is_file_size_equal(self, path, size):
        """Checks if file size at path is equal to size."""
        data = image_utils.qemu_img_info(path)
        virt_size = data.virtual_size / units.Gi
        return virt_size == size
