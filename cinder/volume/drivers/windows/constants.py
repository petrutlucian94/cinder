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

# As described by the Win32 API
VHD_TYPE_FIXED = 2
VHD_TYPE_DYNAMIC = 3
VHD_TYPE_DIFFERENCING = 4

# Subformat names used by qemu-img
VHD_SUBFORMAT_FIXED = 'fixed'
VHD_SUBFORMAT_DYNAMIC = 'dynamic'
VHD_SUBFORMAT_DIFFERENCING = 'differencing'

VHD_TYPE_MAP = {
    VHD_SUBFORMAT_FIXED: VHD_TYPE_FIXED,
    VHD_SUBFORMAT_DYNAMIC: VHD_TYPE_DYNAMIC,
    VHD_SUBFORMAT_DIFFERENCING: VHD_SUBFORMAT_DIFFERENCING
}

VHD_SUBFORMAT_MAP = {
    VHD_TYPE_FIXED: VHD_SUBFORMAT_FIXED,
    VHD_TYPE_DYNAMIC: VHD_SUBFORMAT_DYNAMIC,
    VHD_TYPE_DIFFERENCING: VHD_SUBFORMAT_DIFFERENCING
}
