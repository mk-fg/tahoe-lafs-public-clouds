tahoe-lafs-public-clouds
--------------------

[Tahoe-LAFS](https://tahoe-lafs.org/) backend drivers for no-cost cloud
providers.

Project aims to provide necessary tools to allow using tahoe-lafs-provided
client-side security mechanisms and interfaces with no-cost public cloud storage
like skydrive, google drive and dropbox.

Previously, contents of this project were in my fork of tahoe-lafs
(cloud-backend* brnahces), but as code style and some pactices used here look
way different from upstream, decided that it'd be better to have a separate
project, to have clear boundary and avoid any confusion.

Necessary cloud-backend abstractions isn't in current Tahoe-LAFS releases yet
(as of 1.10a1), but available in
[LeastAuthority/tahoe-lafs](https://github.com/LeastAuthority/tahoe-lafs)
repository (or a local fork at
[mk-fg/tahoe-lafs](https://github.com/mk-fg/tahoe-lafs)).

I intentionally avoid calling providers mentioned above "free" as their policies
are usually openly hostile to users, developers or both, hence that word might
be confusing and innacurate in such context.


Installation
--------------------

Modules in these repo
(e.g. [skydrive](https://github.com/mk-fg/tahoe-lafs-public-clouds/tree/master/skydrive))
correspond to backend drivers, which currently should be placed into
"src/allmydata/storage/backends/cloud" directory inside tahoe-lafs source tree.

Dependency modules for these drivers are listed in the
[requirements.txt](https://github.com/mk-fg/tahoe-lafs-public-clouds/blob/master/requirements.txt)
file and can be either installed by hand or added to
"src/allmydata/_auto_deps.py" in tahoe-lafs, so that they'd be installed and
updated alongside other tahoe-lafs deps.

Finally, CLOUD_INTERFACES variable in
"src/allmydata/storage/backends/cloud/cloud_backend.py" should contain the new
backends, for example:

	CLOUD_INTERFACES = ("cloud.s3", "cloud.openstack", "cloud.googlestorage", "cloud.msazure", "cloud.skydrive")

As for the configuration - check out the
[doc/cloud.rst](https://github.com/mk-fg/tahoe-lafs-public-clouds/blob/master/doc/cloud.rst).

Alternatively, [cloud-backend-drivers
branch](https://github.com/mk-fg/tahoe-lafs/tree/cloud-backend-drivers) in a
local fork has all the changes above merged, along with this project linked as a
[git submodule](https://git.wiki.kernel.org/index.php/GitSubmoduleTutorial).
Just clone that one and you're all set.
