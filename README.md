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
