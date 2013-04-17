================================
Storing Shares on Cloud Services
================================

The Tahoe-LAFS storage server can be configured to store its shares on a
cloud storage service, rather than on the local filesystem.



Microsoft SkyDrive
==================

SkyDrive is a free (with storage space limitations) and commercial file hosting
service `<http://skydrive.com>`_.

Unfortunately, to use SkyDrive service, you'll need to preform quite a few
manual authorization steps, described below.


Authorization
-------------

SkyDrive API requires to register an application in `DevCenter`_, providing you
with client_id and client_secret strings, used for authentication.
According to LiveConnect ToS "You are solely and entirely responsible for all
uses of Live Connect occurring under your Client ID".

App registration in DevCenter is really straightforward and shouldn't take more
than a few clicks.
You should receive "Client ID" and "Client Secret" strings there.
Also be sure to check the "mobile client app" box under "API settings".

Then you need to perform OAuth 2.0 authorization by going to a special URL,
confirming access there and copying resulting URL back to tahoe configuration
file.

To do that, set ``backend = cloud.skydrive`` in ``tahoe.cfg`` file, fill in
``skydrive.client_id`` with received Client ID value and create
``private/skydrive_client_secret`` file with Client Secret.

Then use ``tahoe run [ NODEDIR ]`` to run the node, it will detect that only
client_id and client_secret values are provided and print the error with URL to
visit along with further authentication instructions.

Open that URL in a web browser, authorize with live.com credentials, confirm
access permissions, then copy URL of a resulting blank page (starting with
"https://login.live.com/oauth20_desktop.srf") back into
``private/skydrive_auth_code`` file.

.. _DevCenter: https://manage.dev.live.com/


Configuration parameters
------------------------

To enable storing shares on SkyDrive, add the following keys to the server's
``tahoe.cfg`` file:

``[storage]``

``backend = cloud.skydrive``

    This turns off the local filesystem backend and enables use of SkyDrive.

``skydrive.folder_path = (string, required, unless skydrive.folder_id is set)``

``skydrive.folder_id = (string, required, unless skydrive.folder_path is set)``

    Specify either one of these two (not both!) to set the remote folder in
    which to store Tahoe-LAFS shares.

``skydrive.client_id = (string, required)``

    Client ID should be acquired from Microsoft DevCenter, as described in the
    previous section.

``private/skydrive_client_secret (required)``

    Write Client Secret (acquired along with Client ID) into this file.

``private/skydrive_auth_code (required, see below)``

    API authorization code should be written to this file.

    If this file doesn't exists or is empty (and all the required options listed
    above are set), starting tahoe node with this backend condigured will result
    in it printing authorization instructions and exiting.

    See _`Authorization` section above for more detailed description of that
    process.

``private/skydrive_access_token (optional)``

``private/skydrive_refresh_token (optional)``

    These files contain transient tokens and may be written by hand initially,
    but generally should only be auto-updated and are non-critical.

``skydrive.folder_buckets = (integer, positive integer, optional), default 1``

    Number of subfolders (aka storage buckets) to distribute shares between.

    Multiple folders will take additional http requests to create and traverse
    (on shares' discovery), but may work faster and avoid API limitations and
    shortcomings.

    Share chunks will be balanced between these evenly.

    Can be changed at any time. No rebalancing to even number of files will be
    performed automatically, but it can be done manually when node is offline.

    If set to 1 (default), no additional folders will be created.

``skydrive.api.ratelimit.interval = (positive float, 0 - no limit, optional), default 0``

``skydrive.api.ratelimit.burst = (positive integer, optional), default 1``

    Parameters to limit the rate of any requests to the API.

    If ``skydrive.api.ratelimit.interval`` is set to non-zero, `Token Bucket`_
    rate-limiting algorithm (with specified parameters) will be used to
    introduce delays before making requests when necessary, to prevent API from
    blocking access due to their frequency.

    Mainly useful if lots of small files are uploaded or lots of directories are
    created in rapid succession via free account.

    .. _Token Bucket: https://en.wikipedia.org/wiki/Token_Bucket

``skydrive.api.url = (string, optional)``

    Overrides default URL of the Microsoft LiveConnect SkyDrive API (version 5).

    Can be used with some proxying http daemon to debug, limit or tunnel API
    requests through intermediate server.
    Should be left alone in all but the most unusual use-cases.

``skydrive.api.debug = (boolean, optional)``

    Enable logging of all API http requests.

    Only intended for debugging purposes, should *never* be used in production
    environment, as it is quite noisy and will dump API authentication data.

    Actual data, stored in the lafs grid, should be safe, as it is transmitted
    to/from the storage (API, in this case) in a securely-encrypted form.

``skydrive.api.timeout.* = (positive float, 0 - no limit, optional)``

    Timeouts for I/O activity on various stages of HTTP requests.

    Replace asterisk with a name of a request handling state from
    ``txSkyDriveAPI.request_io_timeouts``.

    For example, `skydrive.api.timeout.res_body = 10` will set timeout between
    individual socket read/write operations when receiving request response body
    (i.e. share data download) to 10 seconds.

    Note that most timeouts there (e.g. "req_body" or "res_body") are not
    applied to the whole operation and should work with uploads and downloads of
    any size (there can be any number of data-receiving operations), but will
    only be triggered if not a single byte of data will be sent/received to/from
    a remote server for a specified time interval.



box.net
=======

Free and commercial file hosting and collaboration service `<http://box.net>`_.

Driver is virtually identical to the one for SkyDrive, including authorization
step (at `<http://www.box.net/developers/services>`_) and all the options (only
they use "box." namespace).

To enable it, use: ``backend = cloud.boxdotnet``



Ubuntu One
==========

Ubuntu One is a free / commercial file hosting service
`<https://one.ubuntu.com/>`_.

It's "Files Cloud" HTTP API seem to be among the simpliest the similar REST APIs
I've seen, second only to totally standard WebDAV of Yandex Disk service.


Authorization
-------------

Driver needs OAuth 1.0 credentials (consumer key/secret, token and token
secret) to work.

These have to be stored in corresponding files within a "private" subdir of a
node directory:

 - private/u1_consumer_key
 - private/u1_consumer_secret
 - private/u1_token
 - private/u1_token_secret

All of them are alphanumeric strings of 5-50 characters and must be acquired
from Ubuntu Single Sign On (Ubuntu SSO) service.

"u1-cli" tool (that comes with `txu1 <https://github.com/mk-fg/txu1>` python
module) can be used to get these credentials - just run ``u1-cli auth`` and
it'll query email/password, then printing consumer/token data to stdout.


Configuration parameters
------------------------

To enable storing shares on SkyDrive, add the following keys to the server's
``tahoe.cfg`` file:

``[storage]``

``backend = cloud.u1``

    This turns off the local filesystem backend and enables Ubuntu One backend.

``u1.path = (string, required)``

    A path to use to store LAFS shares in. Must include volume.

    Example: /~/myvolume/tahoe/storage

``u1.dir_buckets = (integer, positive integer, optional), default 1``

    Number of subfolders (aka storage buckets) to distribute shares between.

    Multiple folders will take additional http requests to create and traverse
    (on shares' discovery), but may work faster and avoid API limitations and
    shortcomings.

    Share chunks will be balanced between these evenly.

    Can be changed at any time. No rebalancing to even number of files will be
    performed automatically, but it can be done manually when node is offline.

    If set to 1 (default), no additional folders will be created.

``private/u1_consumer_key (required)``

``private/u1_consumer_secret (required)``

``private/u1_token (required)``

``private/u1_token_secret (required)``

    U1 OAuth 1.0 credentials should be written (by hand) to these files.

    See "Authorization" section above for more information.

``u1.api.debug = (boolean, optional)``

``u1.api.ratelimit.interval = (positive float, 0 - no limit, optional), default 0``

``u1.api.ratelimit.burst = (positive integer, optional), default 1``

``u1.api.timeout.* = (positive float, 0 - no limit, optional)``

    Same as for SkyDrive and box.net, see more detailed description above.
