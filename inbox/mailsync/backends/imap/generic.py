# deal with unicode literals: http://www.python.org/dev/peps/pep-0263/
# vim: set fileencoding=utf-8 :
"""
----------------
IMAP SYNC ENGINE
----------------

Okay, here's the deal.

The IMAP sync engine runs per-folder on each account. This allows behaviour
like the Inbox to receive new mail via polling while we're still running the
initial sync on a huge All Mail folder.

Only one initial sync can be running per-account at a time, to avoid
hammering the IMAP backend too hard (Gmail shards per-user, so parallelizing
folder download won't actually increase our throughput anyway).

Any time we reconnect, we have to make sure the folder's uidvalidity hasn't
changed, and if it has, we need to update the UIDs for any messages we've
already downloaded. A folder's uidvalidity cannot change during a session
(SELECT during an IMAP session starts a session on a folder) (see
http://tools.ietf.org/html/rfc3501#section-2.3.1.1).

Note that despite a session giving you a HIGHESTMODSEQ at the start of a
SELECT, that session will still always give you the latest message list
including adds, deletes, and flag changes that have happened since that
highestmodseq. (In Gmail, there is a small delay between changes happening on
the web client and those changes registering on a connected IMAP session,
though bizarrely the HIGHESTMODSEQ is updated immediately.) So we have to keep
in mind that the data may be changing behind our backs as we're syncing.
Fetching info about UIDs that no longer exist is not an error but gives us
empty data.

Folder sync state is stored in the ImapFolderSyncStatus table to allow for
restarts.

Here's the state machine:


        -----
        |   ----------------         ----------------------
        ∨   | initial sync | <-----> | initial uidinvalid |
----------  ----------------         ----------------------
| finish |          |
----------          ∨
        ^   ----------------         ----------------------
        |---|      poll    | <-----> |   poll uidinvalid  |
            ----------------         ----------------------
            |  ∧
            ----

We encapsulate sync engine instances in greenlets for cooperative coroutine
scheduling around network I/O.

--------------
SESSION SCOPES
--------------

Database sessions are held for as short a duration as possible---just to
query for needed information or update the local state. Long-held database
sessions reduce scalability.

"""
from __future__ import division
import time
from datetime import datetime

from gevent import Greenlet, sleep, spawn
from sqlalchemy import desc, func
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import load_only

from inbox.util.concurrency import retry_and_report_killed
from inbox.util.itert import chunk
from inbox.util.misc import or_none
from inbox.util.threading import cleanup_subject, thread_messages
from inbox.basicauth import AuthError
from inbox.log import get_logger
log = get_logger()
from inbox.crispin import connection_pool, retry_crispin
from inbox.models import Folder, Account, Message
from inbox.models.backends.imap import (ImapFolderSyncStatus, ImapThread,
                                        ImapUid)
from inbox.mailsync.exc import UidInvalid
from inbox.mailsync.backends.imap import common
from inbox.mailsync.backends.base import (create_db_objects,
                                          commit_uids, MailsyncDone,
                                          mailsync_session_scope)

THROTTLE_WAIT = 60
MAX_THREAD_LENGTH = 500


def _pool(account_id):
    """Get a crispin pool, throwing an error if it's invalid."""
    try:
        return connection_pool(account_id)
    except AuthError:
        raise MailsyncDone()


class FolderSyncEngine(Greenlet):
    """Base class for a per-folder IMAP sync engine."""

    def __init__(self, account_id, folder_name, folder_id, email_address,
                 provider_name, poll_frequency, syncmanager_lock,
                 refresh_flags_max, retry_fail_classes):
        self.account_id = account_id
        self.folder_name = folder_name
        self.folder_id = folder_id
        self.poll_frequency = poll_frequency
        self.syncmanager_lock = syncmanager_lock
        self.refresh_flags_max = refresh_flags_max
        self.retry_fail_classes = retry_fail_classes
        self.state = None
        self.provider_name = provider_name

        with mailsync_session_scope() as db_session:
            account = db_session.query(Account).get(self.account_id)
            self.throttled = account.throttled
            self.namespace_id = account.namespace.id
            assert self.namespace_id is not None, "namespace_id is None"

        self.state_handlers = {
            'initial': self.initial_sync,
            'initial uidinvalid': self.resync_uids_from('initial'),
            'poll': self.poll,
            'poll uidinvalid': self.resync_uids_from('poll'),
            'finish': lambda self: 'finish',
        }

        Greenlet.__init__(self)

    def _run(self):
        # Bind greenlet-local logging context.
        log.new(account_id=self.account_id, folder=self.folder_name)
        return retry_and_report_killed(self._run_impl,
                                       account_id=self.account_id,
                                       folder_name=self.folder_name,
                                       logger=log,
                                       fail_classes=self.retry_fail_classes)

    def _run_impl(self):
        # We defer initializing the pool to here so that we'll retry if there
        # are any errors (remote server 503s or similar) when initializing it.
        self.conn_pool = _pool(self.account_id)
        saved_folder_status = self._load_state()
        # NOTE: The parent ImapSyncMonitor handler could kill us at any
        # time if it receives a shutdown command. The shutdown command is
        # equivalent to ctrl-c.
        while True:
            old_state = self.state
            try:
                self.state = self.state_handlers[old_state]()
            except UidInvalid:
                self.state = self.state + ' uidinvalid'
            # State handlers are idempotent, so it's okay if we're
            # killed between the end of the handler and the commit.
            if self.state != old_state:
                # Don't need to re-query, will auto refresh on re-associate.
                with mailsync_session_scope() as db_session:
                    db_session.add(saved_folder_status)
                    saved_folder_status.state = self.state
                    db_session.commit()
            if self.state == 'finish':
                return

    def _load_state(self):
        with mailsync_session_scope() as db_session:
            try:
                state = ImapFolderSyncStatus.state
                saved_folder_status = db_session.query(ImapFolderSyncStatus)\
                    .filter_by(account_id=self.account_id,
                               folder_id=self.folder_id).options(
                        load_only(state)).one()
            except NoResultFound:
                saved_folder_status = ImapFolderSyncStatus(
                    account_id=self.account_id, folder_id=self.folder_id)
                db_session.add(saved_folder_status)

            saved_folder_status.start_sync()
            db_session.commit()
            self.state = saved_folder_status.state
            return saved_folder_status

    @retry_crispin
    def initial_sync(self):
        log.bind(state='initial')
        log.info('starting initial sync')
        with self.conn_pool.get() as crispin_client:
            crispin_client.select_folder(self.folder_name, uidvalidity_cb)
            self.initial_sync_impl(crispin_client)
        return 'poll'

    @retry_crispin
    def poll(self):
        log.bind(state='poll')
        log.info('polling')
        self.poll_impl()
        return 'poll'

    def resync_uids_from(self, previous_state):
        @retry_crispin
        def resync_uids(self):
            """ Call this when UIDVALIDITY is invalid to fix up the database.

            What happens here is we fetch new UIDs from the IMAP server and
            match them with X-GM-MSGIDs and sub in the new UIDs for the old. No
            messages are re-downloaded.
            """
            log.error("UIDVALIDITY changed")
            raise NotImplementedError
            return previous_state
        return resync_uids

    def initial_sync_impl(self, crispin_client):
        assert crispin_client.selected_folder_name == self.folder_name
        download_stack = self.refresh_uids(crispin_client)
        last_refreshed_uids = time.time()
        flags_refresh_poller = spawn(self.poll_for_flag_changs)
        try:
            while download_stack:
                # Periodically refresh the download stack to detect new uids.
                if time.time() - last_refreshed_uids > self.poll_interval:
                    download_stack = self.refresh_uids(crispin_client)
                    last_refreshed_uids = time.time()
                uid = download_stack.pop()
                self.download_and_commit_uids(crispin_client, self.folder_name,
                                              [uid])
                self.sleep_if_throttled()
        finally:
            flags_refresh_poller.kill()

    def poll_impl(self):
        with self.conn_pool.get() as crispin_client:
            crispin_client.select_folder(self.folder_name, uidvalidity_cb)
            new_uids = self.refresh_uids(crispin_client)
            for uid in new_uids:
                self.download_and_commit_uids(crispin_client, self.folder_name,
                                              [uid])
            self.check_for_changes(crispin_client)
        sleep(self.poll_frequency)

    def refresh_uids(self, crispin_client):
        """Removes any local uids that have disappeared on the remote, and
        return any remote uids that we don't have locally."""
        remote_uids = crispin_client.all_uids()
        with mailsync_session_scope() as db_session:
            local_uids = self.local_uids(db_session)
            self.remove_deleted_uids(db_session, local_uids, remote_uids)
            self.update_uid_counts(db_session,
                                   remote_uid_count=len(remote_uids))

        new_uids = set(remote_uids) - local_uids
        return sorted(new_uids)

    def refresh_flags(self, crispin_client):
        with mailsync_session_scope() as db_session:
            local_uids = self.local_uids(db_session)
        to_refresh = sorted(local_uids)[-self.refresh_flags_max:]
        self.update_metadata(crispin_client, to_refresh)

    @retry_crispin
    def flags_refresh_poller(self):
        while True:
            with self.conn_pool.get() as crispin_client:
                crispin_client.select_folder(self.folder_name, uidvalidity_cb)
                self.refresh_flags(crispin_client)
            sleep(self.poll_frequency)

    def create_message(self, db_session, acct, folder, msg):
        assert acct is not None and acct.namespace is not None

        new_uid = common.create_imap_message(db_session, log, acct, folder,
                                             msg)
        new_uid = self.add_message_attrs(db_session, new_uid, msg, folder)
        return new_uid

    def fetch_similar_threads(self, db_session, new_uid):
        # FIXME: restrict this to messages in the same folder?
        clean_subject = cleanup_subject(new_uid.message.subject)
        # Return similar threads ordered by descending id, so that we append
        # to the most recently created similar thread.
        return db_session.query(ImapThread).filter(
            ImapThread.namespace_id == self.namespace_id,
            ImapThread.subject.like(clean_subject)). \
            order_by(desc(ImapThread.id)).all()

    def add_message_attrs(self, db_session, new_uid, msg, folder):
        """ Post-create-message bits."""
        with db_session.no_autoflush:
            parent_threads = self.fetch_similar_threads(db_session, new_uid)
            construct_new_thread = True

            if parent_threads:
                # If there's a parent thread that isn't too long already,
                # add to it. Otherwise create a new thread.
                parent_thread = parent_threads[0]
                parent_message_count, = db_session.query(
                    func.count(Message.id)). \
                    filter(Message.thread_id == parent_thread.id).one()
                if parent_message_count < MAX_THREAD_LENGTH:
                    construct_new_thread = False

            if construct_new_thread:
                new_uid.message.thread = ImapThread.from_imap_message(
                    db_session, new_uid.account.namespace, new_uid.message)
                new_uid.message.thread_order = 0
            else:
                # FIXME: arbitrarily select the first thread. This shouldn't
                # be a problem now but it could become one if we choose
                # to implement thread-splitting.
                parent_thread = parent_threads[0]
                parent_thread.messages.append(new_uid.message)
                constructed_thread = thread_messages(parent_thread.messages)
                for index, message in enumerate(constructed_thread):
                    message.thread_order = index

            # Make sure this thread has all the correct labels
            common.update_thread_labels(new_uid.message.thread, folder.name,
                                        [folder.canonical_name], db_session)
            new_uid.update_imap_flags(msg.flags)

            return new_uid

    def remove_deleted_uids(self, db_session, local_uids, remote_uids):
        """ Remove imapuid entries that no longer exist on the remote.

        Works as follows:
            1. Do a LIST on the current folder to see what messages are on the
                server.
            2. Compare to message uids stored locally.
            3. Purge messages we have locally but not on the server. Ignore
                messages we have on the server that aren't local.
        """
        if len(remote_uids) > 0 and len(local_uids) > 0:
            for elt in remote_uids:
                assert not isinstance(elt, str)

        to_delete = set(local_uids) - set(remote_uids)
        if to_delete:
            with self.syncmanager_lock:
                common.remove_messages(self.account_id, db_session, to_delete,
                                       self.folder_name)
        return to_delete

    def download_and_commit_uids(self, crispin_client, folder_name, uids):
        # Note that folder_name here might *NOT* be equal to self.folder_name,
        # because, for example, we download messages via the 'All Mail' folder
        # in Gmail.
        raw_messages = safe_download(crispin_client, uids)
        if not raw_messages:
            return 0
        with self.syncmanager_lock:
            with mailsync_session_scope() as db_session:
                new_imapuids = create_db_objects(
                    self.account_id, db_session, log, folder_name,
                    raw_messages, self.create_message)
                commit_uids(db_session, new_imapuids, self.provider_name)
        return len(new_imapuids)

    def update_metadata(self, crispin_client, updated):
        """ Update flags (the only metadata that can change). """

        # bigger chunk because the data being fetched here is very small
        for uids in chunk(updated, 5 * crispin_client.CHUNK_SIZE):
            new_flags = crispin_client.flags(uids)
            # Messages can disappear in the meantime; we'll update them next
            # sync.
            uids = [uid for uid in uids if uid in new_flags]
            with self.syncmanager_lock:
                with mailsync_session_scope() as db_session:
                    common.update_metadata(self.account_id, db_session,
                                           self.folder_name, self.folder_id,
                                           uids, new_flags)
                    db_session.commit()

    def update_uid_counts(self, db_session, **kwargs):
        saved_status = db_session.query(ImapFolderSyncStatus).join(Folder). \
            filter(ImapFolderSyncStatus.account_id == self.account_id,
                   Folder.name == self.folder_name).one()
        # We're not updating the current_remote_count metric
        # so don't update uid_checked_timestamp.
        if kwargs.get('remote_uid_count') is None:
            saved_status.update_metrics(kwargs)
        else:
            metrics = dict(uid_checked_timestamp=datetime.utcnow())
            metrics.update(kwargs)
            saved_status.update_metrics(metrics)

    def sleep_if_throttled(self):
        if self.throttled:
            # Check to see if the account's throttled state has been modified.
            # If so, immediately accelerate.
            with mailsync_session_scope() as db_session:
                acc = db_session.query(Account).get(self.account_id)
                self.throttled = acc.throttled
            if self.throttled:
                log.debug('throttled; sleeping')
            sleep(THROTTLE_WAIT)

    def local_uids(self, db_session):
        return {uid for uid, in db_session.query(ImapUid.msg_uid).
                filter(ImapUid.account_id == self.account_id,
                       ImapUid.folder_id == self.folder_id)}


def uidvalidity_cb(account_id, folder_name, select_info):
    assert folder_name is not None and select_info is not None, \
        "must start IMAP session before verifying UIDVALIDITY"
    with mailsync_session_scope() as db_session:
        saved_folder_info = common.get_folder_info(account_id, db_session,
                                                   folder_name)
        saved_uidvalidity = or_none(saved_folder_info, lambda i:
                                    i.uidvalidity)
    selected_uidvalidity = select_info['UIDVALIDITY']

    if saved_folder_info:
        is_valid = common.uidvalidity_valid(account_id,
                                            selected_uidvalidity,
                                            folder_name, saved_uidvalidity)
        if not is_valid:
            raise UidInvalid(
                'folder: {}, remote uidvalidity: {}, '
                'cached uidvalidity: {}'.format(
                    folder_name, selected_uidvalidity, saved_uidvalidity))
    return select_info


def safe_download(crispin_client, uids):
    try:
        raw_messages = crispin_client.uids(uids)
    except MemoryError, e:
        log.error('ran out of memory while fetching UIDs', uids=uids)
        raise e

    return raw_messages
