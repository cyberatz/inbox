"""
-----------------
GMAIL SYNC ENGINE
-----------------

Gmail is theoretically an IMAP backend, but it differs enough from standard
IMAP that we handle it differently. The state-machine rigamarole noted in
.imap.py applies, but we change a lot of the internal algorithms to fit Gmail's
structure.

Gmail has server-side threading, labels, and all messages are a subset of the
'All Mail' folder.

The only way to delete messages permanently on Gmail is to move a message to
the trash folder and then EXPUNGE.

We use Gmail's thread IDs locally, and download all mail via the All Mail
folder. We expand threads when downloading folders other than All Mail so the
user always gets the full thread when they look at mail.

"""
from __future__ import division

import os

from gevent import spawn, sleep
from gevent.queue import JoinableQueue
from sqlalchemy.orm.exc import NoResultFound

from inbox.util.itert import chunk, partition

from inbox.crispin import GmailSettingError
from inbox.log import get_logger
from inbox.models import Message, Folder, Thread, Namespace, Account
from inbox.models.backends.gmail import GmailAccount
from inbox.models.backends.imap import ImapUid, ImapThread
from inbox.mailsync.backends.base import (create_db_objects,
                                          commit_uids,
                                          MailsyncError,
                                          mailsync_session_scope)
from inbox.mailsync.backends.imap.generic import (
    _pool, uidvalidity_cb, safe_download, THROTTLE_WAIT)
from inbox.mailsync.backends.imap.condstore import CondstoreFolderSyncEngine
from inbox.mailsync.backends.imap.monitor import ImapSyncMonitor
from inbox.mailsync.backends.imap import common

PROVIDER = 'gmail'
SYNC_MONITOR_CLS = 'GmailSyncMonitor'


class GmailSyncMonitor(ImapSyncMonitor):
    def __init__(self, *args, **kwargs):
        kwargs['retry_fail_classes'] = [GmailSettingError]
        ImapSyncMonitor.__init__(self, *args, **kwargs)
        self.sync_engine_class = GmailFolderSyncEngine

    def sync(self):
        sync_folder_names_ids = self.prepare_sync()
        for folder_name, folder_id in sync_folder_names_ids:
            log.info('initializing folder sync')
            thread = GmailFolderSyncEngine(self.account_id, folder_name,
                                           folder_id,
                                           self.email_address,
                                           self.provider_name,
                                           self.poll_frequency,
                                           self.syncmanager_lock,
                                           self.refresh_flags_max,
                                           self.retry_fail_classes)
            thread.start()
            self.folder_monitors.add(thread)
            if thread.should_block:
                while not self._thread_polling(thread) and \
                        not self._thread_finished(thread) and \
                        not thread.ready():
                    sleep(self.heartbeat)

            # Allow individual folder sync monitors to shut themselves down
            # after completing the initial sync.
            if self._thread_finished(thread) or thread.ready():
                log.info('folder sync finished/killed',
                         folder_name=thread.folder_name)
                # NOTE: Greenlet is automatically removed from the group.

        self.folder_monitors.join()

log = get_logger()


class GmailFolderSyncEngine(CondstoreFolderSyncEngine):
    @property
    def should_block(self):
        """Whether to wait for this folder sync to enter the polling state
        before starting others. Used so we can do initial Inbox and All Mail
        syncs in parallel."""
        with _pool(self.account_id).get() as crispin_client:
            return not self.is_inbox(crispin_client)

    def is_inbox(self, crispin_client):
        return self.folder_name == crispin_client.folder_names()['inbox']

    def is_all_mail(self, crispin_client):
        return self.folder_name == crispin_client.folder_names()['all']

    def initial_sync_impl(self, crispin_client):
        assert crispin_client.selected_folder_name == self.folder_name
        self.save_initial_folder_info(crispin_client)
        download_stack = self.refresh_uids(crispin_client)
        changed_uid_channel = JoinableQueue()
        change_poller = spawn(self.poll_for_changes, changed_uid_channel)
        try:
            while download_stack:
                self.process_updates_in_initial_sync(crispin_client,
                                                     changed_uid_channel,
                                                     download_stack)
                # STOPSHIP(emfree): correctly manage thread-expansion here.
                self.sleep_if_throttled()
        finally:
            change_poller.kill()
        assert crispin_client.selected_folder_name == self.folder_name
        self.save_initial_folder_info(crispin_client)
        download_stack = self.setup_download_stack(crispin_client)

    def poll_impl(self):
        should_idle = False
        with self.conn_pool.get() as crispin_client:
            crispin_client.select_folder(self.folder_name, uidvalidity_cb)
            self.check_for_changes(crispin_client)
            if self.is_inbox(crispin_client):
                # Only idle on the inbox folder
                should_idle = True
                self.idle_wait(crispin_client)
        # Relinquish Crispin connection before sleeping.
        if not should_idle:
            sleep(self.poll_frequency)

    def __deduplicate_message_download(self, crispin_client, remote_g_metadata,
                                       uids):
        """
        Deduplicate message download using X-GM-MSGID.

        Returns
        -------
        list
            Deduplicated UIDs.

        """
        with mailsync_session_scope() as db_session:
            local_g_msgids = g_msgids(self.account_id, db_session,
                                      in_={remote_g_metadata[uid].msgid
                                           for uid in uids if uid in
                                           remote_g_metadata})

        full_download, imapuid_only = partition(
            lambda uid: uid in remote_g_metadata and
            remote_g_metadata[uid].msgid in local_g_msgids,
            sorted(uids, key=int))
        if imapuid_only:
            log.info('skipping already downloaded uids',
                     count=len(imapuid_only))
            # Since we always download messages via All Mail and create the
            # relevant All Mail ImapUids too at that time, we don't need to
            # create them again here if we're deduping All Mail downloads.
            if crispin_client.selected_folder_name != \
                    crispin_client.folder_names()['all']:
                add_new_imapuids(crispin_client, remote_g_metadata,
                                 self.syncmanager_lock, imapuid_only)

        return full_download

    def add_message_attrs(self, db_session, new_uid, msg, folder):
        """ Gmail-specific post-create-message bits. """
        # Disable autoflush so we don't try to flush a message with null
        # thread_id, causing a crash, and so that we don't flush on each
        # added/removed label.
        with db_session.no_autoflush:
            new_uid.message.g_msgid = msg.g_msgid
            # NOTE: g_thrid == g_msgid on the first message in the thread :)
            new_uid.message.g_thrid = msg.g_thrid

            # we rely on Gmail's threading instead of our threading algorithm.
            new_uid.message.thread_order = 0
            new_uid.update_imap_flags(msg.flags, msg.g_labels)

            thread = new_uid.message.thread = ImapThread.from_gmail_message(
                db_session, new_uid.account.namespace, new_uid.message)

            # make sure this thread has all the correct labels
            common.update_thread_labels(thread, folder.name, msg.g_labels,
                                        db_session)
            return new_uid

    def download_and_commit_uids(self, crispin_client, folder_name, uids):
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

    def __download_thread(self, all_mail_crispin_client, g_thrid):
        """
        Download all messages in thread identified by `g_thrid`.

        Messages are downloaded oldest-first via All Mail, which allows us
        to get the entire thread regardless of which folders it's in. We do
        oldest-first so that if the thread started with a message sent from the
        Inbox API, we can reconcile this thread appropriately with the existing
        message/thread.
        """
        thread_uids = all_mail_crispin_client.expand_thread(g_thrid)
        thread_g_metadata = all_mail_crispin_client.g_metadata(thread_uids)
        with self.thread_download_lock:
            log.debug('downloading thread',
                      g_thrid=g_thrid, message_count=len(thread_uids))
            to_download = self.__deduplicate_message_download(
                all_mail_crispin_client, thread_g_metadata, thread_uids)
            log.debug(deduplicated_message_count=len(to_download))
            for uids in chunk(to_download, all_mail_crispin_client.CHUNK_SIZE):
                self.download_and_commit_uids(
                    all_mail_crispin_client,
                    all_mail_crispin_client.selected_folder_name, uids)


def uid_download_folders(crispin_client):
    """ Folders that don't get thread-expanded. """
    return [crispin_client.folder_names()[tag] for tag in
            ('trash', 'spam') if tag in crispin_client.folder_names()]


def thread_expand_folders(crispin_client):
    """Folders that *do* get thread-expanded. """
    return [crispin_client.folder_names()[tag] for tag in ('inbox', 'all')]


def g_msgids(account_id, session, in_):
    if not in_:
        return []
    # Easiest way to account-filter Messages is to namespace-filter from
    # the associated thread. (Messages may not necessarily have associated
    # ImapUids.)
    in_ = {long(i) for i in in_}  # in case they are strings
    if len(in_) > 1000:
        # If in_ is really large, passing all the values to MySQL can get
        # deadly slow. (Approximate threshold empirically determined)
        query = session.query(Message.g_msgid).join(Thread).join(Namespace). \
            filter(Namespace.account_id == account_id).all()
        return sorted(g_msgid for g_msgid, in query if g_msgid in in_)
    # But in the normal case that in_ only has a few elements, it's way better
    # to not fetch a bunch of values from MySQL only to return a few of them.
    query = session.query(Message.g_msgid).join(Thread).join(Namespace). \
        filter(Namespace.account_id == account_id,
               Message.g_msgid.in_(in_)).all()
    return {g_msgid for g_msgid, in query}

def add_new_imapuids(crispin_client, remote_g_metadata, syncmanager_lock,
                     uids):
    """
    Add ImapUid entries only for (already-downloaded) messages.

    If a message has already been downloaded via another folder, we only need
    to add `ImapUid` accounting for the current folder. `Message` objects
    etc. have already been created.

    """
    flags = crispin_client.flags(uids)

    with syncmanager_lock:
        with mailsync_session_scope() as db_session:
            # Since we prioritize download for messages in certain threads, we
            # may already have ImapUid entries despite calling this method.
            local_folder_uids = {uid for uid, in
                                 db_session.query(ImapUid.msg_uid).join(Folder)
                                 .filter(
                                     ImapUid.account_id ==
                                     crispin_client.account_id,
                                     Folder.name ==
                                     crispin_client.selected_folder_name,
                                     ImapUid.msg_uid.in_(uids))}
            uids = [uid for uid in uids if uid not in local_folder_uids]

            if uids:
                acc = db_session.query(GmailAccount).get(
                    crispin_client.account_id)

                # collate message objects to relate the new imapuids to
                imapuid_for = dict([(metadata.msgid, uid) for (uid, metadata)
                                    in remote_g_metadata.items()
                                    if uid in uids])
                imapuid_g_msgids = [remote_g_metadata[uid].msgid for uid in
                                    uids]
                message_for = dict([(imapuid_for[m.g_msgid], m) for m in
                                    db_session.query(Message).join(ImapThread)
                                    .filter(
                                        Message.g_msgid.in_(imapuid_g_msgids),
                                        ImapThread.namespace_id ==
                                        acc.namespace.id)])

                # Stop Folder.find_or_create()'s query from triggering a flush.
                with db_session.no_autoflush:
                    new_imapuids = [ImapUid(
                        account=acc,
                        folder=Folder.find_or_create(
                            db_session, acc,
                            crispin_client.selected_folder_name),
                        msg_uid=uid, message=message_for[uid]) for uid in uids
                        if uid in message_for]
                    for item in new_imapuids:
                        # skip uids which have disappeared in the meantime
                        if item.msg_uid in flags:
                            item.update_imap_flags(flags[item.msg_uid].flags,
                                                   flags[item.msg_uid].labels)
                db_session.add_all(new_imapuids)
                db_session.commit()
