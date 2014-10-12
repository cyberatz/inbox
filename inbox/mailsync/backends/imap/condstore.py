"""
-----------------
GENERIC IMAP SYNC ENGINE (~WITH~ COND STORE)
-----------------

Generic IMAP backend with CONDSTORE support.

No support for server-side threading, so we have to thread messages ourselves.

"""
from gevent import sleep, spawn
from gevent.queue import JoinableQueue
from inbox.crispin import retry_crispin
from inbox.mailsync.backends.base import (save_folder_names,
                                          mailsync_session_scope)
from inbox.mailsync.backends.imap import common
from inbox.mailsync.backends.imap.generic import (FolderSyncEngine,
                                                  uidvalidity_cb)
from inbox.log import get_logger
log = get_logger()

IDLE_FOLDERS = ['inbox', 'sent mail']


class CondstoreFolderSyncEngine(FolderSyncEngine):
    @property
    def should_idle(self):
        return self.folder_name.lower() in IDLE_FOLDERS

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
                uid = download_stack.pop()
                self.download_and_commit_uids(crispin_client, self.folder_name,
                                              [uid])
                self.sleep_if_throttled()
        finally:
            change_poller.kill()

    def poll_impl(self):
        with self.conn_pool.get() as crispin_client:
            self.check_for_changes(crispin_client)
            if self.should_idle:
                self.idle_wait(crispin_client)
        # Close IMAP connection before sleeping
        if not self.should_idle:
            sleep(self.poll_frequency)

    @retry_crispin
    def poll_for_changes(self, changed_uid_channel):
        log.new(account_id=self.account_id, folder=self.folder_name)
        while True:
            log.debug('polling for changes during initial sync')
            with self.conn_pool.get() as crispin_client:
                self.check_for_changes(crispin_client, changed_uid_channel)
                if self.should_idle:
                    self.idle_wait(crispin_client)
            # Close IMAP connection before sleeping
            if not self.should_idle:
                sleep(self.poll_frequency)

    def process_updates_in_initial_sync(self, crispin_client,
                                        changed_uid_channel, download_stack):
        if not changed_uid_channel.empty():
            changed_uids = changed_uid_channel.get()
            self.handle_changes(crispin_client, changed_uids)
            # Remove any uids from the stack that were preemptively downloaded
            # (e.g., if their flags changed).
            for u in changed_uids:
                if u in download_stack:
                    download_stack.remove(u)
            changed_uid_channel.task_done()

    def check_for_changes(self, crispin_client, change_channel=None):
        """Checks for new and updated uids using the HIGHESTMODSEQ facility. If
        a value for `change_channel` is given, passes the uids off to it to be
        handled by the main download greenlet during initial sync. Otherwise,
        directly persists the updates."""
        crispin_client.select_folder(self.folder_name, uidvalidity_cb)
        new_highestmodseq = crispin_client.selected_highestmodseq
        new_uidvalidity = crispin_client.selected_uidvalidity
        with mailsync_session_scope() as db_session:
            saved_folder_info = common.get_folder_info(
                self.account_id, db_session, self.folder_name)
            saved_highestmodseq = saved_folder_info.highestmodseq
            if new_highestmodseq == saved_highestmodseq:
                return
            elif new_highestmodseq < saved_highestmodseq:
                # This should really never happen, but if it does, handle it.
                log.warning('got server highestmodseq less than saved '
                            'highestmodseq',
                            new_highestmodseq=new_highestmodseq,
                            saved_highestmodseq=saved_highestmodseq)
                return
            save_folder_names(log, self.account_id,
                              crispin_client.folder_names(), db_session)
        # TODO(emfree): doing the HIGHESTMODSEQ search can be super slow on
        # large folders. We could split this into 'fast but partial' and 'slow
        # but complete' stages, so that we can persist new uids quickly but
        # still have full flags consistency.
        changed_uids = crispin_client.new_and_updated_uids(saved_highestmodseq)
        if changed_uids:
            if change_channel is not None:
                change_channel.put(changed_uids)
                change_channel.join()
            else:
                self.handle_changes(crispin_client, changed_uids)

        with mailsync_session_scope() as db_session:
            common.update_folder_info(self.account_id, db_session,
                                      self.folder_name, new_uidvalidity,
                                      new_highestmodseq)
            db_session.commit()

    def handle_changes(self, crispin_client, changed_uids):
        with mailsync_session_scope() as db_session:
            local_uids = self.local_uids(db_session)
        changed_uids = set(changed_uids)
        new_uids = changed_uids - local_uids
        updated_uids = changed_uids & local_uids
        for uid in new_uids:
            self.download_and_commit_uids(crispin_client, self.folder_name,
                                          [uid])
        self.update_metadata(crispin_client, updated_uids)
        remote_uids = crispin_client.all_uids()
        with mailsync_session_scope() as db_session:
            self.remove_deleted_uids(db_session, local_uids, remote_uids)

    def idle_wait(self, crispin_client):
        # Idle doesn't pick up flag changes, so we don't want to
        # idle for very long, or we won't detect things like
        # messages being marked as read.
        idle_frequency = 30
        log.info('idling', timeout=idle_frequency)
        crispin_client.conn.idle()
        crispin_client.conn.idle_check(timeout=idle_frequency)
        crispin_client.conn.idle_done()

    def save_initial_folder_info(self, crispin_client):
        """Ensures that we have an initial highestmodseq value stored before we
        begin syncing."""
        crispin_client.select_folder(self.folder_name, uidvalidity_cb)
        with mailsync_session_scope() as db_session:
            saved_folder_info = common.get_folder_info(
                self.account_id, db_session, self.folder_name)
            # Ensure that we have an initial highestmodseq value stored before
            # we begin checking for changes.
            if saved_folder_info is None:
                assert (crispin_client.selected_uidvalidity is not None
                        and crispin_client.selected_highestmodseq is
                        not None)
                saved_folder_info = common.update_folder_info(
                    crispin_client.account_id, db_session,
                    self.folder_name,
                    crispin_client.selected_uidvalidity,
                    crispin_client.selected_highestmodseq)
                db_session.commit()
