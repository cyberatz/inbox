from datetime import datetime

import nacl

from inbox.models import Account, Message
from inbox.models.util import EncryptionScheme
from inbox.models.roles import RESERVED, KEY_VERSION
from ..data.messages.replyto_message import message

ACCOUNT_ID = 1
THREAD_ID = 1


def test_local_storage(db, config):
    account = db.session.query(Account).get(ACCOUNT_ID)
    m = Message(db.session, account=account, mid='', folder_name='',
                received_date=datetime.utcnow(),
                flags='', body_string=message)
    m.thread_id = THREAD_ID

    db.session.add(m)
    db.session.commit()

    msg = db.session.query(Message).get(m.id)

    # Ensure .data will access and decrypt the encrypted data from disk
    assert not hasattr(msg, '_data')

    for b in [p.block for p in msg.parts]:
        assert b.encryption_scheme == \
            EncryptionScheme.SECRETBOX_WITH_STATIC_KEY

        lookup_key = b.stored_name
        assert lookup_key

        # Accessing .data verifies data integrity
        data = b.data

        modified_lookup_key = ''.join(lookup_key.split('/'))
        raw = b._get_from_disk(modified_lookup_key)
        assert data != raw

        assert raw[:len(RESERVED)] == RESERVED and \
            raw[len(RESERVED):len(RESERVED) + len(KEY_VERSION)] == KEY_VERSION

        value = nacl.secret.SecretBox(
            key=config.get_required('BLOCK_ENCRYPTION_KEY'),
            encoder=nacl.encoding.HexEncoder
        ).decrypt(
            raw[len(RESERVED) + len(KEY_VERSION):],
            encoder=nacl.encoding.HexEncoder)

        assert data == value


def test_local_deduplication(db, config):
    account = db.session.query(Account).get(ACCOUNT_ID)

    # Message 1
    m = Message(db.session, account=account, mid='', folder_name='',
                received_date=datetime.utcnow(),
                flags='', body_string=message)
    m.thread_id = THREAD_ID

    db.session.add(m)
    db.session.commit()

    count = len(m.parts)

    blocks = []
    storednames = []
    for b in [p.block for p in m.parts]:
        storednames.append(b.stored_name)
        blocks.append(b.id)

    # Identical message 2
    m2 = Message(db.session, account=account, mid='', folder_name='',
                 received_date=datetime.utcnow(),
                 flags='', body_string=message)
    m2.thread_id = THREAD_ID

    db.session.add(m2)
    db.session.commit()

    new_count = len(m2.parts)
    assert new_count == count

    for b in [p.block for p in m2.parts]:
        assert b.id not in blocks and b.stored_name in storednames


# TODO[k]
def test_s3_storage():
    pass
