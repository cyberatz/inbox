rom sqlalchemy import Column, Integer, String, ForeignKey

from inbox.models.backends.imap import ImapAccount
from inbox.models.backends.oauth import OAuthAccount

PROVIDER = 'exchange'
