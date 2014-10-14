import sys
import datetime

import sqlalchemy.orm.exc

from inbox.log import get_logger
log = get_logger()

from inbox.auth import AuthHandler
from inbox.auth.oauth import connect_account as oauth_connect_account
from inbox.auth.oauth import verify_account as oauth_verify_account
from inbox.oauth import oauth_authorize_console
from inbox.models import Namespace
from inbox.config import config
from inbox.models.backends.exchange import ExchangeAccount

PROVIDER = 'exchange'
