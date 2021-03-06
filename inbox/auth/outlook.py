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
from inbox.models.backends.outlook import OutlookAccount

PROVIDER = 'outlook'

# Outlook OAuth app credentials
OAUTH_CLIENT_ID = config.get_required('MS_LIVE_OAUTH_CLIENT_ID')
OAUTH_CLIENT_SECRET = config.get_required('MS_LIVE_OAUTH_CLIENT_SECRET')
OAUTH_REDIRECT_URI = config.get_required('MS_LIVE_OAUTH_REDIRECT_URI')

OAUTH_AUTHENTICATE_URL = 'https://login.live.com/oauth20_authorize.srf'
OAUTH_ACCESS_TOKEN_URL = 'https://login.live.com/oauth20_token.srf'
OAUTH_USER_INFO_URL = 'https://apis.live.net/v5.0/me'
OAUTH_BASE_URL = 'https://apis.live.net/v5.0/'

OAUTH_SCOPE = ' '.join([
    'wl.basic',            # Read access for basic profile info + contacts
    'wl.offline_access',   # ability to read / update user's info at any time
    'wl.signin',           # users already signed in:  also signed in to app
    'wl.emails',           # Read access to user's email addresses
    'wl.imap'             # R/W access to user's email using IMAP / SMTP
    ])


def _this_module():
    return sys.modules[__name__]


def create_auth_account(db_session, email_address, token, exit):
    response = _auth_account(email_address, token, exit)
    account = create_account(db_session, response)

    return account


def _auth_account(email_address, token, exit):
    if not token:
        print ("Please visit the following url to allow access to this "
               "application. The response will provide "
               "code=[AUTHORIZATION_CODE]&lc=XXXX in the location. Paste the"
               " AUTHORIZATION_CODE here:")
    return oauth_authorize_console(_this_module(), email_address, token, exit)


def create_account(db_session, response):
    email_address = response.get('emails')['account']
    try:
        account = db_session.query(OutlookAccount).filter_by(
            email_address=email_address).one()
    except sqlalchemy.orm.exc.NoResultFound:
        namespace = Namespace()
        account = OutlookAccount(namespace=namespace)

    account.refresh_token = response['refresh_token']
    account.date = datetime.datetime.utcnow()
    tok = response.get('access_token')
    expires_in = response.get('expires_in')
    account.set_access_token(tok, expires_in)
    account.scope = response.get('scope')
    account.email_address = email_address
    account.o_id_token = response.get('user_id')
    account.o_id = response.get('id')
    account.name = response.get('name')
    account.gender = response.get('gender')
    account.link = response.get('link')
    account.locale = response.get('locale')

    return account


def connect_account(provider, email, pw):
    return oauth_connect_account(provider, email, pw)


def verify_account(account):
    return oauth_verify_account(account)


class OutlookAuthHandler(AuthHandler):
    connect_account = staticmethod(connect_account)
    create_account = staticmethod(create_account)
    create_auth_account = staticmethod(create_auth_account)
    verify_account = staticmethod(verify_account)

    OAUTH_CLIENT_ID = OAUTH_CLIENT_ID
    OAUTH_CLIENT_SECRET = OAUTH_CLIENT_SECRET
    OAUTH_REDIRECT_URI = OAUTH_REDIRECT_URI
    OAUTH_AUTHENTICATE_URL = OAUTH_AUTHENTICATE_URL
    OAUTH_ACCESS_TOKEN_URL = OAUTH_ACCESS_TOKEN_URL
    OAUTH_USER_INFO_URL = OAUTH_USER_INFO_URL
    OAUTH_BASE_URL = OAUTH_BASE_URL
    OAUTH_SCOPE = OAUTH_SCOPE
