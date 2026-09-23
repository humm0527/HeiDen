"""Credential handling without persistence or authentication calls."""
import json
import os
from urllib.parse import quote, quote_plus, unquote, urlsplit

KEYS = ('RQDATA_USERNAME', 'RQDATA_PASSWORD', 'RQDATAC_CONF', 'RQDATAC2_CONF')


def configured(env=None):
    env = os.environ if env is None else env
    return bool(env.get('RQDATAC_CONF') or env.get('RQDATAC2_CONF') or
                (env.get('RQDATA_USERNAME') and env.get('RQDATA_PASSWORD')))


def without_credentials(env):
    return {key: value for key, value in env.items()
            if not key.upper().startswith('RQDATA')
            and key.upper() != 'RISKAUDIT_RQDATA_PROXY_FALLBACK'}


def secret_values(env=None):
    env = os.environ if env is None else env
    values = set()
    for key in (*KEYS, 'RQDATAC_PROXY', 'RISKAUDIT_RQDATA_PROXY_FALLBACK'):
        value = env.get(key)
        if not value:
            continue
        pieces = {value}
        if '://' in value:
            try:
                parsed = urlsplit(value)
                pieces.update(unquote(item) for item in (parsed.username, parsed.password) if item)
            except ValueError:
                pass
        for piece in pieces:
            values.update((piece, quote(piece, safe=''), quote_plus(piece, safe=''),
                           json.dumps(piece, ensure_ascii=True)[1:-1]))
    return sorted(values, key=len, reverse=True)


def redact(text, values=None):
    for value in secret_values() if values is None else values:
        text = text.replace(value, '[已隐藏]')
    return text
