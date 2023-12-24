"""
See https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Edits/Mediawiki_history_dumps
for a description of the input format and schema.

"""

from dataclasses import dataclass
import re

field_names = [
    # See "Schema details" section of
    # https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Edits/Mediawiki_history_dumps
    
    # Event global
    "wiki_db",
    "event_entity",
    "event_type",
    "event_timestamp",
    "event_comment_escaped",

    # Event user
    "event_user_id",
    "event_user_text_historical_escaped",
    "event_user_text_escaped",
    "event_user_blocks_historical_string",
    "event_user_blocks_string",
    "event_user_groups_historical_string",
    "event_user_groups_string",
    "event_user_is_bot_by_historical_string",
    "event_user_is_bot_by_string",
    "event_user_is_created_by_self",
    "event_user_is_created_by_system",
    "event_user_is_created_by_peer",
    "event_user_is_anonymous",
    "event_user_registration_timestamp",
    "event_user_creation_timestamp",
    "event_user_first_edit_timestamp",
    "event_user_revision_count",
    "event_user_seconds_since_previous_revision",

    # Page
    "page_id",
    "page_title_historical_escaped",
    "page_title_escaped",
    "page_namespace_historical",
    "page_namespace_is_content_historical",
    "page_namespace",
    "page_namespace_is_content",
    "page_is_redirect",
    "page_is_deleted",
    "page_creation_timestamp",
    "page_first_edit_timestamp",
    "page_revision_count",
    "page_seconds_since_previous_revision",

    # User
    "user_id",
    "user_text_historical_escaped",
    "user_text_escaped",
    "user_blocks_historical_string",
    "user_blocks_string",
    "user_groups_historical_string",
    "user_groups_string",
    "user_is_bot_by_historical_string",
    "user_is_bot_by_string",
    "user_is_created_by_self",
    "user_is_created_by_system",
    "user_is_created_by_peer",
    "user_is_anonymous",
    "user_registration_timestamp",
    "user_creation_timestamp",
    "user_first_edit_timestamp",

    # Revision
    "revision_id",
    "revision_parent_id",
    "revision_minor_edit",
    "revision_deleted_parts_string",
    "revision_deleted_parts_are_suppressed",
    "revision_text_bytes",
    "revision_text_bytes_diff",
    "revision_text_sha1",
    "revision_content_model",
    "revision_content_format",
    "revision_is_deleted_by_page_deletion",
    "revision_deleted_by_page_deletion_timestamp",
    "revision_is_identity_reverted",
    "revision_first_identity_reverting_revision_id",
    "revision_seconds_to_identity_revert",
    "revision_is_identity_revert",
    "revision_is_from_before_page_creation",
    "revision_tags_string",
]


@dataclass
class Row:
    wiki_db: str
    event_entity: str
    event_type: str
    event_timestamp: str
    event_comment: str
    event_user_id: int
    event_user_text_historical: str
    event_user_text: str
    event_user_blocks_historical: list[str]
    event_user_blocks: list[str]
    event_user_groups_historical: list[str]
    event_user_groups: list[str]
    event_user_is_bot_by_historical: list[str]
    event_user_is_bot_by: list[str]
    event_user_is_created_by_self: bool
    event_user_is_created_by_system: bool
    event_user_is_created_by_peer: bool
    event_user_is_anonymous: bool
    event_user_registration_timestamp: str
    event_user_creation_timestamp: str
    event_user_first_edit_timestamp: str
    event_user_revision_count: int
    event_user_seconds_since_previous_revision: int
    page_id: int
    page_title_historical: str
    page_title: str
    page_namespace_historical: int
    page_namespace_is_content_historical: bool
    page_namespace: int
    page_namespace_is_content: bool
    page_is_redirect: bool
    page_is_deleted: bool
    page_creation_timestamp: str
    page_first_edit_timestamp: str
    page_revision_count: int
    page_seconds_since_previous_revision: int
    user_id: int
    user_text_historical: str
    user_text: str
    user_blocks_historical: list[str]
    user_blocks: list[str]
    user_groups_historical: list[str]
    user_groups: list[str]
    user_is_bot_by_historical: list[str]
    user_is_bot_by: list[str]
    user_is_created_by_self: bool
    user_is_created_by_system: bool
    user_is_created_by_peer: bool
    user_is_anonymous: bool
    user_registration_timestamp: str
    user_creation_timestamp: str
    user_first_edit_timestamp: str
    revision_id: int
    revision_parent_id: int
    revision_minor_edit: bool
    revision_deleted_parts: list[str]
    revision_deleted_parts_are_suppressed: bool
    revision_text_bytes: int
    revision_text_bytes_diff: int
    revision_text_sha1: str
    revision_content_model: str
    revision_content_format: str
    revision_is_deleted_by_page_deletion: bool
    revision_deleted_by_page_deletion_timestamp: str
    revision_is_identity_reverted: bool
    revision_first_identity_reverting_revision_id: int
    revision_seconds_to_identity_revert: int
    revision_is_identity_revert: bool
    revision_is_from_before_page_creation: bool
    revision_tags: list[str]

    
def build_row(tsv_string):
    """Build a Row from a tsv records string.

    Missing scalar fields in the input record are represented as None.
    Missing string-array fields are represented as empty lists.

    Early versions of the Mediawiki software stored empty revision
    comments as '*' in the database; this is preserved in the
    denomalized history files.  It's not clear how this should be
    handled by build_row().  For now we preserve the '*' in
    Row.event_comment, but that may change in the future.

    """
    fields = tsv_string.split('\t')
    name_types = Row.__annotations__
    args = {}
    for value, name, type in zip(fields, name_types.keys(), name_types.values()):
        if type == str:
            args[name] = unescape_tnr(value)
        elif type == int:
            args[name] = None if value == '' else int(value)
        elif type == bool:
            if value == '':
                args[name] = None
            elif value == 'true':
                args[name] = True
            elif value == 'false':
                args[name] = False
            else:
                raise ValueError(f'{name=}, {type=}, value={repr(value)}')
        elif type == list[str]:
            args[name] = expand_string_array(value)
        else:
            raise RuntimeError(f'{type=}.  This should never happen!')
    return Row(**args)


def unescape_tnr(s):
    r"""Expand literal \t, \n, and \r in s.

    """
    return s.replace('\\t', '\t').replace('\\n', '\n').replace('\\r', '\r')


def unescape_tnr_comma(input_string):
    r"""Expand literal \t, \n, ,\r, and \, in input_string.

    """
    return unescape_tnr(input_string).replace('\\,', ',')


def expand_string_array(input_string):
    """Expand encoded string arrays.

    """
    if input_string == '':
        return []
    strings = re.split(r'(?<!\\),', input_string)
    return [unescape_tnr_comma(s) for s in strings]


def escape_tnr(s):
    r"""Inverse of unescape_tnr()

    """
    return s.replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
