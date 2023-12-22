import pytest

from schema import Row, build_row, unescape_tnr, unescape_tnr_comma


ROW_1 = r'enwiki	revision	create	2001-01-15 19:27:13.0	*	11313587	Office.bomis.com	Office.bomis.com							false	false	true	false	2009-12-28 09:06:07.0	2009-12-28 09:06:09.0	2001-01-15 19:27:13.0	1		26323569	HomePage	HomePage	0	true	0	true	true	false	2010-02-24 14:25:49.0	2001-01-15 19:27:13.0	1																		908493298	0	false		false	26	26	hjnc5wxv75ckwvos9wsd0as31nmnice			false		false			false	true	'

ROW_1_FIELDS = ('enwiki',                               # wiki_db: str
                'revision',                             # event_entity: str
                'create',                               # event_type: str
                '2001-01-15 19:27:13.0',                # event_timestamp: str
                '*',                                    # event_comment: str
                11313587,                               # event_user_id: int
                'Office.bomis.com',                     # event_user_text_historical: str
                'Office.bomis.com',                     # event_user_text: str
                [],                                     # event_user_blocks_historical: list[str]
                [],                                     # event_user_blocks: list[str]
                [],                                     # event_user_groups_historical: list[str]
                [],                                     # event_user_groups: list[str]
                [],                                     # event_user_is_bot_by_historical: list[str]
                [],                                     # event_user_is_bot_by: list[str]
                False,                                  # event_user_is_created_by_self: bool
                False,                                  # event_user_is_created_by_system: bool
                True,                                   # event_user_is_created_by_peer: bool
                False,                                  # event_user_is_anonymous: bool
                '2009-12-28 09:06:07.0',                # event_user_registration_timestamp: str
                '2009-12-28 09:06:09.0',                # event_user_creation_timestamp: str
                '2001-01-15 19:27:13.0',                # event_user_first_edit_timestamp: str
                1,                                      # event_user_revision_count: int
                None,                                   # event_user_seconds_since_previous_revision: int
                26323569,                               # page_id: int
                'HomePage',                             # page_title_historical: str
                'HomePage',                             # page_title: str
                0,                                      # page_namespace_historical: int
                True,                                   # page_namespace_is_content_historical: bool
                0,                                      # page_namespace: int
                True,                                   # page_namespace_is_content: bool
                True,                                   # page_is_redirect: bool
                False,                                  # page_is_deleted: bool
                '2010-02-24 14:25:49.0',                # page_creation_timestamp: str
                '2001-01-15 19:27:13.0',                # page_first_edit_timestamp: str
                1,                                      # page_revision_count: int
                None,                                   # page_seconds_since_previous_revision: int
                None,                                   # user_id: int
                '',                                     # user_text_historical: str
                '',                                     # user_text: str
                [],                                     # user_blocks_historical: list[str]
                [],                                     # user_blocks: list[str]
                [],                                     # user_groups_historical: list[str]
                [],                                     # user_groups: list[str]
                [],                                     # user_is_bot_by_historical: list[str]
                [],                                     # user_is_bot_by: list[str]
                None,                                   # user_is_created_by_self: bool
                None,                                   # user_is_created_by_system: bool
                None,                                   # user_is_created_by_peer: bool
                None,                                   # user_is_anonymous: bool
                '',                                     # user_registration_timestamp: str
                '',                                     # user_creation_timestamp: str
                '',                                     # user_first_edit_timestamp: str
                908493298,                              # revision_id: int
                0,                                      # revision_parent_id: int
                False,                                  # revision_minor_edit: bool
                [],                                     # revision_deleted_parts: list[str]
                False,                                  # revision_deleted_parts_are_suppressed: bool
                26,                                     # revision_text_bytes: int
                26,                                     # revision_text_bytes_diff: int
                'hjnc5wxv75ckwvos9wsd0as31nmnice',      # revision_text_sha1: str
                '',                                     # revision_content_model: str
                '',                                     # revision_content_format: str
                False,                                  # revision_is_deleted_by_page_deletion: bool
                '',                                     # revision_deleted_by_page_deletion_timestamp: str
                False,                                  # revision_is_identity_reverted: bool
                None,                                   # revision_first_identity_reverting_revision_id: int
                None,                                   # revision_seconds_to_identity_revert: int
                False,                                  # revision_is_identity_revert: bool
                True,                                   # revision_is_from_before_page_creation: bool
                [])                                     # revision_tags: list[str]


ROW_2 = r'enwiki	revision	create	2023-01-31 21:32:07.0	/* top */[[WP:AWB/GF|General fixes]], replaced: | nationality    \t= [[Israel]]\n| → | nationality    \t= Israeli\n|	15996738	BattyBot	BattyBot			bot	bot	name,group	name,group	false	false	true	false	2011-12-30 00:06:00.0	2011-12-30 00:06:01.0	2011-12-30 00:11:47.0	1582672	13	62661703	Dmitry_Bukhman	Dmitry_Bukhman	0	true	0	true	false	false	2019-12-25 10:45:29.0	2019-12-25 10:45:29.0	64	3070613																	1136732310	1129829523	false		false	5750	-3	b8tfg7j7xkyxfhj7e5qluocr33clvyb			false		false			false	false	AWB'


ROW_3 = r'enwiki	user	alterblocks	2023-11-04 21:10:24.0																																	44332519	LoomCreek	LoomCreek			extendedconfirmed,ipblock-exempt	extendedconfirmed,ipblock-exempt			false	false	true	false	2022-08-11 16:36:04.0	2022-08-11 16:36:05.0	2022-08-12 23:26:29.0																		'


@pytest.mark.parametrize('input,expected', [
    ('', ''),
    ('foo bar', 'foo bar'),
    ('foo\\tbar', 'foo\tbar'),
    ('foo\\tbar\\nbaz\\r', 'foo\tbar\nbaz\r'),
    ('foo\\x', 'foo\\x'),
    ('foo,bar','foo,bar'),
    ('foo\\,bar', 'foo\\,bar'),
])
def test_unescape_tnr(input, expected):
    assert unescape_tnr(input) == expected


@pytest.mark.parametrize('input,expected', [
    ('', ''),
    ('foo bar', 'foo bar'),
    ('foo\\tbar', 'foo\tbar'),
    ('foo\\tbar\\nbaz\\r', 'foo\tbar\nbaz\r'),
    ('foo\\x', 'foo\\x'),
    ('foo,bar','foo,bar'),
    ('foo\\,bar', 'foo,bar'),
])
def test_unescape_tnr_comma(input, expected):
    assert unescape_tnr_comma(input) == expected


def test_build_row_constructs_row():
    row = build_row(ROW_1)
    assert isinstance(row, Row)
    assert row == Row(*ROW_1_FIELDS)


def test_build_row_converts_int_fields():
    row = build_row(ROW_1)
    assert row.event_user_id == 11313587


def test_build_row_unescapes_tabs_in_strings():
    row = build_row(ROW_2)
    assert row.event_comment == '/* top */[[WP:AWB/GF|General fixes]], replaced: | nationality    \t= [[Israel]]\n| → | nationality    \t= Israeli\n|'


def test_build_row_handles_string_arrays():
    row = build_row(ROW_3)
    assert row.user_groups == ['extendedconfirmed', 'ipblock-exempt']


def test_build_row_raises_value_error_on_malformed_boolean():
    with pytest.raises(ValueError):
        build_row(ROW_1.replace('true', 'xxx'))
