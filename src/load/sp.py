#!/usr/bin/env python3

from pathlib import Path
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, IntegerType, ArrayType
from pyspark.sql.functions import count, lit, desc

def main():
    DUMP_DIR = "/public/dumps/public/other/mediawiki_history"
    DATE = "2023-11"
    WIKI = "enwiki"
    mediawiki_history_path = Path(DUMP_DIR) / DATE / WIKI

    # Note: string unescaping and array conversion is done later
    mediawiki_history_schema = StructType([

        StructField("wiki_db", StringType(), nullable = False),
        StructField("event_entity", StringType(), nullable = False),
        StructField("event_type", StringType(), nullable = False),
        StructField("event_timestamp", StringType(), nullable = True),
        StructField("event_comment_escaped", StringType(), nullable = True),
        
        StructField("event_user_id", LongType(), nullable = True),
        StructField("event_user_text_historical_escaped", StringType(), nullable = True),
        StructField("event_user_text_escaped", StringType(), nullable = True),
        StructField("event_user_blocks_historical_string", StringType(), nullable = True),
        StructField("event_user_blocks_string", StringType(), nullable = True),
        StructField("event_user_groups_historical_string", StringType(), nullable = True),
        StructField("event_user_groups_string", StringType(), nullable = True),
        StructField("event_user_is_bot_by_historical_string", StringType(), nullable = True),
        StructField("event_user_is_bot_by_string", StringType(), nullable = True),
        StructField("event_user_is_created_by_self", BooleanType(), nullable = True),
        StructField("event_user_is_created_by_system", BooleanType(), nullable = True),
        StructField("event_user_is_created_by_peer", BooleanType(), nullable = True),
        StructField("event_user_is_anonymous", BooleanType(), nullable = True),
        StructField("event_user_registration_timestamp", StringType(), nullable = True),
        StructField("event_user_creation_timestamp", StringType(), nullable = True),
        StructField("event_user_first_edit_timestamp", StringType(), nullable = True),
        StructField("event_user_revision_count", LongType(), nullable = True),
        StructField("event_user_seconds_since_previous_revision", LongType(), nullable = True),
        
        StructField("page_id", LongType(), nullable = True),
        StructField("page_title_historical_escaped", StringType(), nullable = True),
        StructField("page_title_escaped", StringType(), nullable = True),
        StructField("page_namespace_historical", IntegerType(), nullable = True),
        StructField("page_namespace_is_content_historical", BooleanType(), nullable = True),
        StructField("page_namespace", IntegerType(), nullable = True),
        StructField("page_namespace_is_content", BooleanType(), nullable = True),
        StructField("page_is_redirect", BooleanType(), nullable = True),
        StructField("page_is_deleted", BooleanType(), nullable = True),
        StructField("page_creation_timestamp", StringType(), nullable = True),
        StructField("page_first_edit_timestamp", StringType(), nullable = True),
        StructField("page_revision_count", LongType(), nullable = True),
        StructField("page_seconds_since_previous_revision", LongType(), nullable = True),
        
        StructField("user_id", LongType(), nullable = True),
        StructField("user_text_historical_escaped",  StringType(), nullable = True),
        StructField("user_text_escaped", StringType(), nullable = True),
        StructField("user_blocks_historical_string", StringType(), nullable = True),
        StructField("user_blocks_string", StringType(), nullable = True),
        StructField("user_groups_historical_string", StringType(), nullable = True),
        StructField("user_groups_string", StringType(), nullable = True),
        StructField("user_is_bot_by_historical_string", StringType(), nullable = True),
        StructField("user_is_bot_by_string", StringType(), nullable = True),
        StructField("user_is_created_by_self", BooleanType(), nullable = True),
        StructField("user_is_created_by_system", BooleanType(), nullable = True),
        StructField("user_is_created_by_peer", BooleanType(), nullable = True),
        StructField("user_is_anonymous", BooleanType(), nullable = True),
        StructField("user_registration_timestamp", StringType(), nullable = True),
        StructField("user_creation_timestamp", StringType(), nullable = True),
        StructField("user_first_edit_timestamp", StringType(), nullable = True),
        
        StructField("revision_id", LongType(), nullable = True),
        StructField("revision_parent_id", LongType(), nullable = True),
        StructField("revision_minor_edit", BooleanType(), nullable = True),
        StructField("revision_deleted_parts_string", StringType(), nullable = True),
        StructField("revision_deleted_parts_are_suppressed", BooleanType(), nullable = True),
        StructField("revision_text_bytes", LongType(), nullable = True),
        StructField("revision_text_bytes_diff", LongType(), nullable = True),
        StructField("revision_text_sha1", StringType(), nullable = True),
        StructField("revision_content_model", StringType(), nullable = True),
        StructField("revision_content_format", StringType(), nullable = True),
        StructField("revision_is_deleted_by_page_deletion", BooleanType(), nullable = True),
        StructField("revision_deleted_by_page_deletion_timestamp", StringType(), nullable = True),
        StructField("revision_is_identity_reverted", BooleanType(), nullable = True),
        StructField("revision_first_identity_reverting_revision_id", LongType(), nullable = True),
        StructField("revision_seconds_to_identity_revert", LongType(), nullable = True),
        StructField("revision_is_identity_revert", BooleanType(), nullable = True),
        StructField("revision_is_from_before_page_creation", BooleanType(), nullable = True),
        StructField("revision_tags_string", StringType(), nullable = True)
    ])

    spark = SparkSession.builder.getOrCreate()

    spark.udf.register("unescape", unescape, StringType())
    spark.udf.register("to_array", toArray, ArrayType(StringType(), False))


    # Note: It's important to set .option("quote", "") to prevent spark to automaticallu use double-quotes to quote text
    mediawiki_history_raw = spark.read.option("delimiter", "\t").option("quote", "").schema(mediawiki_history_schema).csv(mediawiki_history_path)
    print(mediawiki_history_raw)

# Unescaping and array-splitting UDFs
def unescape(str):
    if (str is None):
        return None
    else:
        return str.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t")

# The comma splitter applies a negative lookahead for \ to prevent splitting escaped commas
def toArray(str):
    if (str is None):
        return []
    else:
        return [s.strip().replace("\\,", ",") for s in re.split("(?<!\\\\),", unescape(str))]


if __name__ == '__main__':
    main()
