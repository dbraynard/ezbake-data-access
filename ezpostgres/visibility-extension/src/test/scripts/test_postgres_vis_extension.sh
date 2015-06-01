#!/bin/bash

#   Copyright (C) 2013-2015 Computer Sciences Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "$0 [Postgres user] [Test DB name]" >&2
    exit 1
fi

POSTGRES_USER=${1-postgres}
TEST_DB_NAME=${2-vis_test}

createdb -U $POSTGRES_USER -e $TEST_DB_NAME

psql -U $POSTGRES_USER -d $TEST_DB_NAME <<EOF
CREATE EXTENSION IF NOT EXISTS ezbake_visibility;
CREATE TABLE test_table (id INT PRIMARY KEY, message VARCHAR (1024) NOT NULL, visibility VARCHAR (24000) NOT NULL);
INSERT INTO test_table VALUES (1, 'Hello, World!', 'AA==');
INSERT INTO test_table VALUES (2, 'A AND B', 'CwABAAAAA0EmQgA=');
INSERT INTO test_table VALUES (3, 'A OR B', 'CwABAAAAA0F8QgA=');
BEGIN;
-- A token with auths "A"
SET ezbake.token = 'DAABCwABAAAACkV6U2VjdXJpdHkLAAIAAAAMRGF0YXNldHNUZXN0CwADAAAADERhdGFzZXRzVGVzdAoABgAAAUvraRT6CwAHAAAAAAAIAAIAAAAADAADCwABAAAACURvZSwgSm9obgwAAgsAAQAAAApFelNlY3VyaXR5CwACAAAADERhdGFzZXRzVGVzdAsAAwAAAAxEYXRhc2V0c1Rlc3QKAAYAAAFL62kU+gsABwAAAAAAAAsACgAAAAFVDAALDgABCwAAAAEAAAABQQANAA0LDwAAAAEAAAAGRXpCYWtlCwAAAAEAAAAORXpCYWtlUGxhdGZvcm0LAA8AAAADVVNBCwAQAAAAA09SRwA=';
-- You should see rows 1 and 3
SELECT * FROM test_table;
COMMIT;
BEGIN;
-- A token with auths "A" and "B"
SET ezbake.token = 'DAABCwABAAAACkV6U2VjdXJpdHkLAAIAAAAMRGF0YXNldHNUZXN0CwADAAAADERhdGFzZXRzVGVzdAoABgAAAUvraor9CwAHAAAAAAAIAAIAAAAADAADCwABAAAACURvZSwgSm9obgwAAgsAAQAAAApFelNlY3VyaXR5CwACAAAADERhdGFzZXRzVGVzdAsAAwAAAAxEYXRhc2V0c1Rlc3QKAAYAAAFL62qK/QsABwAAAAAAAAsACgAAAAFVDAALDgABCwAAAAIAAAABQQAAAAFCAA0ADQsPAAAAAQAAAAZFekJha2ULAAAAAQAAAA5FekJha2VQbGF0Zm9ybQsADwAAAANVU0ELABAAAAADT1JHAA==';
-- You should see all three rows
SELECT * FROM test_table;
COMMIT;
EOF

dropdb -U $POSTGRES_USER -e $TEST_DB_NAME
