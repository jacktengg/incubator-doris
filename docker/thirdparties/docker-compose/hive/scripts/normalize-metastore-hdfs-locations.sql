-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

\set ON_ERROR_STOP on

BEGIN;

SELECT set_config('doris.old_hdfs_uri', :'old_uri', true);
SELECT set_config('doris.new_hdfs_uri', :'new_uri', true);

DO $$
BEGIN
    IF to_regclass('"CTLGS"') IS NOT NULL THEN
        UPDATE "CTLGS"
        SET "LOCATION_URI" = replace(
            "LOCATION_URI",
            current_setting('doris.old_hdfs_uri'),
            current_setting('doris.new_hdfs_uri'))
        WHERE "LOCATION_URI" LIKE current_setting('doris.old_hdfs_uri') || '/%';
    END IF;
END
$$;

UPDATE "DBS"
SET "DB_LOCATION_URI" = replace("DB_LOCATION_URI", :'old_uri', :'new_uri')
WHERE "DB_LOCATION_URI" LIKE :'old_uri' || '/%';

UPDATE "SDS"
SET "LOCATION" = replace("LOCATION", :'old_uri', :'new_uri')
WHERE "LOCATION" LIKE :'old_uri' || '/%';

UPDATE "SKEWED_COL_VALUE_LOC_MAP"
SET "LOCATION" = replace("LOCATION", :'old_uri', :'new_uri')
WHERE "LOCATION" LIKE :'old_uri' || '/%';

UPDATE "FUNC_RU"
SET "RESOURCE_URI" = replace("RESOURCE_URI", :'old_uri', :'new_uri')
WHERE "RESOURCE_URI" LIKE :'old_uri' || '/%';

COMMIT;
