-- Copyright 2026 The Cockroach Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- SPDX-License-Identifier: Apache-2.0

WITH
proposed(expires, name, nonce) AS (
SELECT $1::TIMESTAMP, unnest($2::STRING[]), $3::UUID
),
matches (name) AS (
SELECT name
  FROM %[1]s
  JOIN proposed USING (name, nonce)
   FOR UPDATE
)
UPDATE %[1]s AS l
   SET expires=$1::TIMESTAMP
  FROM matches m
 WHERE l.name = m.name
   AND (SELECT count(*) FROM proposed) = (SELECT count(*) FROM matches)
