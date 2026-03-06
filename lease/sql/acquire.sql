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
nonce (nonce) AS (
SELECT gen_random_uuid()
),
proposed (name, expires, nonce, holder) AS (
SELECT unnest($1::STRING[]), $2::TIMESTAMP, nonce, $3::STRING
  FROM nonce
),
blocking AS (
SELECT x.expires
  FROM %[1]s x
  JOIN proposed USING (name)
 WHERE x.expires > $4::TIMESTAMP
   FOR UPDATE
),
acquired AS (
UPSERT INTO %[1]s (name, expires, nonce, holder)
SELECT name, expires, nonce, holder
  FROM proposed
 WHERE NOT EXISTS (SELECT * FROM blocking)
RETURNING nonce
)
SELECT (SELECT max(expires) FROM blocking), (SELECT DISTINCT nonce FROM acquired)
