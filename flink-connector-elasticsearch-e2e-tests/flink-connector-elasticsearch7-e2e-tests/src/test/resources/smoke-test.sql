-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE esTable ("
    a BIGINT NOT NULL,
    b TIME
    c STRING NOT NULL,
    d FLOAT,
    e TINYINT NOT NULL,
    f DATE,
    g TIMESTAMP NOT NULL,
    h as a + 2,
    PRIMARY KEY (a, g) NOT ENFORCED
    ) WITH (
     'connector' = '$CONNECTOR_NAME',
     'index' = '$INDEX',
     'hosts' = '$HOSTS'
    )

INSERT INTO esTable VALUES ('1', '00:00:12', '12.12d', '2', '2003-10-20', '2012-12-12 12:12:12');
