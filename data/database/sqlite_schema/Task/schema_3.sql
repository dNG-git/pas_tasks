-- direct PAS
-- Python Application Services
--
-- (C) direct Netware Group - All rights reserved
-- https://www.direct-netware.de/redirect?pas;tasks
--
-- The following license agreement remains valid unless any additions or
-- changes are being made by direct Netware Group in a written form.
--
-- This program is free software; you can redistribute it and/or modify it
-- under the terms of the GNU General Public License as published by the
-- Free Software Foundation; either version 2 of the License, or (at your
-- option) any later version.
--
-- This program is distributed in the hope that it will be useful, but WITHOUT
-- ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
-- FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
-- more details.
--
-- You should have received a copy of the GNU General Public License along with
-- this program; if not, write to the Free Software Foundation, Inc.,
-- 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
--
-- https://www.direct-netware.de/redirect?licenses;gpl

-- Extend size of tid attribute to 240 characters

ALTER TABLE __db_prefix___task RENAME TO __db_prefix__tmp_task;

CREATE TABLE __db_prefix___task (
 id VARCHAR(32) NOT NULL,
 tid VARCHAR(240) DEFAULT '' NOT NULL,
 name VARCHAR(100) DEFAULT '' NOT NULL,
 status INTEGER DEFAULT '96' NOT NULL,
 hook VARCHAR(255) DEFAULT '' NOT NULL,
 params TEXT,
 time_started DATETIME NOT NULL,
 time_scheduled DATETIME NOT NULL,
 time_updated DATETIME NOT NULL,
 timeout DATETIME NOT NULL,
 PRIMARY KEY (id)
);

INSERT INTO __db_prefix___task SELECT * FROM __db_prefix__tmp_task;

DROP TABLE __db_prefix__tmp_task;
