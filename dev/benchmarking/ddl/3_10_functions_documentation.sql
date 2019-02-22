/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

-- SUMMARIZED_TABLES
CREATE VIEW public.summarized_tables AS
    WITH chosen AS (
          SELECT
            cls.oid AS id
            , cls.relname as tbl_name
          FROM pg_catalog.pg_class AS cls
          JOIN pg_catalog.pg_namespace AS ns ON cls.relnamespace = ns.oid
          WHERE
            cls.relkind = 'r'
            AND ns.nspname = 'public'
        ), all_constraints AS (
          SELECT
            chosen.id AS tbl_id
            , chosen.tbl_name
            , unnest(conkey) AS col_id
            , 'foreign key' AS col_constraint
            FROM pg_catalog.pg_constraint
            JOIN chosen ON chosen.id = conrelid
            WHERE contype = 'f'

          UNION

          SELECT
            chosen.id
            , chosen.tbl_name
            , unnest(indkey)
            , 'unique'
            FROM pg_catalog.pg_index i
            JOIN chosen ON chosen.id = i.indrelid
            WHERE i.indisunique

          UNION

          SELECT
            chosen.id
            , chosen.tbl_name
            , unnest(indkey)
            , 'primary key'
            FROM pg_catalog.pg_index i
            JOIN chosen on chosen.id = i.indrelid
            WHERE i.indisprimary
        ), gathered_constraints AS (
          SELECT
            tbl_id
            , tbl_name
            , col_id
            , string_agg(col_constraint, ', ' ORDER BY col_constraint)
              AS  col_constraint
          FROM all_constraints
          GROUP BY tbl_id, tbl_name, col_id
    )
    SELECT
      chosen.tbl_name AS table_name
      , columns.attnum AS column_number
      , columns.attname AS column_name
      , typ.typname AS type_name
      , CASE
          WHEN columns.attnotnull
            THEN 'not null'
          ELSE ''
        END AS nullable
      , CASE
          WHEN defaults.adsrc like 'nextval%'
            THEN 'serial'
          ELSE defaults.adsrc
        END AS default_value
      , CASE
          WHEN gc.col_constraint = '' OR gc.col_constraint IS NULL
            THEN cnstrnt.consrc
          WHEN cnstrnt.consrc IS NULL
            THEN gc.col_constraint
          ELSE gc.col_constraint || ', ' || cnstrnt.consrc
        END AS description
    FROM pg_catalog.pg_attribute AS columns
      JOIN chosen ON columns.attrelid = chosen.id
      JOIN pg_catalog.pg_type AS typ
        ON typ.oid = columns.atttypid
      LEFT JOIN gathered_constraints AS gc
        ON gc.col_id = columns.attnum
        AND gc.tbl_id = columns.attrelid
      LEFT JOIN pg_attrdef AS defaults
        ON defaults.adrelid = chosen.id
        AND defaults.adnum = columns.attnum
      LEFT JOIN pg_catalog.pg_constraint AS cnstrnt
        ON cnstrnt.conrelid = columns.attrelid
        AND columns.attrelid = ANY(cnstrnt.conkey)
    WHERE
      columns.attnum > 0
    ORDER BY table_name, column_number;


-- _DOCUMENTATION_VIEW_DETAILS
CREATE OR REPLACE FUNCTION public._documentation_view_details(view_name citext)
RETURNS TABLE(
    column_name name
    , type_name name
    , nullable text
    , default_value text
    , description text
) AS
$$
    WITH view_columns AS (
      SELECT
        attname AS column_name
        , attnum AS column_order
      FROM pg_catalog.pg_attribute
      WHERE attrelid=view_name::regclass
    )
    SELECT
      t.column_name
      , type_name
      , coalesce(nullable, '')
      , coalesce(default_value, '')
      , coalesce(description, '')
    FROM public.summarized_tables AS t
    JOIN view_columns AS v ON v.column_name = t.column_name
    WHERE t.table_name || '_view' = view_name OR t.column_name NOT LIKE '%_id'
    ORDER BY column_order;
$$
LANGUAGE sql STABLE;


-- _DOCUMENTATION_VIEW_PIECES
CREATE OR REPLACE FUNCTION public._documentation_view_pieces(view_name citext)
RETURNS TABLE (rst_formatted text)
AS
$$
DECLARE
  column_length integer;
  type_length integer;
  nullable_length integer;
  default_length integer;
  description_length integer;
  sep text;
  border text;
BEGIN

  SELECT greatest(6, max(character_length(v.column_name)))
  FROM _documentation_view_details(view_name) AS v INTO column_length;

  SELECT greatest(4, max(character_length(v.type_name)))
  FROM _documentation_view_details(view_name) AS v INTO type_length;

  SELECT greatest(8, max(character_length(v.nullable)))
  FROM _documentation_view_details(view_name) AS v INTO nullable_length;

  SELECT greatest(7, max(character_length(v.default_value)))
  FROM _documentation_view_details(view_name) AS v INTO default_length;

  SELECT greatest(11, max(character_length(v.description)))
  FROM _documentation_view_details(view_name) AS v INTO description_length;

  SELECT '  ' INTO sep;

  SELECT
      concat_ws(sep
        , rpad('', column_length, '=')
        , rpad('', type_length, '=')
        , rpad('', nullable_length, '=')
        , rpad('', default_length, '=')
        , rpad('', description_length, '=')
      )
    INTO border;

  --WITH entries AS (
  RETURN QUERY
  SELECT
    border
  UNION ALL
  SELECT
      concat_ws(sep
        , rpad('Column', column_length, ' ')
        , rpad('Type', type_length, ' ')
        , rpad('Nullable', nullable_length, ' ')
        , rpad('Default', default_length, ' ')
        , rpad('Description', description_length, ' ')
      )
  UNION ALL
  SELECT border
  UNION ALL
  SELECT
    concat_ws(sep
      , rpad(v.column_name, column_length, ' ')
      , rpad(v.type_name, type_length, ' ')
      , rpad(v.nullable, nullable_length, ' ')
      , rpad(v.default_value, default_length, ' ')
      , rpad(v.description, description_length, ' ')
    )
  FROM _documentation_view_details(view_name) AS v
  UNION ALL
  SELECT border;

END
$$
LANGUAGE plpgsql STABLE;


-- DOCUMENTATION_FOR
CREATE OR REPLACE FUNCTION public.documentation_for(view_name citext)
RETURNS text AS
$$
  DECLARE
    view_description text;
    view_table_markup text;
  BEGIN
    SELECT description FROM pg_description
    WHERE pg_description.objoid=view_name::regclass
    INTO view_description;

    SELECT
      view_name || rpad(E'\n', length(view_name), '-') || E'\n\n' ||
      view_description || E'\n\n' ||
      string_agg(rst_formatted, E'\n')
    INTO view_table_markup
    FROM _documentation_view_pieces(view_name);

    RETURN view_table_markup;
  END
$$
LANGUAGE plpgsql STABLE;

-- DOCUMENTATION
CREATE OR REPLACE FUNCTION public.documentation(dotfile_name text)
RETURNS TABLE (full_text text) AS
$$
  WITH v AS (
      SELECT
        relname::citext AS view_name
      FROM pg_trigger
      JOIN pg_class ON pg_trigger.tgrelid=pg_class.oid
      WHERE NOT tgisinternal
  )
  SELECT
    E'\n\n.. _data-model:\n\nData model\n==========\n\n\n.. graphviz:: '
    || dotfile_name
    || E'\n\n\n'
    || documentation_for(v.view_name)
    || E'\n\n\nback to `Data model <data-model>`_\n'
  FROM v;
$$
LANGUAGE sql STABLE;


-- _DOCUMENTATION_DOTFILE_NODE_FOR
CREATE OR REPLACE FUNCTION public._documentation_dotfile_node_for(tablename name)
RETURNS text AS
$$
DECLARE
  result text;
BEGIN
  WITH node AS (
  SELECT
    tablename::text AS lines
  UNION ALL
  SELECT
    E'[label = \n'
    '  <<table border="0" cellborder="1" cellspacing="0" cellpadding="2">'
  UNION ALL
  -- table name
  SELECT
    '    <tr><td border="0"><font point-size="14">'
    || tablename
    || '</font></td></tr>'
  UNION ALL
  -- primary keys
  SELECT
    '    <tr><td port="' || column_name || '"><b>'
    || column_name
    || ' (pk)</b></td></tr>'
  FROM public.summarized_tables
  WHERE table_name = tablename
    AND description LIKE '%primary key%'
  UNION ALL
  -- columns
  SELECT
    '    <tr><td>'
    || column_name
    || CASE WHEN description LIKE '%unique' THEN ' (u)' ELSE '' END
    || CASE WHEN nullable <> 'not null' THEN ' (o)' ELSE '' END
    || '</td></tr>'
  FROM public.summarized_tables
  WHERE table_name = tablename
    AND (description IS NULL OR description  not like '%key%')
  UNION ALL
  -- foreign keys
  SELECT
    '    <tr><td port="' || column_name || '">'
    || column_name
    || CASE WHEN description LIKE '%unique' THEN ' (u)' ELSE '' END
    || ' (fk) </td></tr>'
  FROM public.summarized_tables
  WHERE table_name = tablename
    AND description LIKE '%foreign key%'
    AND description NOT LIKE '%primary key%'
  UNION ALL
  SELECT
    E'  </table>>\n];'
  )
  SELECT
    string_agg(lines, E'\n')
  INTO result
  FROM node;

  RETURN result;
END
$$
LANGUAGE plpgsql STABLE;


-- _DOCUMENTATION_DOTFILE_EDGES
CREATE OR REPLACE FUNCTION public._documentation_dotfile_edges()
RETURNS text AS
$$
DECLARE
  result text;
BEGIN
  WITH relationship AS (
  SELECT
    conrelid AS fk_table_id
    , confrelid AS pk_table_id
    , unnest(conkey) AS fk_colnum
    , unnest(confkey) AS pk_colnum
    FROM pg_catalog.pg_constraint
    WHERE confkey IS NOT NULL
    AND connamespace='public'::regnamespace
  ), all_edges AS (
  SELECT
    fk_tbl.relname || ':' || fk_col.attname
    || ' -> '
    || pk_tbl.relname || ':' || pk_col.attname
    || ';' AS lines
  FROM relationship
  -- foreign key table + column
  JOIN pg_catalog.pg_attribute AS fk_col
    ON fk_col.attrelid = relationship.fk_table_id
    AND fk_col.attnum = relationship.fk_colnum
  JOIN pg_catalog.pg_class AS fk_tbl
    ON fk_tbl.oid = relationship.fk_table_id
  -- primary key table + column
  JOIN pg_catalog.pg_attribute AS pk_col
    ON pk_col.attrelid = relationship.pk_table_id
    AND pk_col.attnum = relationship.pk_colnum
  JOIN pg_catalog.pg_class AS pk_tbl
    ON pk_tbl.oid = relationship.pk_table_id
  )
  SELECT
    string_agg(lines, E'\n')
    INTO result
    FROM all_edges;

  RETURN result;
END
$$
LANGUAGE plpgsql STABLE;


-- DOCUMENTATION_DOTFILE
CREATE OR REPLACE FUNCTION public.documentation_dotfile()
RETURNS text AS
$$
DECLARE
  result text;
BEGIN
  WITH file_contents AS (
    SELECT
      E'digraph database {\n  concentrate = true;\n'
      '  rankdir = LR;\n'
      '  ratio = ".75";\n'
      '  node [shape = none, fontsize="11", fontname="Helvetica"];\n'
      '  edge [fontsize="8", fontname="Helvetica"];'
      AS lines
    UNION ALL
    SELECT
      E'legend\n[fontsize = "14"\nlabel =\n'
      '<<table border="0" cellpadding="0">\n'
      '  <tr><td><font point-size="16">Legend</font></td></tr>\n'
      '  <tr><td>pk = primary key</td></tr>\n'
      '  <tr><td>fk = foreign key</td></tr>\n'
      '  <tr><td>u = unique</td></tr>\n'
      '  <tr><td>o = optional</td></tr>\n'
      '</table>>\n];'
    UNION ALL
    SELECT
      public._documentation_dotfile_node_for(relname)
    FROM pg_class WHERE relkind='r' AND relnamespace = 'public'::regnamespace
    UNION ALL
    SELECT
      public._documentation_dotfile_edges()
    UNION ALL
    SELECT
      '}'
  )
  SELECT
    string_agg(lines, E'\n') AS dotfile
    INTO result
  FROM file_contents;
  RETURN result;
END
$$
LANGUAGE plpgsql STABLE;
