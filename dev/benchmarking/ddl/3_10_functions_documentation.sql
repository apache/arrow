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


-- _DOCUMENTATION_INGESTION
CREATE OR REPLACE FUNCTION public._documentation_ingestion()
RETURNS text AS
$$
  WITH ingestion_docs AS (
    SELECT
      proname || E'\n'
      || rpad('', character_length(proname), '-')
      || E'\n\n:code:`'
      || proname || '('
      || string_agg(a.argname || ' ' || typname , ', ')
      || E')`\n\n'
      || description
      || E'\n\n\nback to `Benchmark data model <benchmark-data-model>`_\n'
      AS docs
    FROM pg_catalog.pg_proc
    JOIN pg_catalog.pg_namespace
      ON nspname='public'
      AND pg_namespace.oid = pronamespace
      AND proname LIKE '%ingest%'
    JOIN pg_catalog.pg_description
      ON pg_description.objoid=pg_proc.oid,
    LATERAL unnest(proargnames, proargtypes) AS a(argname, argtype)
      JOIN pg_catalog.pg_type
        ON pg_type.oid = a.argtype
    GROUP BY proname, description
  )
  SELECT
    string_agg(docs, E'\n\n') AS docs
  FROM ingestion_docs;
$$
LANGUAGE sql STABLE;

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
    FROM public.summarized_tables_view AS t
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

  -- All of the hard-coded constants here are the string length of the table
  -- column headers: 'Column', 'Type', 'Nullable', 'Default', 'Description'
  SELECT greatest(6, max(character_length(column_name)))
  FROM public._documentation_view_details(view_name) INTO column_length;

  SELECT greatest(4, max(character_length(type_name)))
  FROM public._documentation_view_details(view_name) INTO type_length;

  SELECT greatest(8, max(character_length(nullable)))
  FROM public._documentation_view_details(view_name) INTO nullable_length;

  SELECT greatest(7, max(character_length(default_value)))
  FROM public._documentation_view_details(view_name) INTO default_length;

  SELECT greatest(11, max(character_length(description)))
  FROM public._documentation_view_details(view_name) INTO description_length;

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
  FROM public._documentation_view_details(view_name) AS v
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
    SELECT description FROM pg_catalog.pg_description
    WHERE pg_description.objoid = view_name::regclass
    INTO view_description;

    SELECT
      view_name || E'\n' || rpad('', length(view_name), '-') || E'\n\n' ||
      view_description || E'\n\n' ||
      string_agg(rst_formatted, E'\n')
    INTO view_table_markup
    FROM public._documentation_view_pieces(view_name);

    RETURN view_table_markup;
  END
$$
LANGUAGE plpgsql STABLE;
COMMENT ON FUNCTION public.documentation_for(citext)
IS E'Create an ".rst"-formatted table describing a specific view.\n'
  'Example: SELECT public.documentation_for(''endpoint'');';


-- DOCUMENTATION
CREATE OR REPLACE FUNCTION public.documentation(dotfile_name text)
RETURNS TABLE (full_text text) AS
$$
  WITH v AS (
      SELECT
        public.documentation_for(relname::citext)
        || E'\n\nback to `Benchmark data model <benchmark-data-model>`_\n'
        AS view_documentation
      FROM pg_catalog.pg_trigger
      JOIN pg_catalog.pg_class ON pg_trigger.tgrelid = pg_class.oid
      WHERE NOT tgisinternal
  )
  SELECT
    E'\n.. _benchmark-data-model:\n\n'
    'Benchmark data model\n'
    '====================\n\n\n'
    '.. graphviz:: '
    || dotfile_name
    || E'\n\n\n.. _benchmark-ingestion:\n\n'
      'Benchmark ingestion helper functions\n'
      '====================================\n\n'
    || public._documentation_ingestion()
    || E'\n\n\n.. _benchmark-views:\n\n'
      'Benchmark views\n'
      '===============\n\n\n'
    || string_agg(v.view_documentation, E'\n')
  FROM v
  GROUP BY True;
$$
LANGUAGE sql STABLE;
COMMENT ON FUNCTION public.documentation(text)
IS E'Create an ".rst"-formatted file that shows the columns in '
  'every insertable view in the "public" schema.\n'
  'The text argument is the name of the generated dotfile to be included.\n'
  'Example: SELECT public.documentation(''data_model.dot'');';


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
  FROM public.summarized_tables_view
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
  FROM public.summarized_tables_view
  WHERE table_name = tablename
    AND (description IS NULL OR description  not like '%key%')
  UNION ALL
  -- foreign keys
  SELECT
    '    <tr><td port="' || column_name || '">'
    || column_name
    || CASE WHEN description LIKE '%unique' THEN ' (u)' ELSE '' END
    || ' (fk) </td></tr>'
  FROM public.summarized_tables_view
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
  schemaname name := 'public';
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
      '  <tr><td align="left"><font point-size="16">Legend</font></td></tr>\n'
      '  <tr><td align="left">pk = primary key</td></tr>\n'
      '  <tr><td align="left">fk = foreign key</td></tr>\n'
      '  <tr><td align="left">u = unique*</td></tr>\n'
      '  <tr><td align="left">o = optional</td></tr>\n'
      '  <tr><td align="left">'
      '* multiple uniques in the same table are a unique group</td></tr>\n'
      '</table>>\n];'
    UNION ALL
    SELECT
      string_agg(
        public._documentation_dotfile_node_for(relname),
        E'\n'  -- Forcing the 'env' table to the end makes a better image
        ORDER BY (CASE WHEN relname LIKE 'env%' THEN 'z' ELSE relname END)
    )
    FROM pg_catalog.pg_class
      WHERE relkind='r' AND relnamespace = schemaname::regnamespace
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
COMMENT ON FUNCTION public.documentation_dotfile()
IS E'Create a Graphviz dotfile of the data model: '
  'every table in the "public" schema.\n'
  'Example: SELECT public.documentation_dotfile();';
