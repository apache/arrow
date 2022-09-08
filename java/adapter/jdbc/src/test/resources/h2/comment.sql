create table table1(
  id bigint auto_increment primary key,
  name varchar(255),
  column1 boolean,
  columnN int
  );

COMMENT ON TABLE table1 IS 'This is super special table with valuable data';
COMMENT ON COLUMN table1.id IS 'Record identifier';
COMMENT ON COLUMN table1.name IS 'Name of record';
COMMENT ON COLUMN table1.columnN IS 'Informative description of columnN';