package serialize

import "github.com/apache/arrow/dev/flight-integration/protocol/message/org/apache/arrow/flatbuf"

var (
	SchemaRef = struct {
		Catalogs                 []Field
		DBSchemas                []Field
		Tables                   []Field
		TablesWithIncludedSchema []Field
		TableTypes               []Field
		PrimaryKeys              []Field
		ImportedKeys             []Field
		ExportedKeys             []Field
		CrossReference           []Field
		SqlInfo                  []Field
		XdbcTypeInfo             []Field
	}{
		Catalogs: []Field{
			{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
		},
		DBSchemas: []Field{
			{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
		},
		Tables: []Field{
			{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "table_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "table_type", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
		},
		TablesWithIncludedSchema: []Field{
			{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "table_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "table_type", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "table_schema", Type: flatbuf.TypeBinary, GetTypeTable: BinaryTypeTable(), Nullable: false},
		},
		TableTypes: []Field{
			{Name: "table_type", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
		},
		PrimaryKeys: []Field{
			{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "table_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "column_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: false},
			{Name: "key_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
		},
		ImportedKeys: []Field{
			{Name: "pk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "pk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "pk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "pk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "fk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "fk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "fk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "fk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: false},
			{Name: "fk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "pk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "update_rule", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(8, false), Nullable: false},
			{Name: "delete_rule", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(8, false), Nullable: false},
		},
		ExportedKeys: []Field{
			{Name: "pk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "pk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "pk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "pk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "fk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "fk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "fk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "fk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: false},
			{Name: "fk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "pk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "update_rule", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(8, false), Nullable: false},
			{Name: "delete_rule", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(8, false), Nullable: false},
		},
		CrossReference: []Field{
			{Name: "pk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "pk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "pk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "pk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "fk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "fk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "fk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "fk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: false},
			{Name: "fk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "pk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "update_rule", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(8, false), Nullable: false},
			{Name: "delete_rule", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(8, false), Nullable: false},
		},
		SqlInfo: []Field{
			{Name: "info_name", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, false), Nullable: false},
			{
				Name: "value",
				Type: flatbuf.TypeUnion,
				GetTypeTable: UnionTypeTable(
					flatbuf.UnionModeDense,
					[]Field{
						{Name: "string_value", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable()},
						{Name: "bool_value", Type: flatbuf.TypeBool, GetTypeTable: BoolTypeTable()},
						{Name: "bigint_value", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(64, true)},
						{Name: "int32_bitmask", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true)},
						{
							Name:         "string_list",
							Type:         flatbuf.TypeList,
							GetTypeTable: ListTypeTable(Field{Name: "item", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true})},
						{
							Name: "int32_to_int32_list_map",
							Type: flatbuf.TypeMap,
							GetTypeTable: MapTypeTable(
								Field{Name: "key", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true)},
								Field{
									Name:         "val",
									Type:         flatbuf.TypeList,
									GetTypeTable: ListTypeTable(Field{Name: "item", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: true}),
									Nullable:     true, // TODO: nullable?
								},
							),
						},
					},
				), Nullable: false},
		},
		XdbcTypeInfo: []Field{
			{Name: "type_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: false},
			{Name: "data_type", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: false},
			{Name: "column_size", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: true},
			{Name: "literal_prefix", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "literal_suffix", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{
				Name:         "create_params",
				Type:         flatbuf.TypeList,
				GetTypeTable: ListTypeTable(Field{Name: "item", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable()}),
				Nullable:     true,
			},
			{Name: "nullable", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: false},
			{Name: "case_sensitive", Type: flatbuf.TypeBool, GetTypeTable: BoolTypeTable(), Nullable: false},
			{Name: "searchable", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: false},
			{Name: "unsigned_attribute", Type: flatbuf.TypeBool, GetTypeTable: BoolTypeTable(), Nullable: true},
			{Name: "fixed_prec_scale", Type: flatbuf.TypeBool, GetTypeTable: BoolTypeTable(), Nullable: false},
			{Name: "auto_increment", Type: flatbuf.TypeBool, GetTypeTable: BoolTypeTable(), Nullable: true},
			{Name: "local_type_name", Type: flatbuf.TypeUtf8, GetTypeTable: Utf8TypeTable(), Nullable: true},
			{Name: "minimum_scale", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: true},
			{Name: "maximum_scale", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: true},
			{Name: "sql_data_type", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: false},
			{Name: "datetime_subcode", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: true},
			{Name: "num_prec_radix", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: true},
			{Name: "interval_precision", Type: flatbuf.TypeInt, GetTypeTable: IntTypeTable(32, true), Nullable: true},
		},
	}

	SchemaExample = struct {
		Query []Field
	}{
		Query: []Field{
			{
				Name:         "id",
				Type:         flatbuf.TypeInt,
				GetTypeTable: IntTypeTable(64, true),
				Nullable:     true,
				Metadata: map[string]string{
					"ARROW:FLIGHT:SQL:TABLE_NAME":        "test",
					"ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT": "1",
					"ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE": "0",
					"ARROW:FLIGHT:SQL:TYPE_NAME":         "type_test",
					"ARROW:FLIGHT:SQL:SCHEMA_NAME":       "schema_test",
					"ARROW:FLIGHT:SQL:IS_SEARCHABLE":     "1",
					"ARROW:FLIGHT:SQL:CATALOG_NAME":      "catalog_test",
					"ARROW:FLIGHT:SQL:PRECISION":         "100",
				},
			},
		},
	}
)
