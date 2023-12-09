// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquet

import (
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/compress"
)

// Constants for default property values used for the default reader, writer and column props.
const (
	// Default Buffer size used for the Reader
	DefaultBufSize int64 = 4096 * 4
	// Default data page size limit is 1K it's not guaranteed, but we will try to
	// cut data pages off at this size where possible.
	DefaultDataPageSize int64 = 1024 * 1024
	// Default is for dictionary encoding to be turned on, use WithDictionaryDefault
	// writer property to change that.
	DefaultDictionaryEnabled = true
	// If the dictionary reaches the size of this limitation, the writer will use
	// the fallback encoding (usually plain) instead of continuing to build the
	// dictionary index.
	DefaultDictionaryPageSizeLimit = DefaultDataPageSize
	// In order to attempt to facilitate data page size limits for writing,
	// data is written in batches. Increasing the batch size may improve performance
	// but the larger the batch size, the easier it is to overshoot the datapage limit.
	DefaultWriteBatchSize int64 = 1024
	// Default maximum number of rows for a single row group
	DefaultMaxRowGroupLen int64 = 64 * 1024 * 1024
	// Default is to have stats enabled for all columns, use writer properties to
	// change the default, or to enable/disable for specific columns.
	DefaultStatsEnabled = true
	// If the stats are larger than 4K the writer will skip writing them out anyways.
	DefaultMaxStatsSize int64 = 4096
	DefaultCreatedBy          = "parquet-go version 14.0.2"
	DefaultRootName           = "schema"
)

// ColumnProperties defines the encoding, codec, and so on for a given column.
type ColumnProperties struct {
	Encoding          Encoding
	Codec             compress.Compression
	DictionaryEnabled bool
	StatsEnabled      bool
	MaxStatsSize      int64
	CompressionLevel  int
}

// DefaultColumnProperties returns the default properties which get utilized for writing.
//
// The default column properties are the following constants:
//
//	Encoding:						Encodings.Plain
//	Codec:							compress.Codecs.Uncompressed
//	DictionaryEnabled:	DefaultDictionaryEnabled
//	StatsEnabled:				DefaultStatsEnabled
//	MaxStatsSize:				DefaultMaxStatsSize
//	CompressionLevel:		compress.DefaultCompressionLevel
func DefaultColumnProperties() ColumnProperties {
	return ColumnProperties{
		Encoding:          Encodings.Plain,
		Codec:             compress.Codecs.Uncompressed,
		DictionaryEnabled: DefaultDictionaryEnabled,
		StatsEnabled:      DefaultStatsEnabled,
		MaxStatsSize:      DefaultMaxStatsSize,
		CompressionLevel:  compress.DefaultCompressionLevel,
	}
}

type writerPropConfig struct {
	wr            *WriterProperties
	encodings     map[string]Encoding
	codecs        map[string]compress.Compression
	compressLevel map[string]int
	dictEnabled   map[string]bool
	statsEnabled  map[string]bool
}

// WriterProperty is used as the options for building a writer properties instance
type WriterProperty func(*writerPropConfig)

// WithAllocator specifies the writer to use the given allocator
func WithAllocator(mem memory.Allocator) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.mem = mem
	}
}

// WithDictionaryDefault sets the default value for whether to enable dictionary encoding
func WithDictionaryDefault(dict bool) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.defColumnProps.DictionaryEnabled = dict
	}
}

// WithDictionaryFor allows enabling or disabling dictionary encoding for a given column path string
func WithDictionaryFor(path string, dict bool) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.dictEnabled[path] = dict
	}
}

// WithDictionaryPath is like WithDictionaryFor, but takes a ColumnPath type
func WithDictionaryPath(path ColumnPath, dict bool) WriterProperty {
	return WithDictionaryFor(path.String(), dict)
}

// WithDictionaryPageSizeLimit is the limit of the dictionary at which the writer
// will fallback to plain encoding instead
func WithDictionaryPageSizeLimit(limit int64) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.dictPagesize = limit
	}
}

// WithBatchSize specifies the number of rows to use for batch writes to columns
func WithBatchSize(batch int64) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.batchSize = batch
	}
}

// WithMaxRowGroupLength specifies the number of rows as the maximum number of rows for a given row group in the writer.
func WithMaxRowGroupLength(nrows int64) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.maxRowGroupLen = nrows
	}
}

// WithDataPageSize specifies the size to use for splitting data pages for column writing.
func WithDataPageSize(pgsize int64) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.pageSize = pgsize
	}
}

// WithDataPageVersion specifies whether to use Version 1 or Version 2 of the DataPage spec
func WithDataPageVersion(version DataPageVersion) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.dataPageVersion = version
	}
}

// WithVersion specifies which Parquet Spec version to utilize for writing.
func WithVersion(version Version) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.parquetVersion = version
	}
}

// WithCreatedBy specifies the "created by" string to use for the writer
func WithCreatedBy(createdby string) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.createdBy = createdby
	}
}

// WithRootName enables customization of the name used for the root schema node. This is required
// to maintain compatibility with other tools.
func WithRootName(name string) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.rootName = name
	}
}

// WithRootRepetition enables customization of the repetition used for the root schema node.
// This is required to maintain compatibility with other tools.
func WithRootRepetition(repetition Repetition) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.rootRepetition = repetition
	}
}

// WithEncoding defines the encoding that is used when we aren't using dictionary encoding.
//
// This is either applied if dictionary encoding is disabled, or if we fallback if the dictionary
// grew too large.
func WithEncoding(encoding Encoding) WriterProperty {
	return func(cfg *writerPropConfig) {
		if encoding == Encodings.PlainDict || encoding == Encodings.RLEDict {
			panic("parquet: can't use dictionary encoding as fallback encoding")
		}
		cfg.wr.defColumnProps.Encoding = encoding
	}
}

// WithEncodingFor is for defining the encoding only for a specific column path. This encoding will be used
// if dictionary encoding is disabled for the column or if we fallback because the dictionary grew too large
func WithEncodingFor(path string, encoding Encoding) WriterProperty {
	return func(cfg *writerPropConfig) {
		if encoding == Encodings.PlainDict || encoding == Encodings.RLEDict {
			panic("parquet: can't use dictionary encoding as fallback encoding")
		}
		cfg.encodings[path] = encoding
	}
}

// WithEncodingPath is the same as WithEncodingFor but takes a ColumnPath directly.
func WithEncodingPath(path ColumnPath, encoding Encoding) WriterProperty {
	return WithEncodingFor(path.String(), encoding)
}

// WithCompression specifies the default compression type to use for column writing.
func WithCompression(codec compress.Compression) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.defColumnProps.Codec = codec
	}
}

// WithCompressionFor specifies the compression type for the given column.
func WithCompressionFor(path string, codec compress.Compression) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.codecs[path] = codec
	}
}

// WithCompressionPath is the same as WithCompressionFor but takes a ColumnPath directly.
func WithCompressionPath(path ColumnPath, codec compress.Compression) WriterProperty {
	return WithCompressionFor(path.String(), codec)
}

// WithMaxStatsSize sets a maximum size for the statistics before we decide not to include them.
func WithMaxStatsSize(maxStatsSize int64) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.defColumnProps.MaxStatsSize = maxStatsSize
	}
}

// WithCompressionLevel specifies the default compression level for the compressor in every column.
//
// The provided compression level is compressor specific. The user would have to know what the available
// levels are for the selected compressor. If the compressor does not allow for selecting different
// compression levels, then this function will have no effect. Parquet and Arrow will not validate the
// passed compression level. If no level is selected by the user or if the special compress.DefaultCompressionLevel
// value is used, then parquet will select the compression level.
func WithCompressionLevel(level int) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.defColumnProps.CompressionLevel = level
	}
}

// WithCompressionLevelFor is like WithCompressionLevel but only for the given column path.
func WithCompressionLevelFor(path string, level int) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.compressLevel[path] = level
	}
}

// WithCompressionLevelPath is the same as WithCompressionLevelFor but takes a ColumnPath
func WithCompressionLevelPath(path ColumnPath, level int) WriterProperty {
	return WithCompressionLevelFor(path.String(), level)
}

// WithStats specifies a default for whether or not to enable column statistics.
func WithStats(enabled bool) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.defColumnProps.StatsEnabled = enabled
	}
}

// WithStatsFor specifies a per column value as to enable or disable statistics in the resulting file.
func WithStatsFor(path string, enabled bool) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.statsEnabled[path] = enabled
	}
}

// WithStatsPath is the same as WithStatsFor but takes a ColumnPath
func WithStatsPath(path ColumnPath, enabled bool) WriterProperty {
	return WithStatsFor(path.String(), enabled)
}

// WithEncryptionProperties specifies the file level encryption handling for writing the file.
func WithEncryptionProperties(props *FileEncryptionProperties) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.encryptionProps = props
	}
}

// WithStoreDecimalAsInteger specifies whether to try using an int32/int64 for storing
// decimal data rather than fixed len byte arrays if the precision is low enough.
func WithStoreDecimalAsInteger(enabled bool) WriterProperty {
	return func(cfg *writerPropConfig) {
		cfg.wr.storeDecimalAsInt = enabled
	}
}

// WriterProperties is the collection of properties to use for writing a parquet file. The values are
// read only once it has been constructed.
type WriterProperties struct {
	mem               memory.Allocator
	dictPagesize      int64
	batchSize         int64
	maxRowGroupLen    int64
	pageSize          int64
	parquetVersion    Version
	createdBy         string
	dataPageVersion   DataPageVersion
	rootName          string
	rootRepetition    Repetition
	storeDecimalAsInt bool

	defColumnProps  ColumnProperties
	columnProps     map[string]*ColumnProperties
	encryptionProps *FileEncryptionProperties
}

func defaultWriterProperties() *WriterProperties {
	return &WriterProperties{
		mem:             memory.DefaultAllocator,
		dictPagesize:    DefaultDictionaryPageSizeLimit,
		batchSize:       DefaultWriteBatchSize,
		maxRowGroupLen:  DefaultMaxRowGroupLen,
		pageSize:        DefaultDataPageSize,
		parquetVersion:  V2_LATEST,
		dataPageVersion: DataPageV1,
		createdBy:       DefaultCreatedBy,
		rootName:        DefaultRootName,
		rootRepetition:  Repetitions.Repeated,
		defColumnProps:  DefaultColumnProperties(),
	}
}

// NewWriterProperties takes a list of options for building the properties. If multiple options are used which conflict
// then the last option is the one which will take effect. If no WriterProperty options are provided, then the default
// properties will be utilized for writing.
//
// The Default properties use the following constants:
//
//	Allocator:					memory.DefaultAllocator
//	DictionaryPageSize: DefaultDictionaryPageSizeLimit
//	BatchSize:					DefaultWriteBatchSize
//	MaxRowGroupLength:	DefaultMaxRowGroupLen
//	PageSize:						DefaultDataPageSize
//	ParquetVersion:			V1
//	DataPageVersion:		DataPageV1
//	CreatedBy:					DefaultCreatedBy
func NewWriterProperties(opts ...WriterProperty) *WriterProperties {
	cfg := writerPropConfig{
		wr:            defaultWriterProperties(),
		encodings:     make(map[string]Encoding),
		codecs:        make(map[string]compress.Compression),
		compressLevel: make(map[string]int),
		dictEnabled:   make(map[string]bool),
		statsEnabled:  make(map[string]bool),
	}
	for _, o := range opts {
		o(&cfg)
	}

	cfg.wr.columnProps = make(map[string]*ColumnProperties)
	get := func(key string) *ColumnProperties {
		if p, ok := cfg.wr.columnProps[key]; ok {
			return p
		}
		cfg.wr.columnProps[key] = new(ColumnProperties)
		*cfg.wr.columnProps[key] = cfg.wr.defColumnProps
		return cfg.wr.columnProps[key]
	}

	for key, value := range cfg.encodings {
		get(key).Encoding = value
	}

	for key, value := range cfg.codecs {
		get(key).Codec = value
	}

	for key, value := range cfg.compressLevel {
		get(key).CompressionLevel = value
	}

	for key, value := range cfg.dictEnabled {
		get(key).DictionaryEnabled = value
	}

	for key, value := range cfg.statsEnabled {
		get(key).StatsEnabled = value
	}
	return cfg.wr
}

// FileEncryptionProperties returns the current encryption properties that were
// used to create the writer properties.
func (w *WriterProperties) FileEncryptionProperties() *FileEncryptionProperties {
	return w.encryptionProps
}

func (w *WriterProperties) Allocator() memory.Allocator      { return w.mem }
func (w *WriterProperties) CreatedBy() string                { return w.createdBy }
func (w *WriterProperties) RootName() string                 { return w.rootName }
func (w *WriterProperties) RootRepetition() Repetition       { return w.rootRepetition }
func (w *WriterProperties) WriteBatchSize() int64            { return w.batchSize }
func (w *WriterProperties) DataPageSize() int64              { return w.pageSize }
func (w *WriterProperties) DictionaryPageSizeLimit() int64   { return w.dictPagesize }
func (w *WriterProperties) Version() Version                 { return w.parquetVersion }
func (w *WriterProperties) DataPageVersion() DataPageVersion { return w.dataPageVersion }
func (w *WriterProperties) MaxRowGroupLength() int64         { return w.maxRowGroupLen }

// Compression returns the default compression type that will be used for any columns that don't
// have a specific compression defined.
func (w *WriterProperties) Compression() compress.Compression { return w.defColumnProps.Codec }

// CompressionFor will return the compression type that is specified for the given column path, or
// the default compression codec if there isn't one specific to this column.
func (w *WriterProperties) CompressionFor(path string) compress.Compression {
	if p, ok := w.columnProps[path]; ok {
		return p.Codec
	}
	return w.defColumnProps.Codec
}

// CompressionPath is the same as CompressionFor but takes a ColumnPath
func (w *WriterProperties) CompressionPath(path ColumnPath) compress.Compression {
	return w.CompressionFor(path.String())
}

// CompressionLevel returns the default compression level that will be used for any column
// that doesn't have a compression level specified for it.
func (w *WriterProperties) CompressionLevel() int { return w.defColumnProps.CompressionLevel }

// CompressionLevelFor returns the compression level that will be utilized for the given column,
// or the default compression level if the column doesn't have a specific level specified.
func (w *WriterProperties) CompressionLevelFor(path string) int {
	if p, ok := w.columnProps[path]; ok {
		return p.CompressionLevel
	}
	return w.defColumnProps.CompressionLevel
}

// CompressionLevelPath is the same as CompressionLevelFor but takes a ColumnPath object
func (w *WriterProperties) CompressionLevelPath(path ColumnPath) int {
	return w.CompressionLevelFor(path.String())
}

// Encoding returns the default encoding that will be utilized for any columns which don't have a different value
// specified.
func (w *WriterProperties) Encoding() Encoding { return w.defColumnProps.Encoding }

// EncodingFor returns the encoding that will be used for the given column path, or the default encoding if there
// isn't one specified for this column.
func (w *WriterProperties) EncodingFor(path string) Encoding {
	if p, ok := w.columnProps[path]; ok {
		return p.Encoding
	}
	return w.defColumnProps.Encoding
}

// EncodingPath is the same as EncodingFor but takes a ColumnPath object
func (w *WriterProperties) EncodingPath(path ColumnPath) Encoding {
	return w.EncodingFor(path.String())
}

// DictionaryIndexEncoding returns which encoding will be used for the Dictionary Index values based on the
// parquet version. V1 uses PlainDict and V2 uses RLEDict
func (w *WriterProperties) DictionaryIndexEncoding() Encoding {
	if w.parquetVersion == V1_0 {
		return Encodings.PlainDict
	}
	return Encodings.RLEDict
}

// DictionaryPageEncoding returns the encoding that will be utilized for the DictionaryPage itself based on the parquet
// version. V1 uses PlainDict, v2 uses Plain
func (w *WriterProperties) DictionaryPageEncoding() Encoding {
	if w.parquetVersion == V1_0 {
		return Encodings.PlainDict
	}
	return Encodings.Plain
}

// DictionaryEnabled returns the default value as for whether or not dictionary encoding will be utilized for columns
// that aren't separately specified.
func (w *WriterProperties) DictionaryEnabled() bool { return w.defColumnProps.DictionaryEnabled }

// DictionaryEnabledFor returns whether or not dictionary encoding will be used for the specified column when writing
// or the default value if the column was not separately specified.
func (w *WriterProperties) DictionaryEnabledFor(path string) bool {
	if p, ok := w.columnProps[path]; ok {
		return p.DictionaryEnabled
	}
	return w.defColumnProps.DictionaryEnabled
}

// DictionaryEnabledPath is the same as DictionaryEnabledFor but takes a ColumnPath object.
func (w *WriterProperties) DictionaryEnabledPath(path ColumnPath) bool {
	return w.DictionaryEnabledFor(path.String())
}

// StatisticsEnabled returns the default value for whether or not stats are enabled to be written for columns
// that aren't separately specified.
func (w *WriterProperties) StatisticsEnabled() bool { return w.defColumnProps.StatsEnabled }

// StatisticsEnabledFor returns whether stats will be written for the given column path, or the default value if
// it wasn't separately specified.
func (w *WriterProperties) StatisticsEnabledFor(path string) bool {
	if p, ok := w.columnProps[path]; ok {
		return p.StatsEnabled
	}
	return w.defColumnProps.StatsEnabled
}

// StatisticsEnabledPath is the same as StatisticsEnabledFor but takes a ColumnPath object.
func (w *WriterProperties) StatisticsEnabledPath(path ColumnPath) bool {
	return w.StatisticsEnabledFor(path.String())
}

// MaxStatsSize returns the default maximum size for stats
func (w *WriterProperties) MaxStatsSize() int64 { return w.defColumnProps.MaxStatsSize }

// MaxStatsSizeFor returns the maximum stat size for the given column path
func (w *WriterProperties) MaxStatsSizeFor(path string) int64 {
	if p, ok := w.columnProps[path]; ok {
		return p.MaxStatsSize
	}
	return w.defColumnProps.MaxStatsSize
}

// MaxStatsSizePath is the same as MaxStatsSizeFor but takes a ColumnPath
func (w *WriterProperties) MaxStatsSizePath(path ColumnPath) int64 {
	return w.MaxStatsSizeFor(path.String())
}

// ColumnEncryptionProperties returns the specific properties for encryption that will be used for the given column path
func (w *WriterProperties) ColumnEncryptionProperties(path string) *ColumnEncryptionProperties {
	if w.encryptionProps != nil {
		return w.encryptionProps.ColumnEncryptionProperties(path)
	}
	return nil
}

// StoreDecimalAsInteger returns the config option controlling whether or not
// to try storing decimal data as an integer type if the precision is low enough
// (1 <= prec <= 18 can be stored as an int), otherwise it will be stored as
// a fixed len byte array.
func (w *WriterProperties) StoreDecimalAsInteger() bool {
	return w.storeDecimalAsInt
}
