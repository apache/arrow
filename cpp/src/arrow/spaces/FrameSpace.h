#ifndef EXPERIMENTS_FRAMESPACE_H
#define EXPERIMENTS_FRAMESPACE_H

#include <arrow/table.h>


namespace arrow::spaces {

    /* Arrow DataFrame Underlying structure
     *
     * Goals
     *
     * The intention of this headers is to address an arrow based DataFrame implementation in a way that permits:
     *  - Addressing data beyond the process memory capacity, by allowing transparent access to on-storage, remote, lazy
     *  - Thread safe read-write access to DataFrames
     *  - Preserve “direct memory-like” data access performance
     *
     * Column Space concept
     *
     * A column space of type T is analogous to virtual memory space (https://en.wikipedia.org/wiki/Virtual_memory)
     * with the main distinction that rather than addressing bytes, it is addressing homogenous arrow array elements of type T.
     * Similar to a virtual memory, an address in the ColumnSpace may be:
     *  - unallocated
     *  - mapped to an actual arrow::Array
     *  - fetched as an arrow::Array by lazily invoking an external data provider (such as a file reader or lazily evaluating function)
     *
     *  ReadOnly Property
     *
     *  As a fundamental property, a column space is read only: data available at a given address is guaranteed to not
     *  change for that zone’s lifetime.
     *
     *  New data ranges may be added to the space, either by explicitly associating a new address with an immutable
     *  array or by adding a data provider, but once that data or data provider was associated with an Column Space “pointer”,
     *  repeat access would yield the same result for that pointer’s lifetime.
     *
     *  Note: Column Space is a key concept behind this design proposal, but it doesn’t have an actual class representation
     *  as its functionality is encapsulated by the DataFrameSpace class
     *
     *  Key Constructs defined in this document are:
     *
     *  - DataFrameSpace/SpaceAllocator/DataProvider
     *  - Index
     *  - DataFrame/ImmutableDataFrame/MutableDataFrame
     *  - IndexMutationSession
     *
     *  Recommended ways for reading this header are either:
     *  - Bottom up: starting with DataFrameOperationExamples::puttingItAllTogether
     *  - Top Down: looking at the DataFrameSpace/Index/DataFrame/IndexMutationSession interfaces and the way they connect
     */

    //A mutable table - can very well be the current arrow::Table as it stands
    using MutableTable = arrow::Table;

    //A placeholder type for a Table that cannot be modified by the user
    class ImmutableTable {
    public:
        //An immutable table can be created from an immutable table, via the move constructor -
        //this allows for a shallow copy since the move semantics guarantees data can no longer be altered via the
        //MutableTable
        ImmutableTable(MutableTable &&mutableTable) {}
    };

    /**
     * A proposed data structure that allows for a universal TableReference:
     * The main point is that it provides a unified ways of referring to table data while ensuring a table has only
     * one owner when in mutable state.
     */
    class TableReference {
        std::variant<std::unique_ptr<MutableTable>, std::shared_ptr<ImmutableTable>> data;

    public:
        explicit TableReference(std::unique_ptr<MutableTable> &&src) {
            data = std::move(src);
        }

        TableReference(const TableReference &src) {
            if (data.index() == 0) {
                throw std::logic_error("Cannot copy a mutable table reference - use move semantics");
            } else {
                data = {std::get<1>(data)};
            }
        }

        TableReference(TableReference &&src) = default;

        void makeConst() {
            if (data.index() == 0) {
                data = std::make_shared<ImmutableTable>(std::move(*std::get<0>(data)));
            }
        }
    };

    //A SpacePointer is an address in the DataFrameSpace and consequently an address in each and every subordinated
    //column spaces - effectively SpacePointer identifies a row in the DataFrameSpace
    using SpacePointer = size_t;
    //Identifies the length of row of ranges, typically starting from a SpacePointer
    using RangeLength = uint32_t;

    /**
     * Describes the rows that need to be copied onto a new block as the result of IndexMutationSession defragmentation
     */
    struct TranslationUnit {
        //Pointer to the new block (where previous blocks need to be copied to)
        const SpacePointer targetPointer;
        /**
         * Contains the original locations of row ranges to be copied onto the new block
         */
        std::vector<std::pair<SpacePointer, RangeLength>> sourceRows_;
    };

    /**
     * Contains one entry for each new block allocated by an IndexMutationSession, indicating data that needs storing at
     * that location.
     */
    struct TranslationLog {
        std::vector<TranslationUnit> blockTranslation;
    };


    /**
     *  A DataFrameSpace is an ordered set of parallel Column Spaces (see Column Space story above):
     *  By parallel we imply that a Column Space address is either valid for each Column Source Space or by none of them.
     *
     *  The DataFrameSpace is split in between "Allocated Space" composed of in-memory available arrow arrays/tables and
     *  "Virtual Space" where requests are delegated to external DataProvider objects.
     */
    class DataFrameSpace {
    public:


        /**
         * An allocator used to generate new SpacePointers
         * The allocator doesn't do any actual data mapping or allocation, it simply keeps track of assigned and released pointers.
         *
         * The current proposal envisions simple, fixed size allocation model (in the number of elements not the actual
         * memory size).
         * This is not a fundamental requirement for this proposal, but I generally expect a fixed block
         * size, big enough to amortize non-linear memory accesses and small enough to avoid internal fragmentation, would make
         * underlying implementation more efficient without harming the usability (as large tables can be split over
         * multiple blocks)
         */
        class SpaceManager {
        public:
            //The preset block allocation size
            const RangeLength blockSize = 1024;

            /**
             * "Allocates" a block of "blockSize" and notifies listeners (typically the DataFrameSpace object) of the new
             * pointer creation.
             * No data is actually being allocated, it just ensures that it returns a SpacePointer mapping to a block
             * not currently allocated
             */
            SpacePointer reserveBlock();

            /**
             * Releases a previously reserved block and notifies listeners (typically the DataFrameSpace object) of the
             * block being released.
             * @param spacePointer
             */
            void freeBlock(SpacePointer spacePointer);

            /**
             * Registers listeners (typically a DataFrameSpace objects) to be notified on respective block creation and
             * destruction
             * @param onNewBlock
             * @param onBlockRelease
             */
            void registerListener(std::function<void(SpacePointer)> onNewBlock,
                                  std::function<void(SpacePointer)> onBlockRelease);
        };

        //The space manager responsible of mapping the AllocatedSpace
        SpaceManager spaceManager;

        //The schema associated with this DataFrameSpace
        std::shared_ptr<arrow::Schema> schema_;

        /**
         * A DataProvider is an abstract interface for any external source of arrow tables.
         * The DataProvider guarantees the data values remained the same across all subsequent visit function calls
         *
         * One or more DataProvider objects are mapped by DataFrameSpace into the Virtual Space.
         *
         * Effectively all read requests mapping to a DataProvider are delegated to that provider
         */
        class DataProvider {

            /**
             * Provides access to data for a given range
             * @param visitor - Visitor function used to read the provided data - the function may be invoked one or
             *                  multiple times, delivering requested data in consecutive chunks.
             * @param providerOffset The beginning of the data range, in the DataProvider space (i.e. this offset is
             *                          fully agnostic of corresponding SpacePointer
             * @param rowsCount The numbers of rows to process
             * @param columns The subset of columns to be passed to the visitor function
             */
            virtual void visit(std::function<void(const ImmutableTable &, size_t, uint32_t)> visitor,
                               size_t providerOffset, RangeLength rowsCount,
                               const std::vector<std::string> &columns) const = 0;
        };

    public:

        DataFrameSpace(std::shared_ptr<arrow::Schema> schema) : schema_(schema) {};

        /**
         * Registers as the de-facto container of the block reserved at targetPointer
         *
         * This method is used primarily as part of the defragmentation process to manage new blocks allocated to
         * compact sparse rows.
         *
         * @param targetPointer A pointer previously allocated using SpaceManager::reserveBlock()
         * @param reference An arrow table that would be made immutable and used as the data source of that block until
         * the pointer is released. The table size is expected to be less than or equal to spaceManager.blockSize()
         */
        void registerData(SpacePointer targetPointer, TableReference &&reference);

        /**
         * Registers as the de-facto container of the block reserved at targetPointer
         *
         * Unlike registerData where input table sizes are capped to allocation blockSize, the input table here can be of
         * any size allowing for light weight mapping of an existing arrow table as a DataFrame
         * @param reference An arrow table that would be made immutable and used as the data source of that block until
         * the pointer is released.
         * @return a pointer to the newly mapped data range.
         */
        SpacePointer registerExternalData(TableReference &&reference);

        /**
         * Registers as the de-facto container of the block reserved at targetPointer
         *
         * @param targetPointer A pointer previously allocated using SpaceManager::reserveBlock()
         * @param reference an pointer to an immutable arrow table that would be used as the data source of that block until
         * the pointer is released. The table size is expected to be less than or equal to spaceManager.blockSize()
         */
        void registerData(SpacePointer targetPointer, const std::shared_ptr<ImmutableTable> &reference);

        /**
         * Registers a DataProvider and associates it to a new SpacePointer that would effectively map the corresponding
         * provider data to a virtual space starting a SpacePointer and spanning rowCount rows
         *
         * @param provider - The provider serving virtual data
         * @param rowsCount - The number of rows offered by the provider
         * @return A new SpacePointer mapping the provider data
         */
        SpacePointer registerDataProvider(const DataProvider &provider, size_t rowsCount);

        /**
         * Main accessor method from reading a contiguous data range
         * @param visitor Visitor function used to read the provided data - the function may be invoked one or
         *                multiple times, delivering requested data in consecutive chunks.
         * @param spaceOffset The starting pointer for the requested region
         * @param rowsCount The number of contiguous rows requested
         * @param columns The subset of columns to be read
         */
        void visit(std::function<void(const ImmutableTable &, size_t, uint32_t)> visitor,
                   SpacePointer spaceOffset, RangeLength rowsCount, const std::vector<std::string> &columns) const;

        /**
         * Creates a new table of  SpaceManager::blockSize rows and this->schema_ schema
         *
         * @return  a new table of  SpaceManager::blockSize rows and this->schema_ schema
         */
        std::unique_ptr<MutableTable> allocateNewTableBlock();

        /**
         * Applies the translation log produces by an IndexMutationSession:
         * As part of composing a new index out of other indices mapping the current DataFrameSpace rows, the index
         * creation algorithm may chose to copy sparse rows onto new contiguous regions in order to limit fragmentation.
         * This copy operations are recorder in the translationLog, and effectively executed after the index was created
         * but before the index can be used to for a DataFrame pointing to the current space.
         * @param translationLog List of rowRanges to be copied on newly allocated blocks.
         */
        void applyDefragmentation(const TranslationLog &translationLog) {
            for (const auto &translationUnit : translationLog.blockTranslation) {
                //Allocates the actual arrow table that would be mapped to the new block (to be registered as
                // translationUnit.targetPointer)
                std::unique_ptr<MutableTable> newTableBlock = allocateNewTableBlock();
                size_t targetRow = 0;
                for (const auto &pointerAndRangeLen : translationUnit.sourceRows_) {
                    visit([&](const ImmutableTable &sourceChunk, size_t offset, uint32_t length) {
                        //TODO copy rows sourceChunk[offset:offset+length] to newTableBlock[targetRow,targetRow:lenght]
                        targetRow += length;
                    }, pointerAndRangeLen.first, pointerAndRangeLen.second, schema_->field_names());
                }
                //Once the new block has been initialized, it is added to the data frame space
                registerData(translationUnit.targetPointer, TableReference(std::move(newTableBlock)));
                //NOTE that all the data initialization is done BEFORE the table corresponding to the new block is added
                //to the data frame space effectively allowing us the guarantee that any row accessible through the
                //space remains immutable
            }
        }
    };

    /**
     * The Index class is used to identify the rows from a DataFrameSpace that form an actual DataFrame
     *
     * It stores a number of ranges <SpacePointer, RangeLength> mapping data to the logical order in the DataFrame.
     * It is read-only
     * It limits fragmentation according to a MAX_FRAGMENTATION constant, where fragmentation is defined as:
     * <contiguous ranges count>/<rows count>, and the constraint is:
     * <contiguous ranges count>/<rows count> < MAX_FRAGMENTATION
     * Effectively this is intended to allow the amortized sequential read time to be comparable to a linear memory read.
     *
     * The actual implementation details and algorithm deserve a separate discussion, but the structure I have in mind
     * is similar to a BTree with immutable nodes. This give us a few important properties:
     *      - log(<ranges count>) positional access
     *      - constant time sequential iteration step
     *      - cache friendly positional access, compared to binary searching a vector or traversing a binary tree:
     *      For example:
     *      Given 1<<24 ranges, for BNodes of size 16-32, the max  b-tree height would be 6, hence we have a
     *      maximum of 6 cache misses to locate a chunk by first row position.
     *      By comparison binary searching an offset vector of size 1<<24, would take 24 steps and likely 20 cache misses
     *      (in both cases we assume 16 consecutive offsets make for a cache line, hence the last 4 steps of a binary
     *      search are likely cache hits).
     *      Similarly locating a key in a binary tree of that size may incur up to 24 misses.
     *
     *      - Node reuse in multiple indices: Indices can be sliced and stitched at S*log(S),where S is the number of
     *      stitches and slices in the constructing operation. A BNode is both immutable and agnostic of its absolute
     *      offset in the tree (it only maintains the relative offsets of its children), allowing for subtree reusal at
     *      potentially different positional offsets.
     */
    class Index {

    public:
        //The given fragmentation constraint
        inline static const double MAX_FRAGMENTATION = 0.01;

        Index(std::vector<std::pair<SpacePointer, RangeLength>> initializerList) {
            //Enforcing the MAX_FRAGMENTATION
            double elementCount = 0;
            double rangeCount = 0;
            for (const auto &p: initializerList) {
                rangeCount++;
                elementCount += p.second;
            }
            assert(rangeCount == 0.0 || (rangeCount / elementCount < MAX_FRAGMENTATION));
            //... init code
        }

        /**
         * Provides linear access to a region of the index
         *
         * The function is expected to complete in O(log(rangeCount)) + O(length)
         * @param visitor The function delivering the current active range to the reader
         * @param offset The positional start of the index operation
         * @param length The number of index positions being read;
         */
        void forEach(std::function<void(SpacePointer, RangeLength)> visitor, size_t offset, size_t length) const;

        /**
         * @return Returns the mapped row count
         */
        size_t size();
    };

    //A data frame interface - A DataFrame can be thought of as an <Index,DataFrameSpace> pair.
    class DataFrame {
    public:
        /**
         * Provides access to data for a given range
         * @param visitor Visitor function used to read the provided data - the function may be invoked one or
         *                multiple times, delivering requested data in consecutive chunks.
         * @param columns The subset of columns to be passed to the visitor function
         * @param offset The position start of the range being accessed
         * @param rowCount The number of rows to process
         */
        virtual void visit(std::function<void(const ImmutableTable &, size_t, uint32_t)> visitor,
                           const std::vector<std::string> &columns,
                           size_t offset, size_t rowCount) const = 0;

        //The DataFrameSpaces associated with the space
        virtual const std::shared_ptr<DataFrameSpace> &getSpace() = 0;

        //The Index associated with the space
        virtual const std::shared_ptr<Index> &getIndex() = 0;

        virtual size_t size() const;

    };

    /**
     * A read only data frame - it effectively composes an index and a DataFrameSpace object.
     */
    class ImmutableDataFrame : public DataFrame {
        //The index identifying the position of each row
        const std::shared_ptr<Index> index_;
        //The space containing the data
        const std::shared_ptr<DataFrameSpace> frameSpace_;
    public:
        ImmutableDataFrame(const std::shared_ptr<Index> &index, const std::shared_ptr<DataFrameSpace> &frameSpace) :
                index_(index), frameSpace_(frameSpace) {}


        void visit(std::function<void(const ImmutableTable &, size_t, uint32_t)> visitor,
                   const std::vector<std::string> &columns,
                   size_t offset, size_t rowCount) const override {

            index_->forEach([&](SpacePointer spaceOffset, RangeLength rowsCount) {
                frameSpace_->visit(visitor, spaceOffset, rowCount, columns);
            }, offset, rowCount);
        }

        const std::shared_ptr<DataFrameSpace> &getSpace() override {
            return frameSpace_;
        };

        const std::shared_ptr<Index> &getIndex() override {
            return index_;
        }
    };

    /**
     * A MutableDataFrame is just a light-weight wrapper around an ImmutableDataFrame
     *
     * I have opted for not relying read/write locking mechanism since that is likely to limit the ability to parallelize
     * operations against it and can be particularly hard to manage in a distributed environment.
     * Consequently thread safety relies on the following constructs:
     * - consistent snapshoting - A reader can request a snapshot that is guaranteed to remain immutable
     * - version tracking - Maintain consecutive versions for each change, which allows the reader to verify whether
     * the data to change since the last snapshot
     * - atomic update - The update operation is a simple pointer redirection together with a version update (yes we lock
     *  for that, but that lock is held for a well known number of instructions)
     * - concurrent modification check - each mutate operation specifies the expected new version which should always be
     * <current version>+1, if that version doesn't match the change is rejected. Effectively that guarantees that a writer
     * is modifying the version it "thinks" it is modifying or else the operation fails (a write is required to check on
     * the current version and data snapshot prior making a change)
     */
    class MutableDataFrame : public DataFrame {
        size_t version_ = 0;
        std::shared_ptr<ImmutableDataFrame> currentFrame_;

        mutable std::mutex mutationMutex_;

    public:

        explicit MutableDataFrame(const std::shared_ptr<ImmutableDataFrame> &initialValue) : currentFrame_(
                initialValue) {
        }

        /**
         * It effectively allows one to swap out the underlying immutable frame and replace it with a new one while
         * doing a concurrent modification check
         * @param newVersion - the expected newVersion is used to prevent concurrent modifications, by confirming the
         * change is really applied to the expected version
         * @param newFrame - A pointer to a new ImmutableDataFrame
         */
        void mutate(size_t newVersion, std::shared_ptr<ImmutableDataFrame> newFrame) {
            std::lock_guard<std::mutex> lock(mutationMutex_);
            if (newVersion != version_ + 1) {
                throw std::invalid_argument("Wrong update version");
            }
            currentFrame_ = std::move(newFrame);
        }

        /**
         * @return The current immutable value and the associated version
         */
        std::pair<size_t, std::shared_ptr<ImmutableDataFrame>> snapshot() const {
            std::lock_guard<std::mutex> lock(mutationMutex_);
            return {version_, currentFrame_};
        }

        /**
         * Note: This implementation itself is thread safe
         */
        void visit(std::function<void(const ImmutableTable &, size_t, uint32_t)> visitor,
                   const std::vector<std::string> &columns,
                   size_t offset, size_t rowCount) const override {
            currentFrame_->visit(visitor, columns, offset, rowCount);
        }

        const std::shared_ptr<DataFrameSpace> &getSpace() override {
            return currentFrame_->getSpace();
        };

        const std::shared_ptr<Index> &getIndex() override {
            return currentFrame_->getIndex();
        }

    };

    /**
     * This class provides the foundation for deriving one DataFrame's content from another's.
     * It simply gives the primitives for creating a new Index based of a combination of other indices.
     *
     * A new session is created for each new index construction, it can receive an arbitrary number of addSubIndex calls
     * appending Index slices to the right of the currently accumulated slices. The final index is created by invoking 
     * close method. Once close method is called, no other methods can be invoked against this object instance
     * 
     * In order to ensure FragmentationConstraint is respected by the result, addSubIndex and close methods may need to
     * do some defragmentation by relocating data pointed by incoming indices to new blocks:
     * Conceptually the mutation session stitches together subranges of rows, as long as average range size is big enough,
     * resulting DataFrame can just point to the same ranges, but if data ranges become too fragmented the new DataFrame 
     * read performance would degrade and so it is preferable to copy parts of the fragmented ranges to a newly allocated
     * contiguous area.
     * In practice, the index session is agnostic of the actual DataFrame schema and underlying DataFrameSpace, so index 
     * mutation would only create a TranslationLog (a log of the row copying that needs to happen) and produce a Index that points the
     * new ranges.
     */
    class IndexMutationSession {
        typename DataFrameSpace::SpaceManager &spaceManager_;
    public:
        IndexMutationSession(typename DataFrameSpace::SpaceManager &spaceManager);

        /**
         * Adds a subRange of an index to the current index accumulation.
         * 
         * Because indices a formed of immutable BTree nodes, the time and memory complexity are both O(<tree height>) = O(log(chunks count))
         * Subsequent optimizations can make complexity be amortized min(O(len),O(log(chunks count))), effectively making small range
         * insertions constant in memory and time cost.
         * @param index 
         * @param offset 
         * @param len 
         */
        void addSubIndex(const Index &index, size_t offset, size_t len);

        /**
         * Builds the final version of the index ensuring that it follows the fragmentation constraint specified in 
         * Index class definition:
         * rangeCount / elementCount < MAX_FRAGMENTATION
         * 
         * @return a pair formed of the resulting index and a TranslationLog corresponding to each new block address by
         * the resulting index as the result of the defragmentation;
         */
        std::pair<std::shared_ptr<Index>, TranslationLog> close();
    };


    struct DataFrameOperationExamples {

        /*
         * Each table mutation follow these steps (some may be skipped depenting on the operating nam):
         * Step 1. Associate arrow table data with the data frame space
         * Step 2. Create an index for the new data
         * Step 3. Create a result index by combining one or more indices
         * Step 4. Apply any defragmentation by copying data onto the new buffers
         * Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
         */

        /**
         * Creates a data frame from a table
         * @param sourceTable - a unique pointer to an arrow table - since we expect to use it as an immutable data
         * source, we need to ensure there are no other active pointers reaching the table.
         * @return
         */
        MutableDataFrame dataFrameFromTable(std::unique_ptr<arrow::Table> sourceTable) {
            auto numRows = sourceTable->num_rows();
            // Step 1. Associate arrow table data with the data frame space
            //Create a DataFrameSpace matching the incoming schema
            auto dataFrameSpace = std::make_shared<DataFrameSpace>(sourceTable->schema());
            //Make the incoming table a constant
            TableReference tableReference(std::move(sourceTable));
            tableReference.makeConst();
            //Register the incoming table with the data frame space and retrieve an associated pointer
            SpacePointer dataPointer = dataFrameSpace->registerExternalData(std::move(tableReference));
            //Step 2. Create an index for the new data
            //Create and index identifying the data location in the frame space:
            auto index = std::make_shared<Index>(std::vector<std::pair<SpacePointer, RangeLength>>{
                    {dataPointer, static_cast<RangeLength>(numRows)}});
            //(Steps 3,4 do not apply here)
            //Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            //Create the actual data frame by composing the index with the data frame space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(index, dataFrameSpace);
            //Creates a mutable data frame having this as the original value
            return MutableDataFrame(immutableDataFrame);
        }

        //Placeholder class for a utility that would allow for lazily loading paquet data
        struct MockParquetProvider {
            MockParquetProvider(std::string filePath);

            DataFrameSpace::DataProvider getDataProvider();

            size_t getRowCount();

            std::shared_ptr<arrow::Schema> schema();
        };

        /**
         * Creates a data frame that maps virtually to a parquet file
         * @param sourceTable - a unique pointer to an arrow table - since we expect to use it as an immutable data
         * source, we need to ensure there are no other active pointers reaching the table.
         * @return
         */
        MutableDataFrame dataFrameFromParquetFile(std::string filePath) {
            MockParquetProvider parquetProvider(filePath);
            auto numRows = parquetProvider.getRowCount();
            // Step 1. Associate arrow table data with the data frame space
            //Create a DataFrameSpace matching the incoming schema
            auto dataFrameSpace = std::make_shared<DataFrameSpace>(parquetProvider.schema());
            //Register the incoming provider with the data frame space and retrieve an associated pointer
            SpacePointer dataPointer = dataFrameSpace->registerDataProvider(parquetProvider.getDataProvider(), numRows);
            //Step 2. Create an index for the new data
            //Create and index identifying the data location in the frame space:
            auto index = std::make_shared<Index>(std::vector<std::pair<SpacePointer, RangeLength>>{
                    {dataPointer, static_cast<RangeLength>(numRows)}});
            //(Steps 3,4 do not apply here)
            //Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            //Create the actual data frame by composing the index with the data frame space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(index, dataFrameSpace);
            //Creates a mutable data frame having this as the original value
            return MutableDataFrame(immutableDataFrame);
        }


        /**
         * Inserts data from an arrow Table into a mutable data frame (it basically interleaves newRows with dataFrame
         * at locations specified by newRowRangePositions)
         * @param dataFrame DataFrame to be modified
         * @param newRows An arrow table containing all the new rows to be inserted
         * @param newRowRangePositions List of <insertion point,row count> indicating where the respective rows need to
         * be inserted in the mutable table and the number of rows that need to be inserted at each point
         */
        void insertAt(MutableDataFrame &dataFrame, std::unique_ptr<arrow::Table> newRows,
                      std::vector<std::pair<size_t, size_t>> newRowRangePositions) {
            auto numNewRows = newRows->num_rows();
            //Starts the mutation process with a dataframe snapshot in
            // order to ensure we are working against fixed data;
            auto dataFrameSnapShot = dataFrame.snapshot();
            //* Step 1. Associate arrow table data with the data frame space
            //Retrieve the current data frame space
            auto dataFrameSpace = dataFrameSnapShot.second->getSpace();
            //Make the incoming table a constant
            TableReference newRowsReference(std::move(newRows));
            newRowsReference.makeConst();
            //Add the new rows to the data frame space
            SpacePointer newRowsStartPointer = dataFrameSpace->registerExternalData(std::move(newRowsReference));
            //* Step 2. Create an index for the new data
            //Create an index identifying inserted row location in the data frame space:
            auto newRowsIndex = std::make_shared<Index>(std::vector<std::pair<SpacePointer, RangeLength>>{
                    {newRowsStartPointer, static_cast<RangeLength>(numNewRows)}});
            //* Step 3. Create a result index by combining one or more indices
            //We create an index session and we combine the new row index with existing data frame index:
            auto currentIndex = dataFrameSnapShot.second->getIndex();
            IndexMutationSession indexMutationSession(dataFrameSpace->spaceManager);
            size_t prevOriginalFrameOffset = 0;
            size_t currentInsertOffset = 0;
            for (const auto &positionAndLen :  newRowRangePositions) {
                size_t offsetInOriginalFrame = positionAndLen.first;
                if (offsetInOriginalFrame > prevOriginalFrameOffset) {
                    //Add row range from existing data frame
                    indexMutationSession.addSubIndex(*currentIndex, prevOriginalFrameOffset,
                                                     offsetInOriginalFrame - prevOriginalFrameOffset);
                    prevOriginalFrameOffset = offsetInOriginalFrame;
                }
                //Add the indices corresponding to the new rows
                indexMutationSession.addSubIndex(*newRowsIndex, currentInsertOffset, positionAndLen.second);
            }
            if (prevOriginalFrameOffset < currentIndex->size()) {
                //Add any remaining rows
                indexMutationSession.addSubIndex(*currentIndex, prevOriginalFrameOffset,
                                                 currentIndex->size() - prevOriginalFrameOffset);
            }
            std::pair<std::shared_ptr<Index>, TranslationLog> indexAndDefragmentation = indexMutationSession.close();
            //* Step 4. Apply any defragmentation by copying data onto the new buffers
            dataFrameSpace->applyDefragmentation(indexAndDefragmentation.second);
            //* Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(indexAndDefragmentation.first,
                                                                           dataFrameSpace);
            //apply the new value to the input frame together with the expected version in order to avoid concurrent changes:
            //If the dataFrame has changed since snapshot method was called at the beginning of the function, then mutate
            //would fail because expectedVersion would no longer be dataFrameSnapShot.first
            dataFrame.mutate(dataFrameSnapShot.first + 1, std::move(immutableDataFrame));
        }

        /**
         * Modifies a given set of row ranges - this is very similar to insertAt, the main different being that rather
         * than inserting new rowRanges, we replace row at the old ranges with new ones
         * @param dataFrame DataFrame to be modified
         * @param newRows An arrow table containing all the new rows values for the rows to be modified
         * @param newRowRangePositions List of <insertion point,row count> indicating where the respective rows need to
         * be modifier in the mutable table and the number of rows that need to be altered at each point
         */
        void updateAt(MutableDataFrame &dataFrame, std::unique_ptr<arrow::Table> newRows,
                      std::vector<std::pair<size_t, size_t>> newRowRangePositions) {
            auto numNewRows = newRows->num_rows();
            //Starts the mutation process with a dataframe snapshot in
            // order to ensure we are working against fixed data;
            auto dataFrameSnapShot = dataFrame.snapshot();
            //* Step 1. Associate arrow table data with the data frame space
            //Retrieve the current data frame space
            auto dataFrameSpace = dataFrameSnapShot.second->getSpace();
            //Make the incoming table a constant
            TableReference newRowsReference(std::move(newRows));
            newRowsReference.makeConst();
            //Add the new rows to the data frame space
            SpacePointer newRowsStartPointer = dataFrameSpace->registerExternalData(std::move(newRowsReference));
            //* Step 2. Create an index for the new data
            //Create an index identifying modified row new value location in the data frame space :
            auto newRowsIndex = std::make_shared<Index>(std::vector<std::pair<SpacePointer, RangeLength>>{
                    {newRowsStartPointer, static_cast<RangeLength>(numNewRows)}});
            //* Step 3. Create a result index by combining one or more indices
            //We create an index session and we combine the new row index with existing data frame index:
            auto currentIndex = dataFrameSnapShot.second->getIndex();
            IndexMutationSession indexMutationSession(dataFrameSpace->spaceManager);
            size_t prevOriginalFrameOffset = 0;
            size_t currentInsertOffset = 0;
            for (const auto &positionAndLen :  newRowRangePositions) {
                size_t offsetInOriginalFrame = positionAndLen.first;
                if (offsetInOriginalFrame > prevOriginalFrameOffset) {
                    //Add row range from existing data frame
                    indexMutationSession.addSubIndex(*currentIndex, prevOriginalFrameOffset,
                                                     offsetInOriginalFrame - prevOriginalFrameOffset);
                    prevOriginalFrameOffset = offsetInOriginalFrame;
                }
                //Add the indices corresponding to the new rows
                indexMutationSession.addSubIndex(*newRowsIndex, currentInsertOffset, positionAndLen.second);
                //***********!!!!!!!!!!!!!!!!!!!!!!!!!!***********
                //This is the only difference from the insertAt function - we effectively skip the old rows in the new index:
                prevOriginalFrameOffset += positionAndLen.second;

            }
            if (prevOriginalFrameOffset < currentIndex->size()) {
                //Add any remaining rows
                indexMutationSession.addSubIndex(*currentIndex, prevOriginalFrameOffset,
                                                 currentIndex->size() - prevOriginalFrameOffset);
            }
            std::pair<std::shared_ptr<Index>, TranslationLog> indexAndDefragmentation = indexMutationSession.close();
            //* Step 4. Apply any defragmentation by copying data onto the new buffers
            dataFrameSpace->applyDefragmentation(indexAndDefragmentation.second);
            //* Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(indexAndDefragmentation.first,
                                                                           dataFrameSpace);
            //apply the new value to the input frame together with the expected version in order to avoid concurrent changes:
            //If the dataFrame has changed since snapshot method was called at the beginning of the function, then mutate
            //would fail because expectedVersion would no longer be dataFrameSnapShot.first
            dataFrame.mutate(dataFrameSnapShot.first + 1, std::move(immutableDataFrame));
        }

        /**
         * Creates a new data frame containing a subset of the input data frame (for example the rows matching a filter condition)
         * @param src data frame we are filtering
         * @param rangesToKeep A list of positional ranges expressed as <offset,length>
         * @return a new DataFrame containing only the requested ranges
         */
        MutableDataFrame filter(const MutableDataFrame &src, std::vector<std::pair<size_t, size_t>> rangesToKeep) {
            //Starts the mutation process with a dataframe snapshot in
            // order to ensure we are working against fixed data;
            auto dataFrameSnapShot = src.snapshot();
            //Retrieve the current data frame space
            auto dataFrameSpace = dataFrameSnapShot.second->getSpace();
            //* Step 1. N/A since there is no new external data
            //* Step 2. N/A since there is no new index
            //* Step 3. Create a result index by combining one or more indices
            //We create an index session and combine the subIndices we need to keep
            auto currentIndex = dataFrameSnapShot.second->getIndex();
            IndexMutationSession indexMutationSession(dataFrameSpace->spaceManager);
            for (const auto &rangeToKeep :  rangesToKeep) {
                size_t offsetInOriginalFrame = rangeToKeep.first;
                indexMutationSession.addSubIndex(*currentIndex, rangeToKeep.first, rangeToKeep.second);
            }
            std::pair<std::shared_ptr<Index>, TranslationLog> indexAndDefragmentation = indexMutationSession.close();
            //* Step 4. Apply any defragmentation by copying data onto the new buffers
            dataFrameSpace->applyDefragmentation(indexAndDefragmentation.second);
            //* Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(indexAndDefragmentation.first,
                                                                           dataFrameSpace);
            //Create a new mutable data frame around
            return MutableDataFrame(std::move(immutableDataFrame));
        }


        void puttingItAllTogether() {
            //Creates a data frame that virtually maps to a parquet file while allowing for lazy data loading
            MutableDataFrame df = dataFrameFromParquetFile("/path/To/parquet/file");

            //Following a filter operation we decide we want every 1000th element from the parquet file:
            std::vector<std::pair<size_t,size_t>> filteredRanges;
            for (size_t i = 0; i < df.size();i+=1000) {
                filteredRanges.emplace_back(i,1);
            }
            //we filter the content of the given parquet table and since the resulting data is very sparse the mutation
            //mechanism would automatically defragment, effectively copying the targeted rows in a new contiguous region
            MutableDataFrame every1000th = filter(df,filteredRanges);

            //A second new filter indicate that we are interested in 3 subregions of the parquet data frame of size 10,000 each
            std::vector<std::pair<size_t,size_t>> filter2 = {{12300,10000},
                                                             {32100,10000},
                                                             {65300,10000}};

            //We apply the new filter range, but this time the likely result would map directly to the virtual parquet region same as df
            MutableDataFrame threeRanges = filter(df,filter2);

            //A third filter, shows that we want the 1000 contiguous elements followed by every 10 elements from the subsequet 1000 elements range
            std::vector<std::pair<size_t,size_t>> filter3;
            //todo init filter3 accordingly
            //The defragmentation logic in this case would likely reuse some of the large blocks while copying the sparse data in a new compact region
            MutableDataFrame hybridDf = filter(df,filter3);

            //... after processing hybridDf, we decide that we need to update the values at row 100, 200-250,300-1300:
            //First we store all the desired new row values in a new arrow::Table
            std::unique_ptr<arrow::Table> newTable;
            //todo initialize newTable to have 1201 rows with the respective desired row values for ranges 100, 200-250,300-1300
            //Now we apply the changes to hybridDf - the new version of hybridDf is expected to
            // - continue to point to most* of the same arrow array as the previous one (i.e. most of the unchanged copied data is not)
            // - point to the arrays from newTable for most* of the new rows
            // - a small number of rows (O(<number of cuts>) ... here O(3)), may be relocated to a new contiguous region in order to
            // maintain the defragmentation contract
            updateAt(hybridDf,std::move(newTable),{{100,1},{200,50},{300,1300}});
            //Threading note: By design - updateAt is executed concurrently by multiple threads we guarantee hybridDf final
            //value is actually correct


            //Takeaways:
            //- I have shown how virtual and non-virtual data can coexist in a data frame
            //- I have shown how a new data-frame can be derived from an existing one by reusing most* of the underlying data
            //- I have proposed a copy-write-model that maximizes* the data reuse
            //- I have proposed a fragmentation management constraint that balance shallow copies with providing expectations
            // regarding data density
            //- I have provided mechanism that allows for concurrent readers and a single writer accessing the same data frame
            //- I have provided means for detecting concurrent modifications and maintaining data consistency during a
            //concurrent modification scenario - The envision data structures can potentially accommodate concurrent
            //modification occurring on different data frame region, but that is not covered by this proposal.
            //Disclaimers:
            //- There are implementation details behind the Index architecture not available here, but I am happy to go
            // for a deep dive on that topic and I hope to release some independently working code illustrating the mechanisms
            //- I don't cover the actual details of guaranteeing the immutability of arrow arrays subordinated to an
            //arrow::Table as I am probably not the most qualified person to do it.
            //- I am somewhat dyslexic so please excuse spelling mistakes and flag statements that don't make any sense,
            //I promise there was though and meaning behind them, it just got corrupted during the transcription process
        }
    };
}

#endif //EXPERIMENTS_FRAMESPACE_H
