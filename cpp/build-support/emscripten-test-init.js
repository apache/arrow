var Module = {
};

// make sure tests can access the current parquet test data files
Module.preRun = () => {ENV.PARQUET_TEST_DATA = process.env.PARQUET_TEST_DATA};