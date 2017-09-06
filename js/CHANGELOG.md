<a name="0.1.2"></a>
## [0.1.2](https://github.com/graphistry/arrow/compare/v0.1.1...v0.1.2) (2017-09-06)


### Bug Fixes

* **DictionaryVector:** Add index and value methods to DictionaryVector ([a8341f5](https://github.com/graphistry/arrow/commit/a8341f5))
* **Table:** Add fromStruct method to Table ([e1c7852](https://github.com/graphistry/arrow/commit/e1c7852))



<a name="0.1.1"></a>
## [0.1.1](https://github.com/graphistry/arrow/compare/v0.1.0...v0.1.1) (2017-08-26)



<a name="0.1.0"></a>
# 0.1.0 (2017-08-26)


### Bug Fixes

* **reader:** fix dictionary record batch reader, synthesize dictionary index metadata ([b33e371](https://github.com/graphistry/arrow/commit/b33e371))



<a name="0.0.4"></a>
## 0.0.4 (2017-08-23)


### Bug Fixes

* **vectors:** fix vector iteration, add more tests, improve iteration performance ([3aeab5a](https://github.com/graphistry/arrow/commit/3aeab5a))


### Performance Improvements

* **vector:** avoid memcpy during slice if possible ([e453148](https://github.com/graphistry/arrow/commit/e453148))


<a name="0.0.3"></a>
## 0.0.3 (2017-08-16)

### Performance Improvements

* **tests:** Add perf tests and fill out Table API ([bfcc17c](https://github.com/graphistry/arrow/commit/bfcc17c))
* **VirtualVector:** Inline the `findVirtual` calls so we don't eat the cost of iterating. ([d46f812](https://github.com/graphistry/arrow/commit/d46f812))

<a name="0.0.2"></a>
## 0.0.2 (2017-08-15)

<a name="0.0.1"></a>
## 0.0.1 (2017-08-15)


### Bug Fixes

* **vectors:** Add vector tests, fix slice behavior on LongVectors, and update externs. ([6a67b3b](https://github.com/graphistry/arrow/commit/6a67b3b))


### Features

* **Arrow:** Initial commit of arrow reader, vectors, table ([544bca0](https://github.com/graphistry/arrow/commit/544bca0))


### Performance Improvements

* **tests:** Add perf tests and fill out Table API ([bfcc17c](https://github.com/graphistry/arrow/commit/bfcc17c))
* **VirtualVector:** Inline the `findVirtual` calls so we don't eat the cost of iterating. ([d46f812](https://github.com/graphistry/arrow/commit/d46f812))


