# What is this?
This is a parquet file reader made by lisp. You can read headers of FileMetaData, ColumnMetadata, ChunkData but not all headers. Currently, this project is only support a SNAPPY compression codesc.

For more parquet headers, please read a parquet-format specs. (https://github.com/apache/parquet-format).

# usages
For example, some parquet files is located in "tests/tpch/".

## load parquet file

`load-parquet` will return a `data-parquet-table` class. This class has a column-names, dataset of the loaded parquet file. 
```
(load-parquet "./tests/tpch/region.parquet")
(print (load-parquet "./tests/tpch/region.parquet"))
```
## show me the dataset

### show data set by limit
A defalut limit is 10.
```
(show-data (load-parquet "./tests/tpch/region.parquet"))
(show-data (load-parquet "./tests/tpch/part.parquet") :limit 9)
```
### column names
```
(get-column-names (load-parquet "./tests/tpch/region.parquet"))
```

### take a data by column name
```
(get-data (load-parquet "./tests/tpch/region.parquet"))
(get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey")
(get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_comment")
```

### row and column count
```
(get-num-cols (load-parquet "./tests/tpch/region.parquet"))
(get-num-rows (load-parquet "./tests/tpch/region.parquet"))
```

### five number summary
```
;; means
(sample-mean (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey"))
;; min
(min-item-from (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey"))
;; max
(max-item-from (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey"))
;; quantile
(quantile-of-ordered-seq (sort (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey") #'<) 0.25)
(quantile-of-ordered-seq (sort (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey") #'<) 0.75)
```
