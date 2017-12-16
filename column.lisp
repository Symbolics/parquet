;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;;; Structures and functions to read a column

;; column-chunk
(defstruct column-chunk
  (file-path nil :type string)
  (file-offset 0 :type integer) ; i64
  (meta-data) ; ColumnMetaData
  )

;; (defstruct column-metadata ...
(defstruct column-metadata
  (type) ; Type
  (encodings) ; list<Encoding>
  (path-in-schema) ; list<string>
  (codec) ; CompressionCodec
  (num-values 0 :type integer) ;i64
  (total-uncompressed-size 0 :type integer) ;i64
  (total-compressed-size 0 :type integer)
  (key-value-metadata) ; list<KeyValue>
  (data-page-offset 0 :type integer) ;i64
  (index-page-offset 0 :type integer) ;i64
  (dictionary-page-offset 0 :type integer) ;i64
  )
