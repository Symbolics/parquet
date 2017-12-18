;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;;; Structures and functions to read a column

;; column-chunk
(defstruct column-chunk
  "File where column data is stored.  If not set, assumed to be same file as metadata.
   This path is relative to the current file."
  (file-path "" :type string)
  (file-offset 0 :type int64) ; i64
  (meta-data) ; ColumnMetaData
  ;; File offset of ColumnChunk's OffsetIndex
  (offset_index_offset)
  ;; Size of ColumnChunk's OffsetIndex, in bytes
  (offset_index_length)
  ;; File offset of ColumnChunk's ColumnIndex 
  (column_index_offset)
  ;; Size of ColumnChunk's ColumnIndex, in bytes
  (column_index_length))


(defstruct column-metadata
  "Description for column metadata"
  (type 0 :type int32) ; Type
  (encodings) ;; set of all encodings used for this column
  (path-in-schema) ; list<string>
  (codec) ; CompressionCodec
  (num-values 0 :type int64)
  (total-uncompressed-size 0 :type int64)
  (total-compressed-size 0 :type int64)
  (key-value-metadata) ; list<KeyValue>
  (data-page-offset 0 :type int64) ;; Byte offset from beginning of file to first data page
  (index-page-offset)
  (dictionary-page-offset)
  (statistics)
  (encoding_stats))


(defun extract-column-metadata (metadata field s)
  "parse COLUMN-METADATA from byte streams"
  (get-id-type s field)
  ;;(format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field)) (format t "end of column-metadata : ~A ~%" (file-position s)))
    ((= 1 (field-id field))
     ;; required Type
     (setf (column-metadata-type metadata) (zigzag-to-int (var-ints s)))
     (extract-column-metadata metadata field s))
    ((= 2 (field-id field))
     ;; list <Encoding> encoding
     (let ((len (list-len s)))
       ;;(format t "list length : ~A ~%" len)
       (loop for i from 1 to len
             do (setf (column-metadata-encodings metadata)
                      (push (zigzag-to-int (var-ints s)) (column-metadata-encodings metadata)))))
     (extract-column-metadata metadata field s))
    ((= 3 (field-id field))
     (let ((len (list-len s)))
       ;;(format t "list length : ~A ~%" len)
       (loop for i from 1 to len
             do (setf (column-metadata-path-in-schema metadata)
                      (push (apply #'mkstr (char-list s)) (column-metadata-path-in-schema metadata)))))
     (extract-column-metadata metadata field s))
    ((= 4 (field-id field))
     ;; required CompressionCodec codec
     (setf (column-metadata-codec metadata) (zigzag-to-int (var-ints s)))
     (extract-column-metadata metadata field s))
    ((= 5 (field-id field))
     ;; required i64 num_values
     (setf (column-metadata-num-values metadata) (zigzag-to-int (var-ints s)))
     (extract-column-metadata metadata field s))
    ((= 6 (field-id field))
     ;; required i64 totoal_uncompressed_size
     (setf (column-metadata-total-uncompressed-size metadata) (zigzag-to-int (var-ints s)))
     (extract-column-metadata metadata field s))
    ((= 7 (field-id field))
     ;; required i64 totoal_compressed_size
     (setf (column-metadata-total-compressed-size metadata) (zigzag-to-int (var-ints s)))
     (extract-column-metadata metadata field s))
    ((= 8 (field-id field))
     ;; list<keyValue>
     (let ((len (list-len s)))
       ;;(format t "list length : ~A ~%" len)
       (loop for i from 1 to len
             do (let ((kv (make-key-value))
                      (field-info (make-field)))
                  (extract-key-value kv field-info s)
                  (setf (column-metadata-key-value-metadata metadata)
                        (push kv (column-metadata-key-value-metadata metadata))))))
     (extract-column-metadata metadata field s))
    ((= 9 (field-id field))
     ;; i64 data-page-offset
     (setf (column-metadata-data-page-offset metadata) (zigzag-to-int (var-ints s)))
     (extract-column-metadata metadata field s))
    ((= 10 (field-id field))
     ;; i64 index-page-offset
     (setf (column-metadata-index-page-offset metadata) (zigzag-to-int (var-ints s)))
     (extract-column-metadata metadata field s))
    ((= 11 (field-id field))
     ;; i64 dictionay-page-offset
     (setf (column-metadata-dictionary-page-offset metadata) (zigzag-to-int (var-ints s)))
     (extract-column-metadata metadata field s))
    ((= 12 (field-id field))
     ;; opetional Statistics
     (error "Sorry. No Yet Implemented Statisics"))
    ((= 11 (field-id field))
     (error "Sorry. No Yet Implemented PageEncodingStats"))
    (t (error "Sorry. Unsupported Field/Type of Column-Metadata"))))


(defun extract-column-chunk (columnchunk field s)
  "parse COLUMN_CHUNK from byte stream"
  (get-id-type s field)
  ;;(format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field)) (format t "end of column-chunk : ~A ~%" (file-position s)))
    ((= 1 (field-id field))
     (error "Sorry, Not Yet Implemented file where column data is stored"))
    ((= 2 (field-id field))
     ;; required i64 file-offset
     (setf (column-chunk-file-offset columnchunk) (zigzag-to-int (var-ints s)))
     (extract-column-chunk columnchunk field s))
    ((= 3 (field-id field))
     ;; optional COLUMNMETADATA meta-data
     (let ((c-metadata (make-column-metadata))
           (field-info (make-field)))
       (extract-column-metadata c-metadata field-info s)
       (setf (column-chunk-meta-data columnchunk) c-metadata))
     (extract-column-chunk columnchunk field s))
    ((= 4 (field-id field))
     (error "Sorry, Not Yet Implemented 4 th field of Column-Chunk"))
    ((= 5 (field-id field))
     (error "Sorry, Not Yet Implemented 5 th field of Column-Chunk"))
    ((= 6 (field-id field))
     (error "Sorry, Not Yet Implemented 6 th field of Column-Chunk"))
    ((= 7 (field-id field))
     (error "Sorry, Not Yet Implemented 7 th field of Column-Chunk"))
    (t (error "Sorry. Unsupported Field/Type of Column-Chunk"))))
