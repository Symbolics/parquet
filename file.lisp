;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;; meta-data structures
;; file-metadata
(defstruct file-meta-data
  "Description for file metadata"
  (version 0 :type int32) ; i32
  (schema-element nil) ; list <Schema-elenent>
  (num-rows 0 :type int64) ; i64
  (row-groups nil) ; list <RowGroup>
  (key-value-metadata nil) ; list<KeyValue>
  (created-by)
  (column-orders))

;; schema-element
(defstruct schema-element
  "Represents a element inside a schema definition"
  (type nil) ; Data type for this field. Not set if the current element is a non-leaf node
  ;; If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the vales.
  ;; Otherwise, if specified, this is the maximum bit length to store any of the values.
  ;; (e.g. a low cardinality INT col could have this set to 3).  Note that this is
  ;; in the schema, and therefore fixed for the entire file.
  (type-length) 
  (repetition-type nil) ; FieldRepetition
  (name "" :type string) ;requried
  (num-children 0 :type i32)
  (converted-type nil) ;ConvertedType
  (scale 0 :type i32)
  (precision 0 :type i32)
  (field-id 0 :type i32)
  (logicalType nil))


;; key-value
(defstruct key-value
  (key "" :type string)
  (value "" :type string))

(defun read-first-four-bytes (s)
  "read first four bytes"
  (loop
    repeat 4
    collect (read-byte s nil nil)))

(defun read-last-four-bytes (s)
  "read last four bytes"
  (file-position s (- (file-length s) 4))
  (loop
    repeat 4
    collect (read-byte s nil nil)))


(defun magic-number? (filename)
  "check first/last 'PAR1'"
  (with-open-file (s filename :element-type '(unsigned-byte 8))
    (and (equal '(80 65 82 49) (read-first-four-bytes s))
         (equal '(80 65 82 49) (read-last-four-bytes s)))))


(defun read-u32 (in)
  "read 32 bits integer with litten endian"
  (let ((u4 0))
    (setf (ldb (byte 8 0) u4) (read-byte in))
    (setf (ldb (byte 8 8) u4) (read-byte in))
    (setf (ldb (byte 8 16) u4) (read-byte in))
    (setf (ldb (byte 8 24) u4) (read-byte in))
    (the int32 u4)))


(defun footer-length (filename)
  "length of footer. First, check a magic-number 'PAR1' and then get a footer length"
  (with-open-file (s filename :element-type '(unsigned-byte 8))
    (and (magic-number? filename)
       (file-position s (- (file-length s) 8))
       (read-u32 s))))

(defun extract-key-value (kv field s)
  "parse KEY-VALUE from byte streams"
  (get-id-type s field)
  ;; (format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field)) (format t "end of key-value : ~A ~%" (file-position s)))
    ((= 1 (field-id field))
     ;; string key
     (setf (key-value-key kv) (apply #'mkstr (char-list s)))
     (extract-key-value kv field s))
    ((= 2 (field-id field))
     ;; optional value
     (setf (key-value-value kv) (apply #'mkstr (char-list s)))
     (extract-key-value kv field s))
    (t (error "Sorry. Unsupported Field/Type of FileMetaData"))))


(defun extract-schema-element (schema field s)
  "parse SCHEMA-ELEMENT from byte streams"
  (get-id-type s field)
  ;;(format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field)) (format t "end of schema-element : ~A ~%" (file-position s)))
    ((= 1 (field-id field))
     ;; optional Type type;
     (setf (schema-element-type schema) (zigzag-to-int (var-ints s)))
     (extract-schema-element schema field s))
    ((= 2 (field-id field))
     ;; optional i32 type_length;
     (setf (schema-element-type-length schema) (zigzag-to-int (var-ints s)))
     (extract-schema-element schema field s))
    ((= 3 (field-id field))
     ;; optional FieldRepetitionType repetition_type;
     (setf (schema-element-repetition-type schema) (zigzag-to-int (var-ints s)))
     (extract-schema-element schema field s))
    ((= 4 (field-id field))
     ;; required string name;
     (setf (schema-element-name schema) (apply #'mkstr (char-list s)))
     (extract-schema-element schema field s))
    ((= 5 (field-id field))
     ;; optional i32 num_children;
     (setf (schema-element-num-children schema) (zigzag-to-int (var-ints s)))
     (extract-schema-element schema field s))
    ((= 6 (field-id field))
     ;; optional ConvertedType converted_type;
     (setf (schema-element-converted-type schema) (zigzag-to-int (var-ints s)))
     (extract-schema-element schema field s))
    ((= 7 (field-id field))
     ;; optional i32 scale
     (setf (schema-element-scale schema) (zigzag-to-int (var-ints s)))
     (extract-schema-element schema field s))
    ((= 8 (field-id field))
     ;; optional i32 precision
     (setf (schema-element-precision schema) (zigzag-to-int (var-ints s)))
     (extract-schema-element schema field s))
    ((= 9 (field-id field)) 
     ;; optional i32 field_id;
     (setf (schema-element-field-id schema) (zigzag-to-int (var-ints s)))
     (extract-schema-element schema field s))
    ((= 10 (field-id field))
     ;; optional LogicalType logicalType
     (error "Sorry, Not Yet Implemented LogicalType"))
    (t (error "Sorry. Unsupported Field/Type of FileMetaData"))))

(defun extract-file-meta-data (meta field s)
  "parse FILE-META-DATA from byte streams"
  (get-id-type s field)
  ;;(format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field)) (format t "end of file-meta-data : ~A ~%" (file-position s)))
    ((= 1 (field-id field))
     ;; i32 version
     (setf (file-meta-data-version meta) (zigzag-to-int (var-ints s)))
     (extract-file-meta-data meta field s))
    ((= 2 (field-id field))
     ;; required list<SchemaElement>;
     (let ((len (list-len s)))
       ;;(format t "list length : ~A ~%" len)
       (loop for i from 1 to len
             do (let ((schema (make-schema-element))
                      (field-info (make-field)))
                  ;; extract schema-element
                  (extract-schema-element schema field-info s)
                  (setf (file-meta-data-schema-element meta) (push schema (file-meta-data-schema-element meta))))))
     ;(format t "::::: ~A ~%" meta)
     (extract-file-meta-data meta field s))
    ((= 3 (field-id field))
     ;; required i64 num_rows
     (setf (file-meta-data-num-rows meta) (zigzag-to-int (var-ints s)))
     (extract-file-meta-data meta field s))
    ((= 4 (field-id field))
     ;; required list<RowGroup> row_groups
     (let ((len (list-len s)))
       ;;(format t "list length : ~A ~%" len)
       (loop for i from 1 to len
             do (let ((rowgroup (make-row-group))
                      (field-info (make-field)))
                  (extract-row-group rowgroup field-info s)
                  (setf (file-meta-data-row-groups meta) (push rowgroup (file-meta-data-row-groups meta))))))
     (extract-file-meta-data meta field s))
    ((= 5 (field-id field))
     ;; list<keyValue> key-value-meta
     (let ((len (list-len s)))
       ;;(format t "list length : ~A ~%" len)
       (loop for i from 1 to len
             do (let ((kv (make-key-value))
                      (field-info (make-field)))
                  (extract-key-value kv field-info s)
                  (setf (file-meta-data-key-value-metadata meta)
                        (push kv (file-meta-data-key-value-metadata meta))))))
     (extract-file-meta-data meta field s))
    ((= 6 (field-id field))
     ;; string create-by
     (setf (file-meta-data-created-by meta) (apply #'mkstr (char-list s)))
     (extract-file-meta-data meta field s))
    ((= 7 (field-id field))
     ;; list <columnorder>
     (error "NOT Yet Implemented the ColumnOrder"))
    (t (error "Sorry. Unsupported Field/Type of FileMetaData"))))




;;; functions for reading Parquet structures.

(defun read-file-meta-data (filename)
  "Returns a structure of type file-metadata with the values from FILENAME. Returns an error if FILENAME is not a parquet file."
  (with-open-file (s filename :element-type '(unsigned-byte 8))
    (format t "File-Meta-Data position is : ~A ~%" (file-position s (- (file-length s) (footer-length filename) 8)))
    (let ((fmd (make-file-meta-data))
          (field-info (make-field)))
      (extract-file-meta-data fmd field-info s)
      fmd)))

(defun get-group-columns-metadata (filename)
  "return a structures of Group-Columns-Metadata"
  (first (mapcar #'row-group-columns (file-meta-data-row-groups (read-file-meta-data filename)))))

(defun get-column-chunk-metadata (filename)
  "return a structures of Columns-Chunks"
  (mapcar #'column-chunk-meta-data (first (mapcar #'row-group-columns (file-meta-data-row-groups (read-file-meta-data filename))))))


(file-meta-data-num-rows (read-file-meta-data "./tests/tpch/region.parquet"))
(read-file-meta-data "./tests/tpch/lineitem.parquet")
(read-file-meta-data "./tests/tpch/orders.parquet")
(read-file-meta-data "./tests/tpch/part.parquet")
(read-file-meta-data "./tests/tpch/partsupp.parquet")
(read-file-meta-data "./tests/tpch/supplier.parquet")
(read-file-meta-data "./tests/tpch/customer.parquet")

(get-group-columns-metadata "./tests/tpch/region.parquet")

(get-column-chunk-metadata "./tests/tpch/region.parquet")

(defun get-data-type-columns (metadata)
  "return offset of COLUMN-METADATA"
  (mapcar (lambda (x) (data-type (column-metadata-type x))) metadata))


(defun get-offset-page (metadata)
  "return offset of the PAGE-HEADER"
  (or (column-metadata-data-page-offset metadata)
      (column-metadata-index-page-offset metadata)
      (column-metadata-dictionary-page-offset metadata)))


(defun read-bytes-between (filename start end)
  "read bytes-array between start and end"
  (with-open-file (s filename :element-type '(unsigned-byte 8))
    (file-position s start)
    (let ((vec (make-array end :element-type '(unsigned-byte 8))))
      (loop for i from 0 to (- end 1)
            do (setf (aref vec i) (read-byte s nil nil)))
      vec)))

(defun read-byte-array (bytes)
  "return string column bytes"
  (let ((idx 0))
    (loop while (< idx (length bytes))
          collect (let ((len 0))
                    (setf (ldb (byte 8 0) len) (aref bytes idx))
                    (setf (ldb (byte 8 8) len) (aref bytes (incf idx)))
                    (setf (ldb (byte 8 16) len) (aref bytes (incf idx)))
                    (setf (ldb (byte 8 24) len) (aref bytes (incf idx)))
                    (format t "length : ~A ~%" len)
                    (let ((str (make-array len)))
                      (loop for j from 0 below len
                            collect (setf (aref str j) (code-char (aref bytes (incf idx)))))
                      (incf idx)
                      str)))))


(defun read-string-column (bytes)
  "return string's column"
  (mapcar (lambda (x) (apply #'mkstr (coerce x 'list))) (read-byte-array bytes)))



(defun read-columns-bytes (filename)
  "return bytes vectors for columns"
  (loop for x in (get-column-chunk-metadata filename)
        collect (let ((type (column-metadata-type x))
                      (num-rows (column-metadata-num-values x))
                      (codec (column-metadata-codec x))
                      (compressed-size (page-header-compressed-page-size
                                        (read-page-header filename (get-offset-page x))))
                      (start-offset (page-header-data-offset
                                     (read-page-header filename (get-offset-page x)))))
                  (format t "DATA TYPE : ~A. ~%DATA CODEC : ~A~%" (data-type type) (compression-codec-type codec))
                  (case (data-type type)
                    (BOOLEAN (format t "BOOLEAN") (read-bytes-between filename start-offset compressed-size))
                    (INT32 (format t "INT32") (read-bytes-between filename start-offset compressed-size))
                    (INT64 (format t "INT64") (read-bytes-between filename start-offset compressed-size))
                    (INT96 (format t "INT96") (read-bytes-between filename start-offset compressed-size))
                    (FLOAT (format t "FLOAT") (read-bytes-between filename start-offset compressed-size))
                    (DOUBLE (format t "DOUBLE") (read-bytes-between filename start-offset compressed-size))
                    (BYTE_ARRAY (format t "BYTE_ARRAY") (read-bytes-between filename start-offset compressed-size))
                    (FIXED_LEN_BYTE_ARRAY (format t "FIXED_LEN_BYTE_ARRAY") (read-bytes-between filename start-offset compressed-size))))))


(defun read-columns-vector (filename)
  "return column vectors"
  (loop for x in (get-column-chunk-metadata filename)
        collect (let ((type (column-metadata-type x))
                      (num-rows (column-metadata-num-values x))
                      (codec (column-metadata-codec x))
                      (compressed-size (page-header-compressed-page-size
                                        (read-page-header filename (get-offset-page x))))
                      (start-offset (page-header-data-offset
                                     (read-page-header filename (get-offset-page x)))))
                  (format t "DATA TYPE : ~A. ~%DATA CODEC : ~A~%" (data-type type) (compression-codec-type codec))
                  (case (compression-codec-type codec)
                    (UNCOMPRESSED (format t "UNCOMPRESSED ==> "))
                    ;; NOT YET CODE
                    (SNAPPY (format t "SNAPPY ==>")
                     (case (data-type type)
                       (BOOLEAN (format t "BOOLEAN") (read-bytes-between filename start-offset compressed-size))
                       (INT32 (format t "INT32")
                        ;; XXX - TODO DUPLICATED CODES (LATER REFACTORING?)
                        (let ((bytes (read-bytes-between filename start-offset compressed-size)))
                          ;; with-binary-input-stream should use simple-vector
                          (with-binary-input-stream (in (coerce (uncompress bytes 0 (length bytes)) 'simple-vector))
                            (loop repeat num-rows
                                  collect (read-u32 in)))))
                       (INT64 (format t "INT64")
                        ;; XXX - TODO DUPLICATED CODES (LATER REFACTORING?)
                        (let ((bytes (read-bytes-between filename start-offset compressed-size)))
                          ;; with-binary-input-stream should use simple-vector
                          (with-binary-input-stream (in (coerce (uncompress bytes 0 (length bytes)) 'simple-vector))
                            (loop repeat num-rows
                                  collect (read-u64 in)))))
                       (INT96 (format t "INT96") (read-bytes-between filename start-offset compressed-size))
                       (FLOAT (format t "FLOAT") (read-bytes-between filename start-offset compressed-size))
                       (DOUBLE (format t "DOUBLE") (read-bytes-between filename start-offset compressed-size))
                       (BYTE_ARRAY (format t "BYTE_ARRAY")
                        (let ((bytes (read-bytes-between filename start-offset compressed-size)))
                          (uncompress bytes 0 (length bytes))))
                       (FIXED_LEN_BYTE_ARRAY (format t "FIXED_LEN_BYTE_ARRAY") (read-bytes-between filename start-offset compressed-size))))
                    (GZIP nil)
                    ;; NOT YET CODE
                    (LZO nil)
                    ;; NOT YET CODE
                    (BROTLI nil)
                    ;; NOT YET CODE
                    (LZ4 nil)
                    ;; NOT YET CODE
                    (ZSTD nil)))))



(defparameter col0 (nth 0 (read-columns-vector "./tests/tpch/region.parquet")))
(defparameter col1 (nth 1 (read-columns-vector "./tests/tpch/region.parquet")))
(defparameter col2 (nth 2 (read-columns-vector "./tests/tpch/region.parquet")))


;;; return string arrays 
(read-string-column col1)
(read-string-column col0)



(defun foo ()
  "Test"
  4)
