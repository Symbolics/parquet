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

(print (get-column-chunk-metadata "./tests/tpch/region.parquet"))


;;(defn get-start-offsets (metadata)
;;  "return each column offsets of COLUMN-METADATA"
;;  )
;;(mapcar (lambda (x) column-metadata-)(get-column-chunk-metadata "./tests/tpch/region.parquet")


;; (#S(COLUMN-METADATA :TYPE 6 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("r_comment") :CODEC 1 :NUM-VALUES 5 :TOTAL-UNCOMPRESSED-SIZE 369 :TOTAL-COMPRESSED-SIZE 304 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 112 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;;    #S(COLUMN-METADATA :TYPE 6 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("r_name") :CODEC 1 :NUM-VALUES 5 :TOTAL-UNCOMPRESSED-SIZE 71 :TOTAL-COMPRESSED-SIZE 69 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 43 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;;    #S(COLUMN-METADATA :TYPE 1 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("r_regionkey") :CODEC 1 :NUM-VALUES 5 :TOTAL-UNCOMPRESSED-SIZE 37 :TOTAL-COMPRESSED-SIZE 39 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 4 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL))



;; (defun print-file-metadata (file-metadata struct...
;;   "Prints the file-metadata structure using pretty printing. See CLtL2 section on pretty printing to get nice tables."

(defun foo ()
  "Test"
  4)


;;(get-column-chunk-metadata "./tests/tpch/part.parquet")

;; (#S(COLUMN-METADATA :TYPE 6 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("p_comment") :CODEC 1 :NUM-VALUES 2000 :TOTAL-UNCOMPRESSED-SIZE 35195 :TOTAL-COMPRESSED-SIZE 17725 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 75611 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;; #S(COLUMN-METADATA :TYPE 5 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("p_retailprice") :CODEC 1 :NUM-VALUES 2000 :TOTAL-UNCOMPRESSED-SIZE 16021 :TOTAL-COMPRESSED-SIZE 5112 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 70499 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;; #S(COLUMN-METADATA :TYPE 6 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("p_container") :CODEC 1 :NUM-VALUES 2000 :TOTAL-UNCOMPRESSED-SIZE 23242 :TOTAL-COMPRESSED-SIZE 7075 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 63424 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;; #S(COLUMN-METADATA :TYPE 1 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("p_size") :CODEC 1 :NUM-VALUES 2000 :TOTAL-UNCOMPRESSED-SIZE 8020 :TOTAL-COMPRESSED-SIZE 4017 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 59407 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;; #S(COLUMN-METADATA :TYPE 6 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("p_type") :CODEC 1 :NUM-VALUES 2000 :TOTAL-UNCOMPRESSED-SIZE 49124 :TOTAL-COMPRESSED-SIZE 13279 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 46128 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;; #S(COLUMN-METADATA :TYPE 6 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("p_brand") :CODEC 1 :NUM-VALUES 2000 :TOTAL-UNCOMPRESSED-SIZE 24021 :TOTAL-COMPRESSED-SIZE 5562 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 40566 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;; #S(COLUMN-METADATA :TYPE 6 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("p_mfgr") :CODEC 1 :NUM-VALUES 2000 :TOTAL-UNCOMPRESSED-SIZE 36021 :TOTAL-COMPRESSED-SIZE 4835 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 35731 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;; #S(COLUMN-METADATA :TYPE 6 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("p_name") :CODEC 1 :NUM-VALUES 2000 :TOTAL-UNCOMPRESSED-SIZE 74276 :TOTAL-COMPRESSED-SIZE 27705 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 8026 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)

;; #S(COLUMN-METADATA :TYPE 1 :ENCODINGS (4 0) :PATH-IN-SCHEMA ("p_partkey") :CODEC 1 :NUM-VALUES 2000 :TOTAL-UNCOMPRESSED-SIZE 8020 :TOTAL-COMPRESSED-SIZE 8022 :KEY-VALUE-METADATA NIL :DATA-PAGE-OFFSET 4 :INDEX-PAGE-OFFSET NIL :DICTIONARY-PAGE-OFFSET NIL :STATISTICS NIL :ENCODING_STATS NIL)) 
