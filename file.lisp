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
  (created-by "" :type string)
  (column-orders nil))

;; schema-element
(defstruct schema-element
  "Represents a element inside a schema definition.
   - if it is a group (inner node) then type is undefined and num_children is defined
   - if it is a primitive type (leaf) then type is defined and num_children is undefined
  the nodes are listed in depth first traversal order."
  (type nil) ; parquet-type
  (type-length 0 :type i32)
  (repetition-type nil) ; FieldRepetition
  (name "" :type string)
  (num-children 0 :type i32)
  (converted-type nil) ;ConvertedType
  (scale 0 :type i32)
  (precision 0 :type i32)
  (field-id 0 :type i32)
  (logicalType nil))


;; key-value
;; For key-value, just use a hashtable variable as a slot in file-metadata
(setf key-value (make-hash-table))

;; (defun read-file-metadata (filename)
;;   "Returns a structure of type file-metadata with the values from FILENAME. Returns an error if FILENAME is not a parquet file."
;;   (with-open-file filename
;;     (...

;; (defun print-file-metadata (file-metadata struct...
;;   "Prints the file-metadata structure using pretty printing. See CLtL2 section on pretty printing to get nice tables."


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


;;(with-open-file (s "./tests/tpch/customer.parquet" :element-type '(unsigned-byte 8))
;;  (read-first-four-bytes s))

(defun magic-number? (filename)
  "check first/last 'PAR1'"
  (with-open-file (s filename :element-type '(unsigned-byte 8))
    (and (equal '(80 65 82 49) (read-first-four-bytes s))
         (equal '(80 65 82 49) (read-last-four-bytes s)))))

;; XXX : FIND OUT how to convert bytes-array into string (coerce '(80 65 82 49) 'string)

(defun read-u32 (in)
  "read 32 bits integer with litten endian"
  (let ((u4 0))
    (setf (ldb (byte 8 0) u4) (read-byte in))
    (setf (ldb (byte 8 8) u4) (read-byte in))
    (setf (ldb (byte 8 16) u4) (read-byte in))
    (setf (ldb (byte 8 24) u4) (read-byte in))
    (the int32 u4)))


;; (read-i32 ...)
(defun footer-length (filename)
  "length of footer. First, check a magic-number 'PAR1' and then get a footer length"
  (with-open-file (s filename :element-type '(unsigned-byte 8))
    (and (magic-number? filename)
       (file-position s (- (file-length s) 8))
       (read-u32 s))))

;;(footer-length "./tests/tpch/customer.parquet")


(defun mkstr (&rest args)
  (with-output-to-string (s)
    (dolist (a args)
      (princ a s))))

(defun u-to-s (number bit)
  "Convert an unsigned number to a signed number with `bit` length."
  (if (and (plusp number)
           (< number (ash 1 bit)))
      (if (plusp (logand number (ash 1 (1- bit))))
          (- number (ash 1 bit))
          number)
      (error "Out of bounds error (Number is beyond ~a bit)" bit)))

(defun s-to-u (number bit)
  "Convert a signed number to an unsigned number with `bit` length."
  (if (and (<= (- (ash 1 (1- bit))) number)
           (< number (ash 1 (1- bit))))
      (if (minusp number)
          (+ number (ash 1 bit))
          number)
      (error "Out of bounds error (Number is beyond ~a bit)" bit)))

(defun var-ints (bytes &optional (results 0) (depth 0))
  "serializing integers. ref: https://developers.google.com/protocol-buffers/docs/encoding#varints
   ex) 1010 1100 0000 0010 => #b0101100 #b0000010 => 000 0010 ++ 010 1100 => 256 + 32 + 8 + 4 = 300"
  (if (not (equal #b10000000 (logand #b10000000 (first bytes))))
      (logior results (ash (logand #b01111111 (first bytes)) (* 7 depth)))
      (if (= depth 0)
          (var-ints (cdr bytes) (logior (logand #b01111111 (first bytes)) results) (+ 1 depth))
          (var-ints (cdr bytes) (logior (ash (logand #b01111111 (first bytes)) (* 7 depth)) results) (+ 1 depth)))))

(defun var-ints (s &optional (results 0) (depth 0))
  "serializing integers from stream.
   ref: https://developers.google.com/protocol-buffers/docs/encoding#varints
   ex) 1010 1100 0000 0010 => #b0101100 #b0000010 => 000 0010 ++ 010 1100 => 256 + 32 + 8 + 4 = 300"
  (let ((byte (read-byte s nil nil)))
    (if (not (equal #b10000000 (logand #b10000000 byte)))
        (logior results (ash (logand #b01111111 byte) (* 7 depth)))
      (if (= depth 0)
          (var-ints s (logior (logand #b01111111 byte) results) (+ 1 depth))
          (var-ints s (logior (ash (logand #b01111111 bytes) (* 7 depth)) results) (+ 1 depth))))))


(defun extract-schema-element (schema field s)
  "parse SCHEMA-ELEMENT from byt streams"
  (get-id-type s field)
  (format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
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
  (format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field)) (format t "end of file-meta-data : ~A ~%" (file-position s)))
    ((= 1 (field-id field))
     ;; i32 version
     (setf (file-meta-data-version meta) (zigzag-to-int (var-ints s)))
     (extract-file-meta-data meta field s))
    ((= 2 (field-id field))
     ;; required list<SchemaElement>;
     (let ((len (list-len s)))
       (format t "list length : ~A ~%" len)
       (loop for i from 1 to len
             do (let ((schema (make-schema-element))
                      (field-info (make-field)))
                  ;; extract schema-element
                  (extract-schema-element schema field-info s)
                  (princ schema)
                  ))))
    (t (error "Sorry. Unsupported Field/Type of FileMetaData"))))



(defun read-file-meta-data (filename)
  "Returns a structure of type file-metadata with the values from FILENAME. Returns an error if FILENAME is not a parquet file."
  (with-open-file (s filename :element-type '(unsigned-byte 8))
    (format t "File-Meta-Data position is : ~A ~%" (file-position s (- (file-length s) (footer-length filename) 8)))
    (let ((fmd (make-file-meta-data))
          (field-info (make-field)))
      (extract-file-meta-data fmd field-info s))))

(read-file-meta-data "./tests/tpch/customer.parquet")


;; (defmethod filemeta-read ((meta FileMetaData) in offset)
;;   (let ((pro (make-instance 'Protocol))
;;         (byte 0))
;;     (file-position in offset)
;;     (loop while byte
;;           do (progn 
;;                (getfieldinfo pro in)
;;                ;;(print-obj pro)
;;                (cond
;;                  ((= 0 (type pro))
;;                   ;;(print-obj meta)
;;                   (return))
;;                  ((= 1 (fieldid pro))
;;                   ;; required i32 version
;;                   (setf (version meta) (zigzagtoi32 (varint32 in))))
;;                  ((= 2 (fieldid pro))
;;                   ;; required list<SchemaElement> schema;
;;                   ;; (setf byte (read-byte in nil))
;;                   ;; (format t "=> ~x ~%" byte)
;;                   (loop for i from 1 to (numberList in)
;;                         do (let ((se (make-instance 'SchemaElement)))
;;                              (SchemaElement-Read se in)
;;                              ;;(print-obj se)
;;                              (setf (schema meta) (cons se (schema meta)))))
;;                   ;;(format t "coutn ~a ~%" (numberList byte))
;;                   )
;;                  ((= 3 (fieldid pro))
;;                   ;; required i64 num_rows
;;                   (setf (num_rows meta) (zigzagtoi32 (varint64 in))))
;;                  ((= 4 (fieldid pro))
;;                   ;; required list<RowGroup> row_groups
;;                   ;;(setf byte (read-byte in nil))
;;                   ;;(format t "=> ~x ~%" byte)
;;                   (loop for i from 1 to (numberList in)
;;                         do (let ((rg (make-instance 'RowGroup)))
;;                              (RowGroup-Read rg in)
;;                              (setf (row_groups meta) (cons rg (row_groups meta)))))
;;                   ;;(format t "coutn ~a ~%" (numberList byte))
;;                   )
;;                  ((= 5 (fieldid pro))
;;                   ;; optional list<KeyValue> key_value_metadata
;;                   ;;(setf byte (read-byte in nil))
;;                   ;;(format t "=> ~x ~%" byte)
;;                   (loop for i from 1 to (numberList in)
;;                         do (let ((kv (make-instance 'KeyValue)))
;;                              (format t "KeyValue ~Ath~%" i)
;;                              (KeyValue-Read kv in)
;;                              (setf (key_value_metadata meta) (cons kv (key_value_metadata meta)))))
;;                   (format t "55555~%"))
;;                  ((= 6 (fieldid pro))
;;                   ;; optional string created_by
;;                   (setf (created_by meta) (list-to-string (read-string in))))
;;                  ((= 7 (fieldid pro))
;;                   ;; optional list<ColumnOrder> column_orders;
;;                   (format t "NOT DEFINED YET :: optional list<ColumnOrder> column_orders~%"))
;;                  (t (return)))))))


(defun foo ()
  "Test"
  4)
