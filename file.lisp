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
  (created-by nil :type string)
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
  (name nil :type string)
  (num-children 0 :type i32)
  (converted-type nil) ;ConvertedType
  (scale nil :type i32)
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

(footer-length "./tests/tpch/customer.parquet")


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


;;(defun read-file-metadata (filename)
;;  "Returns a structure of type file-metadata with the values from FILENAME. Returns an error if FILENAME is not a parquet file."
;;)


(defun foo ()
  "Test"
  4)
