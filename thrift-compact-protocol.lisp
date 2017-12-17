;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;; type definition

(deftype int32 ()
  "32bit"
  '(unsigned-byte 32))


;; def intToZigZag(n: Int): Int = (n << 1) ^ (n >> 31)
;; def zigzagToInt(n: Int): Int = (n >>> 1) ^ - (n & 1)
;; def longToZigZag(n: Long): Long = (n << 1) ^ (n >> 63)
;; def zigzagToLong(n: Long): Long = (n >>> 1) ^ - (n & 1)

(defun zigzag-to-int (n)
  "maps signed integers to unsigned integers so that numbers with a small absolute value"
  (logxor (ash n -1) (- (logand n 1))))

(defstruct field
  "id and type in one bytes"
  (id 0)
  (type nil))

(defparameter *field-info* (make-field))

(defun get-id-type (in f)
  "update FIELD (ID, TYPE) struct from one byte, #bIIIITTTT"
  (let ((byte (read-byte in)))
    (setf (field-type f) (logand byte #x0f)
          (field-id f) (+ (field-id f) (ash (logand byte #xf0) -4)))))


(defun list-len (in)
  "length of list from byte"
  (let* ((byte (read-byte in nil))
         (len (logand (ash byte -4) #x0f)))
    (if (<= 15 len) (var-ints in)
        len)))

(defun char-list (in)
  "return list of chars from bytes"
  (let ((len (var-ints in)))
    (loop for i from 1 to len
          collect (code-char (read-byte in nil)))))

;;; structure encoding
;; BOOLEAN_TRUE, encoded as 1
;; BOOLEAN_FALSE, encoded as 2
;; BYTE, encoded as 3
;; I16, encoded as 4
;; I32, encoded as 5
;; I64, encoded as 6
;; DOUBLE, encoded as 7
;; BINARY, used for binary and string fields, encoded as 8
;; LIST, encoded as 9
;; SET, encoded as 10
;; MAP, encoded as 11
;; STRUCT, used for both structs and union fields, encoded as 12
;; Note that because there are 2 specific field types for the boolean values, the encoding of a boolean field value has no length (0 bytes).


;;; List and Set

;; BOOL, encoded as 2
;; BYTE, encoded as 3
;; DOUBLE, encoded as 4
;; I16, encoded as 6
;; I32, encoded as 8
;; I64, encoded as 10
;; STRING, used for binary and string fields, encoded as 11
;; STRUCT, used for structs and union fields, encoded as 12
;; MAP, encoded as 13
;; SET, encoded as 14
;; LIST, encoded as 15
