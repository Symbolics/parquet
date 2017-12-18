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

(defun mkstr (&rest args)
  "make string with char elements"
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

;;; with byte arry 
;; (defun var-ints (bytes &optional (results 0) (depth 0))
;;   "serializing integers. ref: https://developers.google.com/protocol-buffers/docs/encoding#varints
;;    ex) 1010 1100 0000 0010 => #b0101100 #b0000010 => 000 0010 ++ 010 1100 => 256 + 32 + 8 + 4 = 300"
;;   (if (not (equal #b10000000 (logand #b10000000 (first bytes))))
;;       (logior results (ash (logand #b01111111 (first bytes)) (* 7 depth)))
;;       (if (= depth 0)
;;           (var-ints (cdr bytes) (logior (logand #b01111111 (first bytes)) results) (+ 1 depth))
;;           (var-ints (cdr bytes) (logior (ash (logand #b01111111 (first bytes)) (* 7 depth)) results) (+ 1 depth)))))

(defun var-ints (s &optional (results 0) (depth 0))
  "serializing integers from stream. ref: https://developers.google.com/protocol-buffers/docs/encoding#varints
   ex) 1010 1100 0000 0010 => #b0101100 #b0000010 => 000 0010 ++ 010 1100 => 256 + 32 + 8 + 4 = 300"
  (let ((byte (read-byte s nil nil)))
    (if (not (equal #b10000000 (logand #b10000000 byte)))
        (logior results (ash (logand #b01111111 byte) (* 7 depth)))
      (if (= depth 0)
          (var-ints s (logior (logand #b01111111 byte) results) (+ 1 depth))
          (var-ints s (logior (ash (logand #b01111111 byte) (* 7 depth)) results) (+ 1 depth))))))
