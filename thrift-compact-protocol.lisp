;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

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

;; (defun mkstr (&rest args)
;;   "make string with char elements"
;;   (with-output-to-string (s)
;;     (dolist (a args)
;;       (princ a s))))


(defun var-ints (s &optional (results 0) (depth 0))
  "serializing integers from stream. ref: https://developers.google.com/protocol-buffers/docs/encoding#varints
   ex) 1010 1100 0000 0010 => #b0101100 #b0000010 => 000 0010 ++ 010 1100 => 256 + 32 + 8 + 4 = 300"
  (let ((byte (read-byte s nil nil)))
    (if (not (equal #b10000000 (logand #b10000000 byte)))
        (logior results (ash (logand #b01111111 byte) (* 7 depth)))
      (if (= depth 0)
          (var-ints s (logior (logand #b01111111 byte) results) (+ 1 depth))
          (var-ints s (logior (ash (logand #b01111111 byte) (* 7 depth)) results) (+ 1 depth))))))


(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun mkstr (&rest args)
    (with-output-to-string (s)
      (dolist (a args)
        (princ a s))))

  (defun symb (&rest args)
    (values (intern (apply #'mkstr args)))))


(defclass binary-input-stream (fundamental-binary-input-stream)
  ((data :initarg :data :type '(vector (unsigned-byte 8) (*)))
   (index :initarg :index)
   (end :initarg :end)))

(defun make-binary-input-stream (data)
  (make-instance 'binary-input-stream :data data :index 0 :end (length data)))

(defmethod stream-read-byte ((stream fundamental-binary-input-stream))
  (with-slots (data index end) stream
    (if (>= index end)
        :eof
        (prog1 (svref data index)
          (incf index)))))

(defmacro with-binary-input-stream ((stream vector) &body body)
  `(let ((,stream (make-binary-input-stream ,vector)))
     (unwind-protect
          (progn ,@body)
       (close ,stream))))

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

(defmacro def-signed (name unit place &optional val &key (direction :input))
  (let ((sname (symb name "-S" unit))
        (uname (symb name "-U" unit)))
    (case direction
      (:input
       `(defun ,sname (,@place)
          (u-to-s (,uname ,@place) ,unit)))
      (:output
       `(defun ,sname (,@place ,val)
          (,uname ,@place (s-to-u ,val ,unit))))
      (t (error "Unknown direction: ~a~%" direction)))))

;;; read-u8
;;; read-u16
;;; read-u32
;;; read-u64
(defmacro def-read-u* (unit)
  `(defun ,(symb "READ-U" unit) (stream)
     ,(if (<= unit 8)
          `(read-byte stream nil nil)
          (let ((b (gensym)))
            `(let ((,b (read-byte stream nil nil)))
               (when ,b
                 (logior
                  ,b
                  ,@(loop for i from 8 below unit by 8
                       collect `(ash (read-byte stream nil 0) ,i)))))))))

(def-read-u* 8)
(def-read-u* 16)
(def-read-u* 32)
(def-read-u* 64)

;;; read-s8
;;; read-s16
;;; read-s32
;;; read-s64
(def-signed read   8 (stream) nil :direction :input)
(def-signed read  16 (stream) nil :direction :input)
(def-signed read  32 (stream) nil :direction :input)
(def-signed read  64 (stream) nil :direction :input)

;;; read-f32
;;; read-f64
(defmacro def-read-f* (unit)
  `(defun ,(symb "READ-F" unit) (stream)
     (,(symb "DECODE-FLOAT" unit) (,(symb "READ-U" unit)  stream))))

(def-read-f* 32)
(def-read-f* 64)
