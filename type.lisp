;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;; type definition
(deftype int8 () "A signed 8-bit integer." '(signed-byte 8))
(deftype int16 () "A signed 16-bit integer." '(signed-byte 16))
(deftype int32 () "A signed 32-bit integer." '(signed-byte 32))
(deftype int64 () "A signed 64-bit integer." '(signed-byte 64))


(deftype uint8 () "An unsigned 8-bit integer." '(unsigned-byte 8))
(deftype uint16 () "An unsigned 16-bit integer." '(unsigned-byte 16))
(deftype uint32 () "An unsigned 32-bit integer." '(unsigned-byte 32))
(deftype uint64 () "An unsigned 64-bit integer." '(unsigned-byte 64))

;; TYPE is already defined, so with parquet-type
;; (deftype parquet-type () '(member :boolean :int32 :int64 :int96 :int :float :double :byte-array :fixed-length-byte-array))

;; (deftype converted-type () '(member :utf8 :map :map-key-value :list))

;; (deftype field-repetition-type () '(member :required :optional :repeated))

;; (deftype encoding () '(member :plain :group-var-int :plain-dictionary :rle :bit-packed))

;; (deftype compression-codec () '(member :uncompressed :snappy :gzip :lzo))

;; (deftype page-type () '(member :data-page :index-page))


;;; https://github.com/apache/parquet-format/blob/master/Encodings.md
;;; Default encoding.
;;;* BOOLEAN - 1 bit per value. 0 is false; 1 is true.
;;;* INT32 - 4 bytes per value.  Stored as little-endian.
;;;* INT64 - 8 bytes per value.  Stored as little-endian.
;;;* FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
;;;* DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
;;;* BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
;;;* FIXED_LEN_BYTE_ARRAY - Just the bytes.

;; enum Type {
;; BOOLEAN = 0;
;; INT32 = 1;
;; INT64 = 2;
;; INT96 = 3;
;; FLOAT = 4;
;; DOUBLE = 5;
;; BYTE_ARRAY = 6;
;; FIXED_LEN_BYTE_ARRAY = 7;
;; }

(defparameter *parquet-types*
  '((0 . BOOLEAN)
    (1 . INT32)
    (2 . INT64)
    (3 . INT96)
    (4 . FLOAT)
    (5 . DOUBLE)
    (6 . BYTE_ARRAY)
    (7 . FIXED_LEN_BYTE_ARRAY)))

(defmacro data-type (n)
  `(cdr (assoc ,n *parquet-types*)))


;; * Supported compression algorithms.
;; *
;; * Codecs added in 2.3.2 can be read by readers based on 2.3.2 and later.
;; * Codec support may vary between readers based on the format version and
;; * libraries available at runtime. Gzip, Snappy, and LZ4 codecs are
;; * widely available, while Zstd and Brotli require additional libraries.
;; */
;; enum CompressionCodec {
;; UNCOMPRESSED = 0;
;; SNAPPY = 1;
;; GZIP = 2;
;; LZO = 3;
;; BROTLI = 4; // Added in 2.3.2
;; LZ4 = 5;    // Added in 2.3.2
;; ZSTD = 6;   // Added in 2.3.2
;; }


(defparameter *compression-codec*
  '((0 . UNCOMPRESSED)
    (1 . SNAPPY)
    (2 . GZIP)
    (3 . LZO)
    (4 . BROTLI)
    (5 . LZ4)
    (6 . ZSTD)))

(defmacro compression-codec-type (n)
  `(cdr (assoc ,n *compression-codec*)))


