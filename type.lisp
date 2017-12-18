;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;; type definition
(deftype int32 () '(unsigned-byte 32))
(deftype int64 () '(unsigned-byte 64))


;; TYPE is already defined, so with parquet-type
(deftype parquet-type () '(member :boolean :int32 :int64 :int96 :int :float :double :byte-array :fixed-length-byte-array))

(deftype converted-type () '(member :utf8 :map :map-key-value :list))

(deftype field-repetition-type () '(member :required :optional :repeated))

(deftype encoding () '(member :plain :group-var-int :plain-dictionary :rle :bit-packed))

(deftype compression-codec () '(member :uncompressed :snappy :gzip :lzo))

(deftype page-type () '(member :data-page :index-page))


;;; (typep :data-page 'page-type)
