;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")


;;;; Structures and functions for reading page-header, data-page-header, index-page-header and dictionary-page-header.

;; (defstruct page-header ...
(defstruct page-header
  (type) ; PageType
  (uncompressed-page-size 0 :type integer) ; i32
  (compressed-page-size 0 :type integer) ; i32
  (crc 0 :type integer) ; i32
  (data-page-header) ; DataPageHeader
  (index-page-header) ; IndexPageHeader
  (dictionary-page-header) ; DictionaryPageHeader
  )


;; (defstruct data-page-header ...
(defstruct data-page-header
  (num-values 0 :type integer) ;i32
  (encoding) ; Encoding
  (definition-level-encoding) ; Encoding
  (repetition-level-encoding) ; Encoding
  )

;; index page header
(defstruct index-page-header)

(defstruct dictionary-page-header
  (num-values 0 :type integer) ; i32
  )
