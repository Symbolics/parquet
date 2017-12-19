;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")


;;;; Structures and functions for reading page-header, data-page-header, index-page-header and dictionary-page-header.

;; (defstruct page-header ...
(defstruct page-header
  (type) ; PageType
  (uncompressed-page-size 0 :type int32) ; i32
  (compressed-page-size 0 :type int32) ; i32
  (crc 0 :type int32) ; i32
  (data-page-header) ; DataPageHeader
  (index-page-header) ; IndexPageHeader
  (dictionary-page-header) ; DictionaryPageHeader
  (data_page_header_v2)
  ;; ADDED FOR ME
  (data-offset))

;; (defstruct data-page-header ...
(defstruct data-page-header
  "Data page header"
  (num-values 0 :type int32) ;Number of values, including NULLs, in this data pagNumber of values, including NULLs, in this data page.e.
  (encoding) ; Encoding
  (definition-level-encoding) ; Encoding
  (repetition-level-encoding) ; Encoding
  (statistics))

;; index page header
(defstruct index-page-header) ;; 

(defstruct dictionary-page-header
  (num-values 0 :type int32) ; i32
  (encoding)
  (is_sorted))



(defun extract-dictionary-page-header (dic-header field s)
  "parse DICTIONARY-PAGE-HEADER"
  (get-id-type s field)
  ;; (format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field))
     (format t "end of dictionary-page-header : ~A ~%" (file-position s)))
    ((= 1 (field-id field))
     ;; num_values
     (setf (dictionary-page-header-num-values dic-header) (zigzag-to-int (var-ints s)))
     (extract-dictionary-page-header dic-header field s))
    ((= 2 (field-id field))
     ;; encoding
     (setf (dictionary-page-header-encoding dic-header) (zigzag-to-int (var-ints s)))
     (extract-dictionary-page-header dic-header field s))
    ((= 3 (field-id field))
     ;; is-sorted
     (setf (dictionary-page-header-is_sorted dic-header) (field-type field))
     (extract-dictionary-page-header dic-header field s))
    (t (error "Sorry. Unsupported Field/Type of DictionaryPageHeader"))))


(defun extract-data-page-header (dp-header field s)
  "parse DATA-PAGE-HEADER"
  (get-id-type s field)
  ;; (format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field))
     (format t "end of data-page-header : ~A ~%" (file-position s)))
    ((= 1 (field-id field))
     ;; num_values
     (setf (data-page-header-num-values dp-header) (zigzag-to-int (var-ints s)))
     (extract-data-page-header dp-header field s))
    ((= 2 (field-id field))
     ;; encoding
     (setf (data-page-header-encoding dp-header) (zigzag-to-int (var-ints s)))
     (extract-data-page-header dp-header field s))
    ((= 3 (field-id field))
     ;; definition_level_encoding
     (setf (data-page-header-definition-level-encoding dp-header) (zigzag-to-int (var-ints s)))
     (extract-data-page-header dp-header field s))
    ((= 4 (field-id field))
     ;; repetition_level_encoding
     (setf (data-page-header-repetition-level-encoding dp-header) (zigzag-to-int (var-ints s)))
     (extract-data-page-header dp-header field s))
    ((= 5 (field-id field))
     ;; Statistics
     (error "Sorry, Not Yet Implemented Statics of data-page-header"))
    (t (error "Sorry. Unsupported Field/Type of DataPageHeader"))))


(defun extract-page-header (pageheader field s)
  "parse PAGE-HEADER"
  (get-id-type s field)
  ;; (format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field))
     (format t "end of page-header : ~A ~%" (file-position s))
     ;; FROM HERE DATA BYTES
     (setf (page-header-data-offset pageheader) (file-position s)))
    ((= 1 (field-id field))
     ;; PageType
     (setf (page-header-type pageheader) (zigzag-to-int (var-ints s)))
     (extract-page-header pageheader field s))
    ((= 2 (field-id field))
     ;; uncompressed-page-size
     (setf (page-header-uncompressed-page-size pageheader) (zigzag-to-int (var-ints s)))
     (extract-page-header pageheader field s))
    ((= 3 (field-id field))
     ;; compressed-page-size
     (setf (page-header-compressed-page-size pageheader) (zigzag-to-int (var-ints s)))
     (extract-page-header pageheader field s))
    ((= 4 (field-id field))
     ;; PageType
     (setf (page-header-crc pageheader) (zigzag-to-int (var-ints s)))
     (extract-page-header pageheader field s))
    ((= 5 (field-id field))
     ;; DataPageHeader
     (let ((dp-header (make-data-page-header))
           (field-info (make-field)))
       (extract-data-page-header dp-header field-info s)
       (setf (page-header-data-page-header pageheader) dp-header))
     (extract-page-header pageheader field s))
    ((= 6 (field-id field))
     ;; IndexPageHeader
     (error "Sorry. Not Yet Implemented the IndexPageHeader"))
    ((= 7 (field-id field))
     ;; dictionaryPageHeader
     (let ((dic-header (make-dictionary-page-header))
           (field-info (make-field)))
       (extract-dictionary-page-header dic-header field-info s)
       (setf (page-header-dictionary-page-header pageheader) dic-header))
     (extract-page-header pageheader field s))
    ((= 8 (field-id field))
     ;; DatagageHeaderv2
     (error "DataPageHeaderV2"))
    (t (error "Sorry. Unsupported Field/Type of PageHeader"))))

;;; functions for reading Parquet PageHeader structures.

(defun read-page-header (filename offset)
  "Returns a structure of type page-header from OFFSET. Returns an error if FILENAME is not a parquet file."
  (with-open-file (s filename :element-type '(unsigned-byte 8))
    (format t "PageHader start point is : ~A ~%" (file-position s offset))
    (let ((pageheader (make-page-header))
          (field-info (make-field)))
      (extract-page-header pageheader field-info s)
      pageheader)))



;;; TEST region.parquet file
(print (read-page-header "./tests/tpch/region.parquet" 4))
;; (+ 21 22)

(print (read-page-header "./tests/tpch/region.parquet" 43))
;; (+ 60 52)

(print (read-page-header "./tests/tpch/region.parquet" 112))
;; (+ 131 285)
