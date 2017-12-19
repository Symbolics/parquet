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
  (data_page_header_v2))


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
  (format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
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
  (format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
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
  (format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field))
     (format t "end of page-header : ~A ~%" (file-position s))
     ;; FROM HERE DATA BYTES
     )
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

;; (defparameter resion-key (snappy::make-octet-vector 22))

;; (with-open-file (s "./tests/tpch/region.parquet" :element-type '(unsigned-byte 8))
;;     (file-position s 21)
;;     (loop for i from 0 to 21
;;           do (setf (aref resion-key i) (read-byte s nil nil))))

;; (defparameter r-ky (make-array 22 :element-type '(unsigned-byte 8)))
;; (with-open-file (s "./tests/tpch/region.parquet" :element-type '(unsigned-byte 8))
;;   (file-position s 21)
;;   (loop for i from 0 to 21
;;         do (setf (aref r-ky i) (read-byte s nil nil))))

;; ;; resion-key
;; (type-of resion-key)
;; (type-of r-ky)
;; (uncompress resion-key 0 (length resion-key))
;; (uncompress r-ky 0 (length r-ky))
;; ;; read-u32
;; t

;; (defparameter tmpe-octet (snappy::make-octet-vector 52))

;; (with-open-file (s "./tests/tpch/region.parquet" :element-type '(unsigned-byte 8))
;;    (file-position s 60)
;;    (loop for i from 0 to (- 52 1)
;;          do (setf (aref tmpe-octet i) (read-byte s nil nil))))

;; (snappy::uncompress tmpe-octet 0 (snappy::length tmpe-octet))
;; (snappy::utf8-octets-to-string (snappy::uncompress tmpe-octet 0 (snappy::length tmpe-octet)))


;; (defparameter tmpe-octet (snappy::make-octet-vector 285))

;; (with-open-file (s "./tests/tpch/region.parquet" :element-type '(unsigned-byte 8))
;;   (file-position s 131)
;;   (loop for i from 0 to (- 285 1)
;;         do (setf (aref tmpe-octet i) (read-byte s nil nil))))
;; (format t "~A ~%"
;;         (snappy::utf8-octets-to-string (snappy::uncompress tmpe-octet 0 (snappy::length tmpe-octet))))


;; ;;; TEST part.parquet

;; (print (read-page-header "./tests/tpch/part.parquet" 4))
;; ;; (+ 24 8002)

;; (print (read-page-header "./tests/tpch/part.parquet" 8026))
;; ;; (+ 8048 27683)

;; (print (read-page-header "./tests/tpch/part.parquet" 35731))
;; ;; (+ 35752 4814)

;; (print (read-page-header "./tests/tpch/part.parquet" 40566))
;; ;; (+ 40587 5541)

;; (print (read-page-header "./tests/tpch/part.parquet" 46128))
;; ;; (+ 46150 13257)

;; (print (read-page-header "./tests/tpch/part.parquet" 59407))
;; ;; (+ 59427 3997)

;; (print (read-page-header "./tests/tpch/part.parquet" 63424))
;; ;; (+ 63445 7054)

;; (print (read-page-header "./tests/tpch/part.parquet" 70499))
;; ;; (+ 70520 5091)

;; (print (read-page-header "./tests/tpch/part.parquet" 75611))
;; ;; (+ 75633 17703)


;; (ql:quickload "snappy")

;; (defparameter p-partkey (snappy::make-octet-vector 8002))
;; (with-open-file (s "./tests/tpch/part.parquet" :element-type '(unsigned-byte 8))
;;   (file-position s 24)
;;   (loop for i from 0 to (- 8002 1)
;;         do (setf (aref p-partkey i) (read-byte s nil nil))))

;; (snappy::uncompress p-partkey 0 (snappy::length p-partkey))

;; (defparameter p-name (snappy::make-octet-vector 27683))
;; (with-open-file (s "./tests/tpch/part.parquet" :element-type '(unsigned-byte 8))
;;   (file-position s 8048)
;;   (loop for i from 0 to (- 27683 1)
;;         do (setf (aref p-name i) (read-byte s nil nil))))

;; (snappy::utf8-octets-to-string (snappy::uncompress p-name 0 (snappy::length p-name)))


;; (defparameter p-size (snappy::make-octet-vector 3997))
;; (with-open-file (s "./tests/tpch/part.parquet" :element-type '(unsigned-byte 8))
;;   (file-position s 59427)
;;   (loop for i from 0 to (- 3997 1)
;;         do (setf (aref p-size i) (read-byte s nil nil))))

;; (snappy::uncompress p-size 0 (snappy::length p-size))
