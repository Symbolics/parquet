;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;;; Structures and functions to read a row-group.
(defstruct row-group
  "RoWGroup of Columns"
  (columns) ; list<ColumnChunk>
  ;; Total byte size of all the uncompressed column data in this row group
  (total-byte-size 0 :type int64)
  (num-rows 0 :type int64))


(defun extract-row-group (rowgroup field s)
  "parse ROW-GROUP from byte streams"
  (get-id-type s field)
  ;;(format t "field : ~A, type : ~A ~%" (field-id field) (field-type field))
  (cond
    ((= 0 (field-type field)) (format t "end of row-group : ~A ~%" (file-position s)))
    ((= 1 (field-id field))
     ;; list <ColumnChunk> cloumns
     (let ((len (list-len s)))
       ;;(format t "list length : ~A ~%" len)
       (loop for i from 1 to len
             do (let ((columnchunk (make-column-chunk))
                      (field-info (make-field)))
                  ;; extract column-chunk
                  (extract-column-chunk columnchunk field-info s)
                  (setf (row-group-columns rowgroup) (push columnchunk (row-group-columns rowgroup))))))
     (extract-row-group rowgroup field s))
    ((= 2 (field-id field))
     ;; required i64 total-byte-size
     (setf (row-group-total-byte-size rowgroup) (zigzag-to-int (var-ints s)))
     (extract-row-group rowgroup field s))
    ((= 3 (field-id field))
     ;; required i64 num-rows
     (setf (row-group-num-rows rowgroup) (zigzag-to-int (var-ints s)))
     (extract-row-group rowgroup field s))
    ((= 4 (field-id field))
     ;; list <SortingColumn> sorting-columns
     (error "Sorry, Not Yet Implemented SoringColumn"))
    (t (error "Sorry. Unsupported Field/Type of FileMetaData"))))
