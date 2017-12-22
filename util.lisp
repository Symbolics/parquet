;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;;"express a data parquet table structure of parquet data"
(defclass data-parquet-table()
  ((num-rows :accessor num-rows :initarg :num-rows)
   (column-names :accessor column-names :initform nil)
   (data :accessor data :initform (make-hash-table :test 'equal))))


(defmethod print-object ((the-data-table data-parquet-table) out)
  "print the-parquet-table instance"
  (with-slots (num-rows column-names data) the-data-table
    (format out "DIMENSIONS   : ~A X ~A ~%" num-rows (get-num-cols the-data-table))
    (format out "COLUMN-NAMES : ~A~%" column-names)
    (format out "DATA         : ~A~%" data)))

(defmethod insert-data ((the-data-table data-parquet-table) the-column-name the-data)
  "insert the data with the column-name"
  (setf (column-names the-data-table) (cons the-column-name (column-names the-data-table)))
  (setf (gethash the-column-name (data the-data-table)) the-data))

(defmethod get-column-names ((the-data-table data-parquet-table))
  "return column names"
  (column-names the-data-table))

(defmethod get-num-cols ((the-data-table data-parquet-table))
  "return num col"
  (length (column-names the-data-table)))

(defmethod get-data ((the-data-table data-parquet-table))
  "return HASH-TABLE of data"
  (data the-data-table))

(defmethod get-data-by-name ((the-data-table data-parquet-table) the-name)
  "return only dataset of the name"
  (gethash the-name (data the-data-table)))

(defmethod get-num-rows ((the-data-table data-parquet-table))
  "return num-rows"
  (num-rows the-data-table))

(defmethod show-data ((the-data-table data-parquet-table) &key (limit 10))
  "show data by limit"
  (when (> limit (get-num-rows the-data-table))
    (setf limit (get-num-rows the-data-table)))
  (format t "~%~%========== SHOW DATA ==================================================~%")
  (format t "~A" the-data-table)
  (format t "============== CONTENT ================================================~%")
  (mapcar (lambda (x) (format t "~A => ~A ~%" x (subseq (gethash x (data the-data-table)) 0 limit)))
          (get-column-names the-data-table))
  (format t "=======================================================================~%"))


(defun load-parquet (filename)
  "load the parquet file and return DATA-PARQUET-TABLE"
  (let* ((file-metadata (read-file-meta-data filename))
         (column-names (mapcar #'schema-element-name (file-meta-data-schema-element file-metadata)))
         (column-vectors (read-columns-vector filename))
         (dt (make-instance 'data-parquet-table :num-rows (file-meta-data-num-rows file-metadata))))
    (loop for col in column-names
          for data in column-vectors
          do (insert-data dt col data))
    dt))


;;; example of usages
;(print (load-parquet "./tests/tpch/region.parquet"))

;(get-column-names (load-parquet "./tests/tpch/region.parquet"))
;(get-num-cols (load-parquet "./tests/tpch/region.parquet"))
;(get-num-rows (load-parquet "./tests/tpch/region.parquet"))

;(get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey")
;;(get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_comment")

;(get-data (load-parquet "./tests/tpch/region.parquet"))

;(show-data (load-parquet "./tests/tpch/region.parquet"))
;(show-data (load-parquet "./tests/tpch/part.parquet") :limit 9)

;(sample-mean (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey"))
;(min-item-from (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey"))
;(max-item-from (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey"))

;(quantile-of-ordered-seq (sort (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey") #'<) 0.25)
;(quantile-of-ordered-seq (sort (get-data-by-name (load-parquet "./tests/tpch/region.parquet") "r_regionkey") #'<) 0.75)
