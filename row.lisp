;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;;; Structures and functions to read a row-group.
(defstruct row-group
  (columns) ; list<ColumnChunk>
  (total-byte-size 0 :type integer) ; i64
  (num-rows 0 :type integer) ; i64
  )
