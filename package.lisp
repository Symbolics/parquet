;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(eval-when (:compile-toplevel :load-toplevel :execute)
  (ql:quickload "trivial-gray-streams")
  (ql:quickload "ieee-floats"))

(defpackage "PARQUET"
  (:nicknames "PRQT")
  (:use "CL" "COM.GOOGLE.BASE" "SNAPPY" "IEEE-FLOATS" "TRIVIAL-GRAY-STREAMS")
  (:export ;;#:foo
           ;;#:magic-number?
           #:read-columns-vector
           #:read-columns-bytes
           #:load-parquet
           #:get-column-names
           #:get-num-cols
           #:get-num-rows
           #:get-data-by-name
           #:get-data
           #:show-data
           #:sample-mean
           #:min-item-from
           #:max-item-from
           #:quantile-of-ordered-seq))
