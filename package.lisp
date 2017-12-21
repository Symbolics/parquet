;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(defpackage "PARQUET"
  (:nicknames "PRQT")
  (:use "CL" "COM.GOOGLE.BASE" "SNAPPY" "CL-BINARY")
  (:export #:foo
           #:magic-number?
           #:read-columns-vector
           #:read-columns-bytes))

