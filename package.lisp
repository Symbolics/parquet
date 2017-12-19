;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(defpackage "PARQUET"
  (:nicknames "PRQT")
  (:use "CL" "SNAPPY")
  (:export #:foo
           #:magic-number?))

