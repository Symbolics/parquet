;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(defpackage "PARQUET"
  (:nicknames "PRQT")
  (:use "CL" "SNAPPY" "CL-BINARY")
  (:export #:foo
           #:magic-number?))

