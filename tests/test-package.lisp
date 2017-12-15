;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet-tests -*-

(defpackage "PARQUET-TESTS"
  (:use "CL"
	"FIVEAM"
	"PARQUET")
  (:export #:run!
           #:run-tests
           #:all-tests))
