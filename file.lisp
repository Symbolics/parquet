;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

;;;; Read file meta-data. This might look like this

;; (defstruct file-metadata ...
;; (defstruct schema-element ...
;; For key-value, just use a hashtable variable as a slot in file-metadata

;; (defun read-file-metadata (filename)
;;   "Returns a structure of type file-metadata with the values from FILENAME. Returns an error if FILENAME is not a parquet file."
;;   (with-open-file filename
;;     (...

;; (defun print-file-metadata (file-metadata struct...
;;   "Prints the file-metadata structure using pretty printing. See CLtL2 section on pretty printing to get nice tables."

(defun foo ()
  "Test"
  4)
