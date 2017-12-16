;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: CL-USER -*-

(asdf:defsystem "parquet"
  :description "reader for parquet file"
  :author "Inchul <ijung@mapr.com>"
  :license "BSD"
  :serial t
  :in-order-to ((test-op (test-op "parquet/tests")))
  :components ((:file "package")
               (:file "type")
               (:file "file")
               (:file "row")
               (:file "column")
               (:file "page")
               (:file "parquet-reader")))

(asdf:defsystem "parquet/tests"
  :description "test reader-parquet"
  :author "Inchul <ijung@mapr.com>"
  :license "BSD"
  :depends-on ("parquet"
               "fiveam")
  :components ((:module "tests"
                :serial t
                :components ((:file "test-package")
                             (:file "main"))))
  :perform (asdf:test-op (o s)
			 (uiop:symbol-call :fiveam '#:run!
					   (uiop:find-symbol* :all-tests
							      :parquet-tests))))




