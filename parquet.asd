;;;; cl-parquet-reader.asd

(asdf:defsystem "parquet"
  :description "reader for parquet file"
  :author "Inchul <ijung@mapr.com>"
  :license "BSD"
  :serial t
  :in-order-to ((test-op (test-op "parquet/tests")))
  :components ((:file "package")
               (:file "parquet-reader")))


(asdf:defsystem "parquet/tests"
  :description "test reader-parquet"
  :author "Inchul <ijung@mapr.com>"
  :license "BSD"
  :depends-on ("fiveam" "parquet")
  :components ((:module "tests"
                :serial t
                :components ((:file "test-package")
                             (:file "test-main-tests")))))
;;  :perform (test-op (o s)
;;                    (uiop:symbol-call :fiveam :run! 'parquet-tests:all-tests)))

