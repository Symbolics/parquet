;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet-tests -*-

(in-package "PARQUET-TESTS")

(def-suite all-tests
    :description "The master suite of all parquet tests.")
(in-suite all-tests)

(defun test-parquet ()
  (run! 'all-tests))

(test dummy-more-tests
  :description "this is another tests"
  (is-false (= 2 3) "2 is not equal to 3"))

(test dummy-tests
  :description "this is just a placeholder"
  (is (listp (list 1 2 3)) "Another error message for this test.")
  (is (= 5 (+ 3 2)))
  (is (= (length (list 1 2)) (length (list 1 2))))
  (is (= 4 (foo)) "Foo should return 4, but returned ~A." (foo)))

;; +--------------+--------------+-----------------------+
;; | R_REGIONKEY  |    R_NAME    |       R_COMMENT       |
;; +--------------+--------------+-----------------------+
;; | 0            | AFRICA       | lar deposits. blithe  |
;; | 1            | AMERICA      | hs use ironic, even   |
;; | 2            | ASIA         | ges. thinly even pin  |
;; | 3            | EUROPE       | ly final courts cajo  |
;; | 4            | MIDDLE EAST  | uickly special accou  |
;; +--------------+--------------+-----------------------+

(test parquet-file-tests
  :description "some tests for parquet format"
  (is (parquet::magic-number? "tests/tpch/region.parquet") "This file has magic-number")
  (is (= 193 (parquet::footer-length "tests/tpch/region.parquet")) "it has a footer of 193 length, but ~A." (parquet::footer-length "tests/tpch/region.parquet"))
  (is (= 5 (parquet::file-meta-data-num-rows (parquet::read-file-meta-data "tests/tpch/region.parquet"))) "REGION.PARQUET has 5 rownum, but ~A." (parquet::file-meta-data-num-rows (parquet::read-file-meta-data "tests/tpch/region.parquet")))
  (is (= 3 (length (parquet::get-column-chunk-metadata "tests/tpch/region.parquet"))) "REGION.PARQUET has 3 columns, but ~A." (parquet::get-column-chunk-metadata "tests/tpch/region.parquet"))
  (is (= 22 (parquet::page-header-compressed-page-size (parquet::read-page-header "./tests/tpch/region.parquet" 4))))
  (is (equal '("AFRICA" "AMERICA" "ASIA" "EUROPE" "MIDDLE EAST") (second (read-columns-vector "./tests/tpch/region.parquet"))))
  (is (equal '(0 1 2 3 4) (third (read-columns-vector "./tests/tpch/region.parquet")))))

(test thrift-tests
  :description "compact protocol"
  (is (equal '(0 -1 1 -2 2147483647 -2147483648) (mapcar #'parquet::zigzag-to-int '(0 1 2 3 4294967294 4294967295)))))


