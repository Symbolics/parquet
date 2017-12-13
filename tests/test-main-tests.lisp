;;; (cl-parquet-reader-tests:run-tests)

(in-package #:parquet-tests)

(def-suite all-tests)

(in-suite all-tests)

(test dummy-more-tests
  "this is another tests"
  (is (= 2 3)))

(test dummy-tests
  "this is just a placeholder"
  (is (listp (list 1 2 3)))
  (is (= 5 (+ 3 2)))
  (is (= (length (list 1 2)) (length (list 1 2)))))


(defun run-tests()
  (run! 'all-tests))


(run-tests)
