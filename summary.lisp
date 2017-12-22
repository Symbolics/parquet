;;;; -*- Mode: LISP; Base: 10; Syntax: ANSI-Common-lisp; Package: parquet -*-

(in-package "PARQUET")

(defun sample-mean (the-seq &key (start 0) (end (length the-seq)))
  "return a means from start to end"
  (/ (reduce #'+ the-seq :start start :end end) (- end start)))

(defun max-item-from (the-seq)
  "return max item amont THE-SEQ"
  (apply  #'max (coerce the-seq 'list)))

(defun min-item-from (the-seq)
  "return max item amont THE-SEQ"
  (apply  #'min (coerce the-seq 'list)))

;;(sort #(0 1 3 3 2 1 2 3) #'<)

(defun quantile-of-ordered-seq (the-seq p)
  "given THE-SEQ or ordered SEQ (from smallest to largest)
   and p =  percentile; 0 <= p <= 1 " 
  (assert (and (plusp (length the-seq)) (<= 0.0 p 1.0)))
  (let* ((n (length the-seq))
         (p-sub-0 (/ 0.5 n))
         (p-sub-n-1 (- 1.0 (/ 0.5 n))))
    (cond
     ((<= p p-sub-0)
      (elt the-seq 0))
     ((>= p p-sub-n-1)
      (elt the-seq (1- n)))
     (t
      (let* ((i (max 0 (truncate (- (* n p) 0.5))))
             (p-sub-i (/ (+ i 0.5) n))
             (i+1 (min (1+ i) (1- n)))
             (p-sub-i+1 (/ (+ i+1 0.5) n))
             (delta-p (- p-sub-i+1 p-sub-i))
             (f (if (zerop delta-p)
                  0.0
                  (/ (- p p-sub-i) delta-p))))
        (+ (* (- 1.0 f) (elt the-seq i))
           (* f (elt the-seq i+1))))))))

