(ns net-eval.core-test
 (:use clojure.test
       slingshot.test
       net-eval.core))

(defn with-worker [f]
  (let [worker (start-worker 9999)]
    (do
      (f)
      (stop-worker worker))))

(use-fixtures :once with-worker)

(deftest deftask-test
  (is (= '(fn [a b] a b)
         (do (deftask atask "Some doc" [a b] a b)
             (:task (meta #'atask)))))
  (is (= '(fn [] (+ 1 2))
         (do (deftask atask [] (+ 1 2))
             (:task (meta #'atask))))))

(deftest net-eval-test
  (is (= [[1 2 3]]
         (do (deftask atask [] [1 2 3])
             (map deref
               (net-eval [["127.0.0.1" 9999 #'atask]])))))
  (is (= [[1 2] [1 2]]
         (do (deftask atask [] [1 2])
             (map deref
               (net-eval [["127.0.0.1" 9999 #'atask]
                          ["127.0.0.1" 9999 #'atask]])))))
  (is (= [[1 2 3]]
         (do (deftask atask [a b c] [a b c])
             (map deref
               (net-eval [["127.0.0.1" 9999 #'atask 1 2 3]]))))))

(deftest error-test
  (is (thrown+? [:type :net-eval.core/connection-error]
      (deref (first
               (net-eval [["127.0.0.1" 9876 #'atask 1 2 3]]))))))

