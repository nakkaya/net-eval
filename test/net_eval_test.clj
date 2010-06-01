(ns net-eval-test
  (:use [clojure.test])
  (:use [clojure.contrib.error-kit :as errkit])
  (:use [net-eval]))

(defmacro with-private-fns [[ns fns] & tests]
 "Refers private fns from ns and runs tests in context."
   `(let ~(reduce #(conj %1 %2 `(ns-resolve '~ns '~%2)) [] fns)
        ~@tests))

(deftest test-defmacro
  (is (= '(fn [a b] a b) (do (deftask atask "Some doc" [a b] a b)
			     (:task (meta #'atask)))))
  (is (= '(fn [] (+ 1 2)) (do (deftask atask [] (+ 1 2))
			      (:task (meta #'atask))))))

(with-private-fns [net-eval [connect net-write net-read]]
  (deftest test-network
    (let [conn (connect "127.0.0.1" 9999)]
      (is (= "clojure.core=> 3"  (do (net-write conn "(+ 1 2)")
				     (net-read conn))))
      (is (= "clojure.core=> (0 1)"  (do (net-write conn "(range 2)")
					 (net-read conn)))))))

(with-private-fns [net-eval [connect send-task]]
  (deftest test-task
    (let [conn (connect "127.0.0.1" 9999)]
      (is (= [1 2 3]  (do (deftask atask [] [1 2 3])
			  (send-task conn #'atask nil))))
      (is (= '(0 1 2) (do (deftask atask [a] (range a))
			  (send-task conn #'atask '(3)))))
      (is (= 500 (do (deftask atask [a] (range a))
		     (count (send-task conn #'atask '(500)))))))))

(with-private-fns [net-eval [connect fire-task]]
  (deftest test-agent-state
    (is (= [1 2 3] (do (deftask atask [] [1 2 3])
		       (fire-task ["127.0.0.1" 9999 #'atask]))))
    (is (= [1 2 3] (do (deftask atask [] [1 2 3])
		       (fire-task ["127.0.0.1" 9999 #'atask]))))
    (is (= nil (do (deftask atask [] [1 2 3])
		   (fire-task ["127.0.1.1" 9999 #'atask]))))
    (is (= [1 2 3] (do (deftask atask [] [1 2 3])
		       (errkit/with-handler (fire-task ["127.0.1.1" 9999 #'atask])
			 (handle connection-error [task task-pos exception]
				 (continue-with (fire-task ["127.0.0.1" 9999 #'atask])))))))))

(deftest test-net-eval
  (is (= [[1 2 3]] (do (deftask atask [] [1 2 3])
		       (map deref (net-eval [["127.0.0.1" 9999 #'atask]])))))
  (is (= [[1 2] [1 2]] (do (deftask atask [] [1 2])
			   (map 
			    deref (net-eval [["127.0.0.1" 9999 #'atask]
					     ["127.0.0.1" 9999 #'atask]])))))
  (is (= [[1 2 3]] (do (deftask atask [a b c] [a b c])
		       (map 
			deref 
			(net-eval [["127.0.0.1" 9999 #'atask 1 2 3]]))))))

