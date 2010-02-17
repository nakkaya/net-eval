(ns net-eval
  #^{:author "Nurullah Akkaya",
     :doc "Simple distributed computing."}
  (:gen-class)
  (:use clojure.test)
  (:use clojure.contrib.server-socket)
  (:use clojure.contrib.str-utils)
  (:use [clojure.contrib.duck-streams :only [reader writer]])
  (:import (java.net Socket)
	   (java.io PrintWriter InputStreamReader BufferedReader)
	   (java.net InetSocketAddress)))
(def connect-timeout 3)

(defn to-fn [func]
  (if (vector? (first func))
    (cons 'fn func)
    (to-fn (rest func))))

(defmacro deftask
  "Define a func and save a copy of it as a list in its metadata."
  [& body]
  `(alter-meta!
     (defn ~@body)
        assoc :task (quote ~(to-fn body))))

(defn- connect
  "Connect to a remote socket and return handles for input and output."
  [host port]
  (let [socket (Socket.)]
    (.connect socket (InetSocketAddress. host port) connect-timeout)
    (ref {:socket socket
	  :in  (reader socket)
	  :out (writer socket)})))

(defn- net-write [conn cmd]
  (doto (:out @conn)
    (.println cmd)
    (.flush)))

(defn- net-read [conn]
  (.readLine (:in @conn)))

(defn- send-task
  "Send the task to a remote machine, append arguments to the call if any, 
   return the object send from the remote machine."
  [conn task args]
  (let [meta (meta task)
	f (if (nil? args)
	    (list (:task meta))
	    (cons (:task meta) args))]
    (net-write conn f)
    (read-string (re-sub  #".*=>" "" (net-read conn)))))

(defn- fire-task
  "Connect to a slave, send the task and return the output, 
   swallow any errors."
  [task]
  (try
   (let [[host port _ & args] task
	 call (task 2)
	 conn (connect host port)
	 res (send-task conn call args)]
     (.close (:socket @conn))
     res)
   (catch Exception e)))

(defn net-eval
  "Send tasks for evaluation, takes a vector of vectors containing 
   host port and task."
  [tasks]  
  (for [task tasks]
    (future (fire-task task))))

(defn await-nodes [results]
  "Wait for all nodes to return."
  (map deref results))

(defn -main
  "Create a REPL server, waiting for incoming tasks."
  [& args]
  (let [port (if (not(nil? args)) (first args) 9999)]
    (create-repl-server port)))

(deftest test-defmacro
  (is (= '(fn [a b] a b) (do (deftask atask "Some doc" [a b] a b)
			     (:task (meta #'atask)))))
  (is (= '(fn [] (+ 1 2)) (do (deftask atask [] (+ 1 2))
			      (:task (meta #'atask))))))

(deftest test-network
  (let [conn (connect "127.0.0.1" 9999)]
    (is (= "clojure.core=> 3"  (do (net-write conn "(+ 1 2)")
				   (net-read conn))))
    (is (= "clojure.core=> (0 1)"  (do (net-write conn "(range 2)")
				       (net-read conn))))))

(deftest test-task
  (let [conn (connect "127.0.0.1" 9999)]
    (is (= [1 2 3]  (do (deftask atask [] [1 2 3])
			(send-task conn #'atask nil))))
    (is (= '(0 1 2) (do (deftask atask [a] (range a))
			(send-task conn #'atask '(3)))))
    (is (= 500 (do (deftask atask [a] (range a))
		   (count (send-task conn #'atask '(500))))))))

(deftest test-agent-state
  (is (= [1 2 3] (do (deftask atask [] [1 2 3])
                     (fire-task ["127.0.0.1" 9999 #'atask]))))
  (is (= [1 2 3] (do (deftask atask [] [1 2 3])
                     (fire-task ["127.0.0.1" 9999 #'atask]))))
  (is (= nil (do (deftask atask [] [1 2 3])
                 (fire-task ["127.0.1.1" 9999 #'atask])))))

(deftest test-net-eval
  (is (= [[1 2 3]] (do (deftask atask [] [1 2 3])
		       (let [a (net-eval [["127.0.0.1" 9999 #'atask]])]
			 (await-nodes a)))))
  (is (= [[1 2] [1 2]] (do (deftask atask [] [1 2])
			   (let [a (net-eval [["127.0.0.1" 9999 #'atask]
					      ["127.0.0.1" 9999 #'atask]])]
			     (await-nodes a)))))
  (is (= [[1 2 3]] (do (deftask atask [a b c] [a b c])
		       (let [a (net-eval 
				[["127.0.0.1" 9999 #'atask 1 2 3]])]
			 (await-nodes a))))))

(defn test-ns-hook []
  (try (create-repl-server 9999) (catch Exception e))
  (test-defmacro)
  (test-network)
  (test-task)
  (test-agent-state)
  (test-net-eval))
