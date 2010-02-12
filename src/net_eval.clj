(ns net-eval
  #^{:author "Nurullah Akkaya",
     :doc "Simple distributed computing."}
  (:gen-class)
  (:use clojure.test)
  (:use clojure.contrib.server-socket)
  (:use clojure.contrib.str-utils)
  (:import (java.net Socket)
	   (java.io PrintWriter InputStreamReader BufferedReader)
	   (java.net InetSocketAddress)))
(def connect-timeout 3)

(defmacro deftask
  "Define a func and save a copy of it as a list in its metadata."
  [& body]
  `(alter-meta!
     (defn ~@body)
        assoc :task (quote ~(cons 'fn body))))

(defn- connect [host port]
  "Connect to a remote socket and return handles for input and output."
  (let [socket (Socket.)]
    (.connect socket (InetSocketAddress. host port) connect-timeout)
    (ref {:socket socket
	  :in (BufferedReader. (InputStreamReader. (.getInputStream socket)))
	  :out (PrintWriter. (.getOutputStream socket))})))

(defn- net-write [conn cmd]
  (doto (:out @conn)
    (.println cmd)
    (.flush)))

(defn- net-read [conn]
  (.readLine (:in @conn)))

(defn- send-task [conn task & args]
  "Send the task to a remote machine, append arguments to the call if any, return the object send from the remote machine."
  (let [meta (meta task)
	f (if (nil? args)
	    (list (:task meta))
	    (list (:task meta) (first args)))]
    (net-write conn f)
    (read-string (re-sub  #".*=>" "" (net-read conn)))))

(defn- fire-task [list host port task]
  "Connect to a slave, send the task and append output to the list, swallow any errors."
  (try
   (let [conn (connect host port)
	 res (send-task conn task)]
     (.close (:socket @conn))
     (conj list res))
   (catch Exception e list)))

(defn net-eval [tasks]
  "Send tasks for evaluation, takes a vector of vectors containing host port and task."
  (let [agent (agent [])] 
    (doseq [task tasks] 
      (send agent fire-task (first task) (second task) (task 2)))
    agent))

(defn -main [& args]
  "Create a REPL server, waiting for incoming tasks."
  (let [port (if (not(nil? args)) (first args) 9999)]
    (create-repl-server port)))

(deftest test-network
  (let [conn (connect "127.0.0.1" 9999)]
    (is (= "clojure.core=> 3"  (do (net-write conn "(+ 1 2)")
				   (net-read conn))))
    (is (= "clojure.core=> (0 1)"  (do (net-write conn "(range 2)")
				       (net-read conn))))))

(deftest test-task
  (let [conn (connect "127.0.0.1" 9999)]
    (is (= [1 2 3]  (do (deftask atask [] [1 2 3])
			(send-task conn #'atask))))
    (is (= '(0 1 2) (do (deftask atask [a] (range a))
			(send-task conn #'atask 3))))
    (is (= 500 (do (deftask atask [a] (range a))
		   (count (send-task conn #'atask 500)))))))

(deftest test-agent-state
  (is (= [[1 2 3]] (do (deftask atask [] [1 2 3])
		       (fire-task [] "127.0.0.1" 9999 #'atask))))
  (is (= [[1][1 2 3]] (do (deftask atask [] [1 2 3])
			  (fire-task [[1]] "127.0.0.1" 9999 #'atask))))
  (is (= [] (do (deftask atask [] [1 2 3])
		(fire-task [] "127.0.1.1" 9999 #'atask)))))

(deftest test-net-eval
  (is (= [[1 2 3]] (do (deftask atask [] [1 2 3])
		       (let [a (net-eval [["127.0.0.1" 9999 #'atask]])]
			 (await a) @a))))
  (is (= [[1 2] [1 2]] (do (deftask atask [] [1 2])
			   (let [a (net-eval [["127.0.0.1" 9999 #'atask]
					      ["127.0.0.1" 9999 #'atask]])]
			     (await a) @a)))))

(defn test-ns-hook []
  (try (create-repl-server 9999) (catch Exception e))
  (test-network)
  (test-task)
  (test-agent-state)
  (test-net-eval))
