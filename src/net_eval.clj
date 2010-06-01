(ns net-eval
  #^{:author "Nurullah Akkaya",
     :doc "Simple distributed computing."}
  (:gen-class)
  (:use clojure.test)
  (:use clojure.contrib.server-socket)
  (:use clojure.contrib.str-utils)
  (:use [clojure.contrib.error-kit :as errkit])
  (:use [clojure.contrib.duck-streams :only [reader writer]])
  (:import (java.net Socket)
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
  (let [f (cons (:task (meta task)) args)]
    (net-write conn f)
    (read-string (re-sub  #".*=>" "" (net-read conn)))))

(errkit/deferror connection-error []
  "Raised when an exception occurred when executing task. task-pos
  is the position of task within the sequence given to net-eval."
  [task task-pos exception]
  {:msg (str "Connection error: Task " task-pos " failed to execute.")
   :unhandled (constantly nil)})

(defn- fire-task
  "Connect to a slave, send the task and return the output, 
   swallow any errors."
  ([task]
     (fire-task task 0))
  ([task task-pos]
     (try
      (let [[host port _ & args] task
            call (task 2)
            conn (connect host port)]
        (with-open [_ (:socket @conn)]
          (send-task conn call args)))
      (catch Exception e
        (errkit/raise connection-error task task-pos e)))))

(defn net-eval
  "Send tasks for evaluation, takes a vector of vectors containing 
   host port and task.

   Raises connection-error (using clojure.contrib.error-kit) if
   an error occurred while executing a task."
  [tasks]  
  (vec (map (fn [task task-pos]
              (let [fire-task (errkit/rebind-fn fire-task)]
                (future (fire-task task task-pos))))
            tasks
            (iterate inc 0))))

(defn -main
  "Create a REPL server, waiting for incoming tasks."
  [& args]
  (let [port (if (seq args) (first args) 9999)]
    (create-repl-server port)))
