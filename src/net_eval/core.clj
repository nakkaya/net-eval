(ns net-eval.core
 "Execute code on remote nodes."
 (:gen-class)
 (:use [clojure.tools.nrepl.server :only [start-server stop-server]]
       [clojure.tools.nrepl :as nrepl]
       [slingshot.slingshot :only [throw+]]))

(def connect-timeout 1000)
(def default-port 9999)

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

(defn- send-task
  "Send the task to a remote machine, append arguments to the call if any,
   return the object send from the remote machine."
  [conn task args]
  (let [f (with-out-str (pr (cons (:task (meta task)) args)))]
    (-> (nrepl/client conn connect-timeout)
        (nrepl/message {:op "eval" :code f})
        nrepl/response-values
        first)))

(defn- fire-request
  "Connect to a slave, send the task and return the output,
   swallow any errors."
  ([request]
   (fire-request request 0))
  ([request request-pos]
     (try
      (let [[host port task & args] request]
        (with-open [conn (nrepl/connect :host host :port port)]
          (send-task conn task args)))
     (catch Exception e
       (throw+ {:type ::connection-error :request request :request-pos request-pos :exception e})))))

(defn net-eval
  "Send tasks for evaluation, takes a vector of vectors containing
   host, port, task and args.

   Raises :net-eval/connection-error (using slingshot.slingshot.throw+) if
   an error occurred while executing a task."
  [requests]
  (vec (map (fn [request request-pos]
              (future (fire-request request request-pos)))
            requests
            (iterate inc 0))))

(defn start-worker
  "Create a net-eval worker, waiting for incoming tasks."
  ([]
   (start-worker default-port))
  ([port]
   (start-server :port port)))

(defn stop-worker
  "End execution of the net-eval worker."
  ([worker]
   (stop-server worker)))

(defn -main
  [& args]
  (if (seq args)
   (start-worker (first args))
   (start-worker)))
