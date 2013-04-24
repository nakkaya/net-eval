# net-eval

A Clojure library designed to execute code on remote nodes.

## Usage

Start the REPL by running `lein repl` in the top directory. Start a worker:

    (use '[net-eval.core :only [start-worker]])
    (start-worker 4321)

In another terminal (possibly on anther computer) start the REPL again, define
a task and send it to the worker.

    (use 'net-eval.core)
    
    (deftask sum-and-print-task [x y]
     (let [s (+ x y)]
      (do
       (println s)
       s)))
    
    (def ip "localhost")
    (def response (net-eval [[ip 4321 #'sum-and-print-task 4 5]
                             [ip 4321 #'sum-and-print-task 6 9]]))
    (println (map deref response))

