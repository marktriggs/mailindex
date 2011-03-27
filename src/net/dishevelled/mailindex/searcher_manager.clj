(ns net.dishevelled.mailindex.searcher-manager
  (:refer-clojure :exclude (take))
  (:use clojure.java.io)
  (:import (org.apache.lucene.search IndexSearcher)
           (org.apache.lucene.index IndexReader)
           (org.apache.lucene.store FSDirectory)))


;;; Locks in Clojure?  Mark!  How could you?!
;;;
;;; OK OK.  I'm sorry.  But I couldn't see a way of doing this that yielded the
;;; behaviour I wanted without jumping through a lot more hoops.  The desired
;;; semantics:
;;;
;;;   * A single IndexSearcher is held open and used to handle all queries.
;;;
;;;   * After the indexing thread wakes up and does its thing, it attempts to
;;;     reopen the IndexSearcher's underlying IndexReader to make the new
;;;     messages searchable.  If nothing has been added, this reopen does
;;;     nothing, but otherwise it yields a new IndexReader which is used to
;;;     create a replacement IndexSearcher.  This new searcher becomes our
;;;     active one.
;;;
;;;   * However!  A search in progress while this happens might still be using
;;;     the old IndexReader and we don't want to close it out from under them.
;;;
;;;   * Luckily, Lucene's IndexReader provides incRef() and decRef(), which
;;;     provide simple reference counting semantics for IndexReaders.  We just
;;;     need to make sure that our searcher calls incRef() before it starts
;;;     using our searcher, and decRef() once it's finished.  Lucene closes the
;;;     IndexReader automatically when its refcount hits zero.
;;;
;;;   * So, here's the challenge: how do you get the current IndexSearcher at a
;;;     moment in time and call incRef() on its underlying IndexReader as an
;;;     atomic operation?  Options I considered:
;;;
;;;       - Store the current IndexSearcher in an atom and try:
;;;
;;;              (let [searcher @current-index-searcher]
;;;                (-> searcher .getIndexReader .incRef))
;;;
;;;            Oops!  Doesn't work because the indexer might have closed our
;;;            IndexReader just moments before we had a chance to call incRef.
;;;
;;;
;;;       - Have an agent be responsible for managing the current IndexSearcher
;;;         and push all open/reopen/search operations through that.
;;;         Conceptually neat, but forces us to serialise our search queries.
;;;         Lucene's IndexSearcher is thread-safe and multi-threaded access is
;;;         encouraged, so it's a shame to give this away.
;;;
;;;       - Some sort of craziness with agents and futures.  Have
;;;         open/reopen/search operations go through an agent, and have the
;;;         thread performing a search pass a callback (continuation) that will
;;;         be called with results when their query is completed.  The agent
;;;         fires off queries in a different thread via futures and keeps track
;;;         of which IndexSearcher objects are still in use.  As queries
;;;         complete, they signal their completion to the agent, and call the
;;;         provided callback to supply the results.  Complicated.
;;;
;;;  If you can think of a Clojure-esque solution that is as simple as the
;;;  following code, let me know!
;;;


(def on-deck-searchers (atom {}))
(def lock (Object.))

(defn open [path]
  (locking lock
    (swap! on-deck-searchers
           assoc path (IndexSearcher.
                       (IndexReader/open
                        (FSDirectory/open (file path)))))))

(defn reopen [path]
  (locking lock
    (let [ir (.getIndexReader (@on-deck-searchers path))
          new-ir (.reopen ir)]
      (when-not (identical? ir new-ir)
        (swap! on-deck-searchers assoc path (IndexSearcher. new-ir))
        (.decRef ir)))))


(defn take [path]
  (locking lock
    (let [searcher (@on-deck-searchers path)
          ir (.getIndexReader searcher)]
      (.incRef ir)
      searcher)))


(defn release [path searcher]
  (locking lock
    (let [ir (.getIndexReader searcher)]
      (.decRef ir))))
