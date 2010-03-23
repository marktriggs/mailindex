(ns net.dishevelled.mailindex.fieldpool
  (:import (org.apache.lucene.document Field Field$Store Field$Index)))

(def *field-pools* (ref {}))

(defn- new-pool [name store tokenize]
  "Create a new pool of Field objects for a given field type."
  (ref {:next-available 0
	:fields (repeatedly #(Field. name "" store tokenize))}))


(defn- next-field [pool]
  "Return a Field object from a pool, re-using an old one where possible."
  (dosync
   (let [i (:next-available @pool)]
     (alter pool update-in [:next-available] inc)
     (nth (:fields @pool) i))))


(defn- make-field [name store tokenize]
  "Return a new field object."
  (dosync
   (when-not (@*field-pools* name)
     (alter *field-pools* assoc name (new-pool name store tokenize))))

  (next-field (@*field-pools* name)))


(defn reset []
  "Mark all Field objects as available for re-use."
  (doseq [pool (vals @*field-pools*)]
    (when (> (:next-available @pool) 100)
      (println "Pool size got to:" (:next-available @pool)))
    (dosync
     (doseq [field (take (:next-available @pool) (:fields @pool))]
       (.setValue field nil))
     (alter pool assoc :next-available 0))))


(defn tokenized-field [name val]
  "Create a tokenized, stored field."
  (doto (make-field name Field$Store/YES Field$Index/TOKENIZED)
    (.setValue val)))


(defn tokenized-unstored-field [name val]
  "Create a tokenized, stored field."
  (doto (make-field name Field$Store/NO Field$Index/TOKENIZED)
    (.setValue val)))


(defn untokenized-field [name val]
  "Create an untokenized, unstored field."
  (doto (make-field name Field$Store/NO Field$Index/UN_TOKENIZED)
    (.setValue val)))


(defn stored-field [#^String name #^String val]
  "Create a stored, untokenized field."
  (doto (make-field name Field$Store/YES Field$Index/UN_TOKENIZED)
    (.setOmitNorms true)
    (.setValue val)))

