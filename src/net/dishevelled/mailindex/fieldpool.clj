(ns net.dishevelled.mailindex.fieldpool
  (:import (org.apache.lucene.document Field Field$Index Field$Store)))

(def field-pools (ref {}))

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


(defn- make-field [name store tokenize & [val]]
  "Return a new field object."
  (dosync
   (when-not (@field-pools name)
     (alter field-pools assoc name (new-pool name store tokenize))))

  (let [^Field field (next-field (@field-pools name))]
    (when val (.setValue field ^String val))
    field))


(defn reset []
  "Mark all Field objects as available for re-use."
  (doseq [pool (vals @field-pools)]
    (dosync
     (alter pool assoc :next-available 0))))


(defn tokenized-field [name val]
  "Create a tokenized, stored field."
  (make-field name Field$Store/YES Field$Index/ANALYZED val))


(defn tokenized-unstored-field [name val]
  "Create a tokenized, stored field."
  (make-field name Field$Store/NO Field$Index/ANALYZED val))


(defn untokenized-field [name val]
  "Create an untokenized, unstored field."
  (make-field name Field$Store/NO Field$Index/NOT_ANALYZED val))


(defn stored-field [^String name ^String val]
  "Create a stored, untokenized field."
  (doto (make-field name Field$Store/YES Field$Index/NOT_ANALYZED val)
    (.setOmitNorms true)))

