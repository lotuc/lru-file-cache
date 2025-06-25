(ns lotuc.lru-file-cache-test
  (:require
   [clojure.java.io :as io]
   [lotuc.lru-file-cache :as lru-file-cache]))

(def c (lru-file-cache/create-lru-file-cache
        "/tmp/lru-cache" 1000))

(def v0 (lru-file-cache/cache-file-by-md5 c "/tmp/b.json"))
(def v1 (lru-file-cache/cache-file-by-md5 c "/tmp/a.log"))
(def v2 (lru-file-cache/cache-file-by-md5 c "/tmp/a.csv"))

(.length (io/file "/tmp/b.json"))
(.length (io/file "/tmp/a.log"))
(.length (io/file "/tmp/a.csv"))

[c [v0 v1 v2]]

(lru-file-cache/clear-cache c)
(lru-file-cache/save-cache c)
