(ns lotuc.lru-file-cache.walkthrough
  (:require
   [lotuc.lru-file-cache :as lru-file-cache]))

(def c (lru-file-cache/create-lru-file-cache
        "/tmp/lru-cache"
        {:max-size 1024
         :cleanup-opts {:interval-ms 3000}}))

;;; cache files
(def md5s
  (->> (for [i (range 200)]
         (do (Thread/sleep 1)
             (lru-file-cache/cache-content-by-md5
              c (str "hello world " i))))
       (into [])))

;;; wait 3s, cleanup works & size lower than 200
(count (:lru-map @(:!lru c)))
;;; when cleanup done, left size should less than `max-size`
(< (:size @(:!lru c)) 1024)

;;; earlier files get cleaned
(lru-file-cache/get-file-by-md5 c (md5s 0))

;;; get cached file
(lru-file-cache/get-file-by-md5 c (last md5s))

;;; stop all cache background tasks
(lru-file-cache/close-lru-file-cache! c)
