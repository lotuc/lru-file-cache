(ns lotuc.lru-file-cache
  (:require
   [lotuc.lru-file-cache.lru :as lru]))

(defn create-lru-file-cache
  ([cache-dir
    {:keys [max-size scan-lock watch-opts scan-opts cleanup-opts]
     :as opts}]
   (lru/create-lru-cache (assoc opts :cache-dir cache-dir)))
  ([cache-dir]
   (create-lru-file-cache cache-dir nil)))

(defn close-lru-file-cache!
  "Stop associated backgroud tasks."
  [cache]
  (lru/close-lru-cache! cache))

(defn cache-file-by-md5
  "Cache a file using its MD5 hash as the key."
  [cache file]
  (lru/lru-put cache file))

(defn cache-content-by-md5
  "Cache spitable content."
  [cache content-or-write-fn]
  (if (fn? content-or-write-fn)
    (lru/lru-put-writer-fn cache content-or-write-fn)
    (lru/lru-put-content cache content-or-write-fn)))

(defn get-file-by-md5
  "Get a file from the cache by its MD5 hash."
  [cache md5]
  (lru/lru-get cache md5))
