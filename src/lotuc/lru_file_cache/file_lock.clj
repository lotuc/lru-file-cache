(ns lotuc.lru-file-cache.file-lock
  (:require
   [clojure.core.async :as async]
   [clojure.java.io :as io]
   [lotuc.lru-file-cache.utils :as u]))

;;; There might be multiple process using the same cache directory. We want only
;;; one of them actually watching the directory.

(defn drain-chan [n ch]
  (async/go-loop []
    (if-some [v (async/<! ch)]
      (do (locking *out* (prn n v) (flush))
          (recur))
      (prn n :done))))

(defn -locked-by-me? [lock-file me-id]
  (try (and (.exists (io/file lock-file))
            (= (slurp lock-file) me-id))
       (catch Throwable _ false)))

(defn -try-lock [lock-file me-id]
  (let [f (java.io.File/createTempFile "lock-" "-lock")]
    (u/ensure-parents f)
    (spit f me-id)
    (u/ensure-parents lock-file)
    (u/atomic-move-file f lock-file)
    (-locked-by-me? lock-file me-id)))

(comment
  (-try-lock "/tmp/a.lock" "42")
  (-locked-by-me? "/tmp/a.lock" "42")
  (-locked-by-me? "/tmp/a.lock" "41")
  #_())

(defn watch-lock-file
  [lock-file
   {:keys [me-id
           watch-ms-locked-by-me
           watch-ms-locked-by-others
           contend-on-timeout]
    :or {watch-ms-locked-by-me 60000
         watch-ms-locked-by-others 10000
         contend-on-timeout (* 10000 6)}}]
  (let [me-id (or me-id (str (random-uuid)))
        lock-file (io/file lock-file)
        data (async/chan)
        ctrl (async/chan)
        timeout-sec
        (fn [is-me] (if is-me watch-ms-locked-by-me watch-ms-locked-by-others))
        try-lock
        #(-try-lock lock-file me-id)
        locked-by-me?
        #(-locked-by-me? lock-file me-id)
        hb-lost?
        #(if-some [mtime (u/get-file-mtime lock-file)]
           (> (- (System/currentTimeMillis) mtime)
              contend-on-timeout)
           true)

        cont?
        (fn [is-me]
          (async/go
            (async/alt!
              (async/timeout (timeout-sec is-me)) ([_] true)
              ctrl ([v] (some? v)))))

        is-me
        (if (and (.exists lock-file) (not (hb-lost?)))
          (locked-by-me?)
          (try-lock))]
    (async/go
      (when is-me
        (async/<! (async/thread (u/update-file-mtime lock-file)))
        (async/>! data :taken))
      (loop [is-me is-me]
        (if (async/<! (cont? is-me))
          (let [is-me'
                (or (async/<! (async/thread (locked-by-me?)))
                    (if (async/<! (async/thread (hb-lost?)))
                      (async/<! (async/thread (try-lock)))
                      false))]
            (when is-me'
              (async/<! (async/thread (u/update-file-mtime lock-file))))
            (when (not= is-me is-me')
              (async/>! data (if is-me' :taken :lost)))
            (recur is-me'))
          (do (when (async/<! (async/thread (locked-by-me?)))
                (async/<! (async/thread (u/delete-file-if-exists lock-file))))
              (async/close! data)))))
    {:data data :ctrl ctrl}))

(comment
  (do (do (def watcher0 (watch-lock-file
                         "/tmp/a.lock"
                         {:me-id "watcher0"
                          :watch-ms-locked-by-me 3000
                          :watch-ms-locked-by-others 3000
                          :contend-on-timeout 5000}))
          (drain-chan "watcher0" (:data watcher0)))
      (do (def watcher1 (watch-lock-file
                         "/tmp/a.lock"
                         {:me-id "watcher1"
                          :watch-ms-locked-by-me 3000
                          :watch-ms-locked-by-others 3000
                          :contend-on-timeout 5000}))
          (drain-chan "watcher1" (:data watcher1))))

  (do
    (async/close! (:ctrl watcher1))
    (async/close! (:ctrl watcher0)))
  #_())
