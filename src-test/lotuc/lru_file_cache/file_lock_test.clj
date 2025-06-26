(ns lotuc.lru-file-cache.file-lock-test
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer :all]
   [lotuc.lru-file-cache.file-lock :as file-lock]
   [lotuc.lru-file-cache.utils :as u]))

(deftest lock-test
  (testing "check lock & try lock"
    (let [f   (java.io.File/createTempFile "lock-file-" ".lock")
          id0 "a"
          id1 "b"]
      (try
        (testing "no one is locking"
          (is (= false (file-lock/-locked-by-me? f id0)))
          (is (= false (file-lock/-locked-by-me? f id1))))

        (testing "locked by id0"
          (is (= true (file-lock/-try-lock f id0)))

          (is (= true (file-lock/-locked-by-me? f id0)))
          (is (= false (file-lock/-locked-by-me? f id1))))

        (testing "locked by id1"
          (is (= true (file-lock/-try-lock f id1)))

          (is (= false (file-lock/-locked-by-me? f id0)))
          (is (= true (file-lock/-locked-by-me? f id1))))

        (finally (u/delete-file-if-exists f))))))

(defn -take-or-timeout
  ([ch ms]
   (-take-or-timeout ch ms nil))
  ([ch ms timeout-val]
   (async/<!!
    (async/go
      (async/alt!
        ch ([v] v)
        (async/timeout ms) ([_] timeout-val))))))

(deftest watch-lock-test
  (testing "watch lock file"
    (let [f   (java.io.File/createTempFile "lock-file-" ".lock")
          ids (mapv (fn [i] (str "id-" i)) (range 3))

          poll-ms-locked-by-me 100
          poll-ms-locked-by-others 150
          contend-ms 210

          watchers
          (loop [[id & ids] ids r []]
            ;; ensure one of them get lock at first
            (Thread/sleep 100)
            (if id
              (recur ids
                     (->> {:me-id id
                           :poll-ms-locked-by-me poll-ms-locked-by-me
                           :poll-ms-locked-by-others poll-ms-locked-by-others
                           :contend-ms contend-ms}
                          (file-lock/watch-lock-file f)
                          ((fn [m] (conj r m)))))
              r))

          get-holds
          (fn* ^:once get-holds
               ([] (get-holds (constantly true)))
               ([idx->check?]
                (->> (map-indexed
                      (fn [i {:keys [data id]}]
                        (when (idx->check? i)
                          (future
                            (some-> (-take-or-timeout
                                     data (+ 100 poll-ms-locked-by-others))
                                    (assoc :index i)))))
                      watchers)
                     (filter some?)
                     (mapv deref))))

          holds (get-holds)
          hold-idx (:index (first (filter :hold? holds)))]
      (is (= 1 (count (filter :hold? holds))))
      (is (some? hold-idx))
      (let [{:keys [data ctrl]} (watchers hold-idx)]
        (async/close! ctrl)
        ;; stopped
        (is (= nil (-take-or-timeout data (* 2 poll-ms-locked-by-me) ::timeout))))

      (let [holds' (get-holds #(not= % hold-idx))
            hold-idx' (:index (first (filter :hold? holds')))]
        (is (= 1 (count (filter :hold? holds'))))
        (is (some? hold-idx'))
        (is (not= hold-idx' hold-idx)))

      (let [{:keys [data ctrl]} (watchers hold-idx)]
        (async/close! ctrl)))))

(comment
  (defmacro def-&-let [bindings & body]
    `(let [~@bindings]
       ~@(mapv (fn [[n]] `(def ~n ~n)) (partition 2 bindings))
       ~@body))

  (defn drain-chan [n ch]
    (async/go-loop []
      (if-some [v (async/<! ch)]
        (do (locking *out* (prn n v) (flush))
            (recur))
        (prn n :done))))
  #_())
