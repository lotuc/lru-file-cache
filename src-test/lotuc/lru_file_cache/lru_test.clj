(ns lotuc.lru-file-cache.lru-test
  (:require
   [clojure.test :refer :all]
   [lotuc.lru-file-cache.lru :as lru]
   [lotuc.lru-file-cache.utils :as u]
   [lotuc.lru-file-cache.file-lock :as file-lock]))

(deftest record-test
  (testing "record & record removal"
    (let [!lru (atom {})
          cache {:!lru !lru}]
      (lru/-record cache "md5-0" 0 42)
      (is (= {:lru-map {"md5-0" 0} :size 42} @!lru))

      (lru/-record cache "md5-0" 1 42)
      (is (= {:lru-map {"md5-0" 1} :size 42} @!lru))

      (lru/-record cache "md5-1" 2 20)
      (is (= {:lru-map {"md5-0" 1 "md5-1" 2} :size 62} @!lru))

      (lru/-record-removal cache "md5-0" 42)
      (is (= {:lru-map {"md5-1" 2} :size 20} @!lru)))))

(deftest lru-cache-test
  (testing "lru cache test"
    (let [d (java.io.File/createTempFile "cache-dir" ".dir")
          _ (u/delete-file-if-exists d)
          _ (.mkdirs d)
          file-count 100
          max-size 169
          c (lru/create-lru-cache
             {:cache-dir (str d)
              :scan-lock {:watch-opts {:me-id "test"}}
              :max-size max-size
              :scan-opts {:interval-ms 100}
              :cleanup-opts {:interval-ms 100}})]

      (try
        (doseq [i (range file-count)]
          (lru/lru-put-content c (str "hello " i)))
        ;; TODO: hardcoding 3s for the cleanup to complete

        (Thread/sleep 3000)
        (testing (is (< (count (:lru-map @(:!lru c))) file-count)) "cleaned up")
        (testing (is (<= (:size @(:!lru c)) max-size)) "store limit")

        ;; add another group of files
        (doseq [i (range file-count)]
          (lru/lru-put-content c (str "hello " (+ i file-count))))
        (Thread/sleep 3000)
        (testing (is (< (count (:lru-map @(:!lru c))) file-count)) "cleaned up")
        (testing (is (<= (:size @(:!lru c)) max-size)) "store limit")

        (finally (lru/close-lru-cache! c))))))
