(ns ^{:author "smnirven"
      :doc "Contains methods needed for all manner of requests to MTurk"}
  biomass.request
  (:require [ring.util.codec :as codec]
            [clj-time.local :refer [local-now format-local-time]]
            [clj-http.client :as client])
  (:import (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)))

(defonce $HMAC_ALGORITHM "HmacSHA1")
(defonce $API_VERSION "2012-03-25")
(defonce $SERVICE "AWSMechanicalTurkRequester")
;; default to sandbox
(def $BASE_URL (atom "https://mechanicalturk.sandbox.amazonaws.com/"))

(def aws-access-key (ref nil))
(def aws-secret-access-key (ref nil))

(defn set-aws-creds
  [{:keys [AWSAccessKey AWSSecretAccessKey]}]
  (dosync
   (ref-set aws-access-key AWSAccessKey)
   (ref-set aws-secret-access-key AWSSecretAccessKey)))

(defn set-aws-target-as-sandbox
  [sandbox?]
  (if sandbox?
    (reset! $BASE_URL "https://mechanicalturk.sandbox.amazonaws.com/")
    (reset! $BASE_URL "https://mechanicalturk.amazonaws.com/")))

(defn is-aws-target-sandbox?
  []
  (= @$BASE_URL "https://mechanicalturk.sandbox.amazonaws.com/"))

(defn gen-aws-signature
  "Generates an RFC 2104 compliant HMAC for AWS authentication as
  outlined in the following article:
  http://docs.aws.amazon.com/AWSMechTurk/latest/AWSMechanicalTurkRequester/MakingRequests_RequestAuthenticationArticle.html"
  [operation timestamp]
  (let [data (str $SERVICE operation timestamp)
        signing-key (SecretKeySpec. (.getBytes @aws-secret-access-key) $HMAC_ALGORITHM)
        mac (doto (Mac/getInstance $HMAC_ALGORITHM) (.init signing-key))]
    (codec/base64-encode (.doFinal mac (.getBytes data)))))

(defn- gen-timestamp
  []
  (format-local-time (local-now) :date-time-no-ms))

(defn get-default-params
  [operation]
  (let [ts (gen-timestamp)]
    {:AWSAccessKeyId @aws-access-key
     :Version $API_VERSION
     :Service $SERVICE
     :Timestamp ts
     :Signature (gen-aws-signature operation ts)
     :Operation operation}))

(defn send-request
  [operation params]
  (if (and @aws-access-key @aws-secret-access-key)
    (let [final-params (merge params (get-default-params operation))]
      (client/get @$BASE_URL {:query-params final-params}))
    {:error "AWS credentials are unset."}))
