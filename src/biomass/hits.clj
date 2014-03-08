(ns ^{:author "smnirven"
      :doc "Contains methods for making HITs API requests to MTurk"}
  biomass.hits
  (:require [biomass.request :refer :all]
            [biomass.response.hits :refer :all]
            [biomass.util :as util]
            [xml-to-clj.core :as xc]))

(defn- send-and-parse
  [operation params]
  (let [resp (send-request operation params)]
    (if (= (:status resp) 200)
      (parse operation resp)
      resp)))

(defn create-hit-with-type-id
  [{:keys [hit-type-id hit-layout-id assignment-duration
           lifetime layout-params] :as params}]
  {:pre [(string? hit-type-id)
         (string? hit-layout-id)
         (integer? assignment-duration)
         (integer? lifetime)
         (map? layout-params)]}
  (send-and-parse "CreateHIT" (merge {:HITTypeId hit-type-id
                                      :HITLayoutId hit-layout-id
                                      :AssignmentDurationInSeconds assignment-duration
                                      :LifetimeInSeconds lifetime}
                                     (util/restify-layout-params layout-params))))

(defn- create-hit-with-property-values
  [params]
  (throw (java.lang.UnsupportedOperationException. "Creating a HIT without hit-type-id is not supported yet")))

(defn get-hit
  [hit-id]
  (send-and-parse "GetHIT" {:HITId hit-id}))

(defn get-reviewable-hits
  []
  (send-and-parse "GetReviewableHITs" {}))

(defn search-hits
  []
  (send-and-parse "SearchHITs" {}))

(defn get-hits-for-qualification-type
  [{:keys [qualification-type-id page-size page-number]}]
  ;;TODO: defaults for optional page-size and page-number params
  (send-request {:Operation "GetHITsForQualificationType"
                 :PageSize page-size
                 :PageNumber page-number}))

(defn get-qual-structure
  [{:keys [qual-num qual-req comparator integer-value locale-value req-to-preview?]}]
  (let [qual-prefix (str "QualificationRequirement." qual-num ".")
        qual-comparator-key (keyword (str qual-prefix "Comparator"))
        qual-id (condp = qual-req
                  :masters (if is-aws-target-sandbox?
                             "2ARFPLSP75KLA8M8DH1HTEQVJT3SY6"
                             "2F1QJWKUDD8XADTFD2Q0G6UTO95ALH")
                  :categorization-masters (if is-aws-target-sandbox?
                                            "2F1KVCNHMVHV8E9PBUB2A4J79LU20F"
                                            "2NDP2L92HECWY8NS8H3CK0CP5L9GHO")
                  :photo-moderation-masters (if is-aws-target-sandbox?
                                              "2TGBB6BFMFFOM08IBMAFGGESC1UWJX"
                                              "21VZU98JHSTLZ5BPP4A9NOBJEK3DPG")
                  :number-hits-approved "00000000000000000040"
                  :locale "00000000000000000071"
                  :adult "00000000000000000060"
                  :percent-assignments-approved "000000000000000000L0")
        qual-map {(keyword (str qual-prefix "QualificationTypeId")) qual-id}
        prev-param (if-not (nil? req-to-preview?)
                     {(keyword (str qual-prefix "RequiredToPreview")) req-to-preview?})
        comparator-params (cond
                           (and comparator integer-value)
                           {qual-comparator-key comparator
                            (keyword (str qual-prefix "IntegerValue")) integer-value}
                           (and comparator locale-value)
                           {qual-comparator-key comparator
                            (keyword (str qual-prefix "LocaleValue")) locale-value}
                           :else
                           {qual-comparator-key "Exists"})]
    (merge
     qual-map
     prev-param
     comparator-params)))

(defn register-hit-type
  [{:keys [title description reward-amount reward-currency
           assignment-duration keywords qual-requirements]
    :or {keywords ""}}]
  {:pre [(string? title) (not (empty? title))
         (string? description) (not (empty? description))
         (float? reward-amount)
         (string? reward-currency)
         (integer? assignment-duration)]}
  (let [qual-params (if qual-requirements
                      (apply merge (for [req qual-requirements]
                                     (get-qual-structure req)))
                      {})
        hit-params {:Title title
                    :Description description
                    :Reward.1.Amount reward-amount
                    :Reward.1.CurrencyCode reward-currency
                    :AssignmentDurationInSeconds assignment-duration
                    :keywords keywords}]
    (send-and-parse "RegisterHITType" (merge hit-params qual-params))))


(defn create-hit
  [{:keys [hit-type-id] :as params}]
  (if-not (empty? hit-type-id)
    (create-hit-with-type-id params)
    (create-hit-with-property-values params)))

(defn disable-hit
  [hit-id]
  (send-and-parse "DisableHIT" {:HITId hit-id}))

(defn dispose-hit
  [hit-id]
  (send-and-parse "DisposeHIT" {:HITId hit-id}))
