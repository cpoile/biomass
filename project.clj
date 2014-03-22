(defproject com.smnirven/biomass "0.5.1"
  :description "Boss people around. No suit required."
  :url "https://github.com/smnirven/biomass"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [ring/ring-codec "1.0.0"]
                 [clj-http "0.9.1" :exclusions [cheshire
                                                crouton
                                                org.clojure/tools.reader
                                                commons-codec]] ;; only this because ring-codec is behind
                 [clj-time "0.6.0"]
                 [com.github.kyleburton/clj-xpath "1.4.3"]
                 [com.github.cpoile/xml-to-clj "0.9.0"]
                 [org.clojars.zaxtax/java-aws-mturk "1.6.2"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :resource-paths ["test-resources"]
                   :plugins [[lein-midje "3.1.3"]]}})
