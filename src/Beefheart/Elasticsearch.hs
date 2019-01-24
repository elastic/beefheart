module Beefheart.Elasticsearch
    ( bootstrapElasticsearch
    , encodeMetrics
    , indexAnalytics
    , mappingName
    ) where

import           ClassyPrelude
import           Data.Aeson
-- Aeson values are internally represented as `HashMap`s, which we import here
-- in order to munge them a little bit later.
import           Data.HashMap.Lazy      hiding (filter, fromList, map)
import qualified Data.HashMap.Lazy      as      HML
import           Data.Time.Clock.POSIX
import           Database.V5.Bloodhound hiding (Bucket)
import           Network.HTTP.Client           (defaultManagerSettings)

import Beefheart.Types
import Beefheart.Utils

-- |Self-explanatory
indexSettings = IndexSettings (ShardCount 2) (ReplicaCount 1)
-- |Since we'll try and be forward-compatible, work with just one ES type of
-- |`doc`
mappingName = MappingName "doc"

-- |Simple one-off to set up necessary ES indices
bootstrapElasticsearch :: BHEnv  -- ^ Bloodhound environment
                       -> Text   -- ^ Index prefix to use for template
                       -> IO ()  -- ^ For now, just ignore results
bootstrapElasticsearch es prefix = do
  existing <- runBH es $ templateExists templateName
  case existing of
    True -> pure ()
    False -> do
      runBH es $ do
        putTemplate template templateName
        pure ()
  where templateName = TemplateName "beefheart"
        template = IndexTemplate
                     (TemplatePattern $ prefix <> "*")
                     (Just indexSettings)
                     [toJSON AnalyticsMapping]

-- |Main entrypoint for indexing `Analytics` values. Munges the value before
-- |indexing in order to ensure it's well-formed for querying and visualization.
indexAnalytics :: BHEnv           -- ^ Bloodhound environment
               -> [BulkOperation] -- ^ List of operations to index
               -> IO (Either EsError BulkResponse) -- ^ Bulk request response
indexAnalytics es operations = do
  response <- runBH es . bulk . fromList $ operations
  parseEsResponse response

-- |Function suitable to be passed to a `fold` (assuming that it's first
-- |composed with a `POSIXTime`)
encodeMetrics :: Text            -- ^ Human-readable service name
              -> POSIXTime       -- ^ Time that the metrics were recorded
              -> [Value]         -- ^ Accumulated value
              -> PointOfPresence -- ^ Where the metrics were recorded
              -> Metrics         -- ^ Metrics we'd like to munge into a new
                                 -- ^ `Value`
              -> [Value]         -- ^ Accumulated value from the `fold`
encodeMetrics serviceName ts acc pop metrics = mergedObject : acc
  -- While the vanilla `Metrics` we get from Fastly are fine, enriching the
  -- value with a top-level key for the datacenter it came from along with the
  -- timestamp makes the documents easier to visualize and query.
  where mergedObject = mergeAeson
                       [ object
                         [ "pointofpresence" .= pop
                         , "service" .= serviceName
                         , "timestamp" .= (ts * 1000)
                         ]
                       , toJSON metrics
                       ]

-- |Small helper function to take a list of Aeson `Value`s and merge them together.
-- |source: https://stackoverflow.com/questions/44408359/how-to-merge-aeson-objects
mergeAeson :: [Value] -> Value
mergeAeson = Object . HML.unions . map (\(Object x) -> x)
