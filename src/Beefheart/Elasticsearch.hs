module Beefheart.Elasticsearch
    ( bootstrapElasticsearch
    , indexAnalytics
    , mappingName
    , mergeAeson
    , toBulkOperations
    ) where

import           ClassyPrelude
import           Control.Monad.Catch
import           Data.Aeson
-- Aeson values are internally represented as `HashMap`s, which we import here
-- in order to munge them a little bit later.
import           Data.HashMap.Lazy      hiding (filter, fromList, map)
import qualified Data.HashMap.Lazy      as      HML
import           Data.Time.Clock.POSIX
import           Database.V5.Bloodhound hiding (Bucket)

import Beefheart.Types

-- |Self-explanatory
indexSettings :: IndexSettings
indexSettings = IndexSettings (ShardCount 2) (ReplicaCount 1)

-- |Since we'll try and be forward-compatible, work with just one ES type of
-- |`doc`
mappingName :: MappingName
mappingName = MappingName "doc"

-- |Simple one-off to set up necessary ES indices
bootstrapElasticsearch
  :: MonadIO m
  => BHEnv  -- ^ Bloodhound environment
  -> Text   -- ^ Index prefix to use for template
  -> m ()  -- ^ For now, just ignore results
bootstrapElasticsearch es prefix = do
  existing <- runBH es $ templateExists templateName
  if existing then return () else
    runBH es $ do
      _ <- putTemplate template templateName
      return ()
  where templateName = TemplateName "beefheart"
        template = IndexTemplate
                     (TemplatePattern $ prefix <> "*")
                     (Just indexSettings)
                     [toJSON AnalyticsMapping]

-- |Given a Bloodhound environment and a list of operations, run bulk indexing
-- and get a response back.
indexAnalytics
  :: (MonadIO m, MonadThrow m)
  => BHEnv                            -- ^ Bloodhound environment
  -> [BulkOperation]                  -- ^ List of operations to index
  -> m (Either EsError BulkResponse) -- ^ Bulk request response
indexAnalytics es operations = do
  response <- runBH es . bulk . fromList $ operations
  parseEsResponse response

-- |Helper to take a response from Fastly and form it into a bulk operation for
-- Elasticsearch. The output from this is expected to be fed into
-- `indexAnalytics`.
toBulkOperations
  :: Text            -- ^ Index prefix
  -> Text            -- ^ Date pattern for indexed documents (think "%Y")
  -> Text            -- ^ Human-readable name for service
  -> Analytics       -- ^ Actual response from Fastly that needs to be converted
  -> [BulkOperation] -- ^ List of resultant `BulkOperation`s
toBulkOperations prefix datePattern serviceName metrics = map toOperation . normalize $ metrics
  where
    -- `normalize` in this context means taking an `Analytics` value and massaging it
    -- into the Aeson `Value` (or JSON) that we'd ultimately like it to be
    -- represented as in Elasticsearch. Note that because each response from
    -- Fastly includes an array of metrics from each `PointOfPresence`,
    -- we'll get a list of `Value`s from one `Analytics` value here.
    normalize analytics = fData analytics >>= toESDoc
    -- Given a `Datacenter`, extract the list of metrics, and fold the
    -- `HashMap` into a list of `Value`s.
    toESDoc metrics' = foldlWithKey' (encodeMetrics serviceName $ recorded metrics') []
                      $ datacenter metrics'

    -- Take an Aeson `Value` and put it into BulkOperation form.
    toOperation = BulkIndexAuto indexName mappingName
    -- Note that `indexSuffix` might try to be _too_ helpful by quoting
    -- itself, which is why we filter out extraneous quotes.
    indexName = IndexName $ prefix <> "-" <> pack (filter (/= '"') indexSuffix)
    indexSuffix =
      formatTime defaultTimeLocale (show datePattern) $
        posixSecondsToUTCTime (timestamp metrics)

-- |Helper to take a list of `Value`s, a `Metric` we'd ultimately like to index,
-- and return a list of `Value`s. The function signature can be composed
-- initially with some static values (like the service name and timestamp the
-- metrics were recorded at) and be used as a higher-order argument to a fold.
encodeMetrics
  :: Text            -- ^ Human-readable service name
  -> POSIXTime       -- ^ Time that the metrics were recorded
  -> [Value]         -- ^ Accumulated value
  -> PointOfPresence -- ^ Where the metrics were recorded
  -> Metrics         -- ^ Metrics we'd like to munge into a new `Value`
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
