module Beefheart.Elasticsearch
    ( bootstrapElasticsearch
    , datePatternIndexName
    , indexAnalytics
    , mappingName
    , mergeAeson
    , toBulkOperations
    ) where

import           ClassyPrelude
import           Control.Monad.Catch
import           Control.Monad.Except (runExceptT)
import           Data.Aeson
import           Data.Default (def)
-- Aeson values are internally represented as `HashMap`s, which we import here
-- in order to munge them a little bit later.
import           Data.HashMap.Lazy      hiding (filter, fromList, map)
import qualified Data.HashMap.Lazy      as      HML
import           Data.Time.Clock.POSIX
import           Database.V5.Bloodhound hiding (Bucket)
import           Network.HTTP.Req

import Beefheart.Types

-- |Since we'll try and be forward-compatible, work with just one ES type of
-- |`doc`
mappingName :: MappingName
mappingName = MappingName "_doc"

-- |Check for the presence of a particular HTTP URL, and load up a JSON if it
-- isn't present.
checkOrLoad :: (ToJSON a, MonadHttp m)
            => Url scheme       -- ^ `URL` to check
            -> Url scheme       -- ^ `URL` to use to load the json
            -> Int              -- ^ ES Port
            -> a                -- ^ Potential `ToJSON` value to load
            -> m IgnoreResponse -- ^ Body-less response (useful for response code?)
checkOrLoad checkUrl loadUrl port' payload = do
  response <- req'' $ req HEAD checkUrl NoReqBody ignoreResponse (port port')
  if responseStatusCode response == 200
  then do
    return response
  else do
    creation <- req PUT loadUrl (ReqBodyJson payload) ignoreResponse (port port')
    return creation
  where req'' = runReq def { httpConfigCheckResponse = (\_ _ _ -> Nothing) }

-- |If the `URL`s to check and PUT JSON are the same, define a little helper
-- function.
idempotentLoad
  :: (ToJSON a, MonadHttp m)
  => Url scheme       -- ^ Single `URL` point to both `HEAD` and `POST`
  -> Int              -- ^ ES Port
  -> a                -- ^ Potential JSON payload
  -> m IgnoreResponse -- ^ `MonadHttp` `m` returning response `HEAD`
idempotentLoad url port' body = checkOrLoad url url port' body

-- |Simple one-off to set up necessary ES indices and other machinery like
-- index lifecycles. In the future, we shouldn't ignore the json response, but
-- this is okay for now.
bootstrapElasticsearch
  :: (Monad m, MonadIO m)
  => Bool -- ^ Whether to setup ILM pieces as well
  -> Text -- ^ Our index name
  -> Url scheme -- ^ Host portion of Elasticsearch `URL` (scheme, host)
  -> Int -- ^ ES Port
  -> m (Either HttpException IgnoreResponse) -- ^ Return either exception or response headers
bootstrapElasticsearch noILM esIndex esUrl port' =
  runExceptT $ do
    if noILM then do
      setupTemplate esIndex esUrl port'
    else do
      setupTemplate esIndex esUrl port'
        >> setupILM esUrl port'
        >> setupAlias esIndex esUrl port'

-- |Configure the index (and alias).
setupAlias
  :: (MonadHttp m)
  => Text  -- ^ Our index name
  -> Url scheme -- ^ Host portion of Elasticsearch `URL` (scheme, host)
  -> Int -- ^ ES Port
  -> m (IgnoreResponse) -- ^ Return either exception or response headers
setupAlias esIndex esUrl port' =
  checkOrLoad (esUrl /: "_alias" /: esIndex) (esUrl /: indexName) port' newIndex
  where indexName = (esIndex <> "-000001")
        newIndex = toJSON $
          object
          [ "aliases" .= object
            [ esIndex .= object
              [ "is_write_index" .= True
              ]
            ]
          ]

-- |Setup ILM policy.
setupILM
  :: (MonadHttp m)
  => Url scheme -- ^ Host portion of Elasticsearch `URL` (scheme, host)
  -> Int -- ^ ES Port
  -> m (IgnoreResponse) -- ^ Return either exception or response headers
setupILM esUrl port' =
  idempotentLoad (esUrl /: "_ilm" /: "policy" /: "beefheart") port' ilmPolicy
  where ilmPolicy = toJSON $
          object
          [ "policy" .= object
            [ "phases" .= object
              [ "hot" .= object
                [ "actions" .= object
                  [ "rollover" .= object
                    [ "max_size" .= ("20GB" :: Text)
                    ]
                  ]
                ]
              , "delete" .= object
                [ "min_age" .= ("180d" :: Text)
                , "actions" .= object
                  [ "delete" .= object []
                  ]
                ]
              ]
            ]
          ]

-- |Set up index templates for the application.
setupTemplate
  :: (MonadHttp m)
  => Text  -- ^ Our index name
  -> Url scheme -- ^ Host portion of Elasticsearch `URL` (scheme, host)
  -> Int -- ^ ES Port
  -> m (IgnoreResponse) -- ^ Return either exception or response headers
setupTemplate esIndex esUrl port' =
  idempotentLoad (esUrl /: "_template" /: "beefheart") port' analyticsTemplate
  where analyticsTemplate = toJSON $
          object
          [ "index_patterns" .= [ esIndex <> "*" ]
          , "settings" .= object
            [ "number_of_shards" .= (2 :: Int)
            , "number_of_replicas" .= (1 :: Int)
            , "index.lifecycle.name" .= ("beefheart" :: Text)
            , "index.lifecycle.rollover_alias" .= esIndex
            ]
          , "mappings" .= toJSON AnalyticsMapping
          ]

-- |Given a Bloodhound environment and a list of operations, run bulk indexing
-- and get a response back.
indexAnalytics
  :: (MonadIO m, MonadThrow m)
  => BHEnv                           -- ^ Bloodhound environment
  -> [BulkOperation]                 -- ^ List of operations to index
  -> m (Either EsError BulkResponse) -- ^ Bulk request response
indexAnalytics es operations = do
  response <- runBH es . bulk . fromList $ operations
  parseEsResponse response

-- |Helper to take a response from Fastly and form it into a bulk operation for
-- Elasticsearch. The output from this is expected to be fed into
-- `indexAnalytics`.
toBulkOperations
  :: (POSIXTime -> IndexName) -- ^ Index prefix name generator
  -> Text                -- ^ Human-readable name for service
  -> Analytics           -- ^ Actual response from Fastly that needs to be converted
  -> [BulkOperation]     -- ^ List of resultant `BulkOperation`s
toBulkOperations indexNamer serviceName metrics = map toOperation . normalize $ metrics
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
    toOperation = BulkIndexAuto (indexNamer $ timestamp metrics) mappingName

-- |Index name when we want to use date pattern format (without ILM)
datePatternIndexName
  :: Text -- ^ The index prefix
  -> Text -- ^ Date pattern (think "%Y")
  -> POSIXTime -- ^ Timestamp to create the index suffix from
  -> IndexName -- ^ Our final ES index name
datePatternIndexName prefix datePattern ts =
  -- Note that `indexSuffix` might try to be _too_ helpful by quoting
  -- itself, which is why we filter out extraneous quotes.
  IndexName $ prefix <> "-" <> pack (filter (/= '"') indexSuffix)
  where indexSuffix = formatTime defaultTimeLocale (show datePattern) $
                        posixSecondsToUTCTime ts

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
