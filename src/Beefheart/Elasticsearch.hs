module Beefheart.Elasticsearch
    ( bootstrapElasticsearch
    , datePatternIndexName
    , indexAnalytics
    , mappingName
    , mergeAeson
    , toBulkOperations
    ) where

import RIO hiding (Handler)
import RIO.Text hiding (filter, map)
import RIO.Time
import RIO.Vector hiding (filter, map)

import           Control.Monad.Except (runExceptT)
import           Control.Monad.Catch (Handler(..), MonadMask)
import           Control.Retry
import           Data.Aeson
-- Aeson values are internally represented as `HashMap`s, which we import here
-- in order to munge them a little bit later.
import           Data.HashMap.Lazy      hiding (filter, fromList, map)
import qualified Data.HashMap.Lazy      as      HML
import           Data.Time.Clock.POSIX
import           Database.V5.Bloodhound hiding (Bucket)
import           Network.HTTP.Req
import qualified Network.HTTP.Client as HTTP

import Beefheart.Types
import Beefheart.Utils

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
  -- We wrap the `Req` library's `runReq` here in order to override exception
  -- catching behavior. Normally, the library actually throws an exception when
  -- it encounters a non-2xx respose code (weird, right?). Instead we swallow
  -- it so that we can explicitly check the response code with normal control
  -- flow.
  where req'' = runReq defaultHttpConfig { httpConfigCheckResponse = (\_ _ _ -> Nothing) }

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
  :: MonadIO m
  => Bool -- ^ Whether to enable ILM lifecycle rules
  -> Int -- ^ ILM size before rotation
  -> Int -- ^ Max number of days before deletion
  -> Text -- ^ Index name
  -> Url scheme -- ^ ES HTTP endpoint
  -> Int -- ^ ES Port
  -> m (Either HttpException IgnoreResponse)
bootstrapElasticsearch ilmDisabled ilmSize ilmDays idx esUrl esPort =
  runExceptT $ do
    if ilmDisabled then do
      setupTemplate idx esUrl esPort
    else do
      setupTemplate idx esUrl esPort
        >> setupILM ilmSize ilmDays esUrl esPort
        >> setupAlias idx esUrl esPort

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
  => Int -- ^ Max index size before rotation
  -> Int -- ^ Max days to retain indices
  -> Url scheme -- ^ Host portion of Elasticsearch `URL` (scheme, host)
  -> Int -- ^ ES Port
  -> m (IgnoreResponse) -- ^ Return either exception or response headers
setupILM gb days esUrl port' =
  idempotentLoad (esUrl /: "_ilm" /: "policy" /: "beefheart") port' ilmPolicy
  where ilmPolicy = toJSON $
          object
          [ "policy" .= object
            [ "phases" .= object
              [ "hot" .= object
                [ "actions" .= object
                  [ "rollover" .= object
                    [ "max_size" .= (tshow gb <> "GB" :: Text)
                    ]
                  ]
                ]
              , "delete" .= object
                [ "min_age" .= (tshow days <> "d" :: Text)
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
  :: (MonadIO m, MonadMask m, MonadReader env m, HasLogFunc env)
  => BHEnv                           -- ^ Bloodhound environment
  -> [BulkOperation]                 -- ^ List of operations to index
  -> m (Either EsError BulkResponse) -- ^ Bulk request response
indexAnalytics es operations = do
  -- `recovering` is a retry higher-order function that we can use to wrap
  -- around functions that may throw exceptions. It takes a policy (which we
  -- have a central definition for elsewhere) that dictates how we backoff, a
  -- function that gets called for each failure, and finally our monadic action
  -- to run.
  response <- recovering backoffThenGiveUp [shouldRetry] esRequest
  parseEsResponse response
  where
    -- `esRequest` ends up being the Thing We Want to Run
    esRequest = return $ runBH es . bulk . fromList $ operations
    -- `shouldRetry` gets handed a `RetryStatus` whenever `recovering` consults
    -- it to determine if it should actually retry the action (and as long as
    -- the policy allows it). It needs to implement a `Handler`, which accepts
    -- an exception type and hands back a boolean indicating whether to
    -- proceed. We're dumb here and just say we should always retry requests -
    -- our policy will eventually give up, but we'll persist through timeouts
    -- or disconnections.
    shouldRetry retryStatus = Handler $ \(_ :: HTTP.HttpException) -> do
      logError . display $
        "Failed bulk request to Elasticsearch. Failed " <> (tshow $ rsIterNumber retryStatus) <> " times so far."
      return True

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
                         -- Formatting the timestamp explicitly instead of
                         -- using a unix-style epoch timestamp avoids ambiguity
                         -- that I observed between versions 6 and 7 of
                         -- Elasticsearch.
                         , "timestamp" .= formattedTimestamp
                         ]
                       , toJSON metrics
                       ]
        -- This value ends up looking like "2019-09-13T18:00:00Z"
        formattedTimestamp = formatTime defaultTimeLocale (iso8601DateFormat $ Just ("%H:%M:%SZ")) $
                               posixSecondsToUTCTime ts

-- |Small helper function to take a list of Aeson `Value`s and merge them together.
-- |source: https://stackoverflow.com/questions/44408359/how-to-merge-aeson-objects
mergeAeson :: [Value] -> Value
mergeAeson = Object . HML.unions . map (\(Object x) -> x)
