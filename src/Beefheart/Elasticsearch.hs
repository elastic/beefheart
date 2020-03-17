module Beefheart.Elasticsearch
    ( bootstrapElasticsearch
    , datePatternIndexName
    , indexAnalytics
    , mappingName
    , mergeAeson
    , toBulkOperations
    ) where

import RIO hiding (Handler)
import qualified RIO.ByteString.Lazy as BSL
import RIO.Text (pack)
import RIO.Time

import           Data.Aeson
-- Aeson values are internally represented as `HashMap`s, which we import here
-- in order to munge them a little bit later.
import qualified Data.HashMap.Lazy as HML
import           Data.Time.Clock.POSIX
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
            -> Option scheme    -- ^ HTTP options such as port, user auth, etc.
            -> a                -- ^ Potential `ToJSON` value to load
            -> m IgnoreResponse -- ^ Body-less response (useful for response code?)
checkOrLoad checkUrl loadUrl opts payload = do
  response <- req GET checkUrl NoReqBody ignoreResponse opts
  if responseStatusCode response == 200
  then
    return response
  else
    req PUT loadUrl (ReqBodyJson payload) ignoreResponse opts

-- |If the `URL`s to check and PUT JSON are the same, define a little helper
-- function.
idempotentLoad
  :: (ToJSON a, MonadHttp m)
  => Url scheme       -- ^ Single `URL` point to both `HEAD` and `POST`
  -> Option scheme    -- ^ HTTP options such as port, user auth, etc.
  -> a                -- ^ Potential JSON payload
  -> m IgnoreResponse -- ^ `MonadHttp` `m` returning response `HEAD`
idempotentLoad url = checkOrLoad url url

-- |Simple one-off to set up necessary ES indices and other machinery like
-- index lifecycles. In the future, we shouldn't ignore the json response, but
-- this is okay for now.
bootstrapElasticsearch
  :: MonadHttp m
  => Bool -- ^ Whether to enable ILM lifecycle rules
  -> Int -- ^ ILM size before rotation
  -> Int -- ^ Max number of days before deletion
  -> Text -- ^ Index name
  -> (Url scheme, Option scheme ) -- ^ ES HTTP endpoint and HTTP options such as port, user auth, etc.
  -> m IgnoreResponse
bootstrapElasticsearch ilmDisabled ilmSize ilmDays idx (esUrl, reqOpts) =
  if ilmDisabled then
    setupTemplate idx esUrl reqOpts
  else
    setupTemplate idx esUrl reqOpts
    >> setupILM ilmSize ilmDays esUrl reqOpts
    >> setupAlias idx esUrl reqOpts

-- |Configure the index (and alias).
setupAlias
  :: (MonadHttp m)
  => Text  -- ^ Our index name
  -> Url scheme -- ^ Host portion of Elasticsearch `URL` (scheme, host)
  -> Option scheme -- ^ HTTP options such as port, user auth, etc.
  -> m IgnoreResponse -- ^ Return either exception or response headers
setupAlias esIndex esUrl opts =
  checkOrLoad (esUrl /: "_alias" /: esIndex) (esUrl /: indexName) opts newIndex
  where indexName = esIndex <> "-000001"
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
  -> Option scheme -- ^ HTTP options such as port, user auth, etc.
  -> m IgnoreResponse -- ^ Return either exception or response headers
setupILM gb days esUrl opts =
  idempotentLoad (esUrl /: "_ilm" /: "policy" /: "beefheart") opts ilmPolicy
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
  -> Option scheme -- ^ HTTP options such as port, user auth, etc.
  -> m IgnoreResponse -- ^ Return either exception or response headers
setupTemplate esIndex esUrl opts =
  idempotentLoad (esUrl /: "_template" /: "beefheart") opts analyticsTemplate
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

-- |Accepts a collection of bulk operations, connection tuple, and bulk indexes
-- all documents into Elasticsearch.
indexAnalytics
  :: (Foldable t, MonadHttp m, FromJSON a)
  => t BulkOperation -- ^ Usually a list of bulk operations
  -> (Url scheme, Option scheme) -- ^ ES connection information
  -> m (JsonResponse a) -- ^ Bulk response JSON
indexAnalytics operations (url, opts) =
  req POST (url /: "_bulk") body jsonResponse (opts <> contentType)
  where
    body = ReqBodyLbs $ asNDJSON operations
    contentType = header "Content-Type" "application/x-ndjson"

-- |Transforms (usually) a list of `BulkOperation`s into a newline-delimited
-- `ByteString` suitable for the Elasticsearch bulk API.
asNDJSON
  :: Foldable t
  => t BulkOperation -- ^ Bulk operations to process
  -> BSL.ByteString -- ^ Raw `ByteString` to send over HTTP
asNDJSON = foldl' bulkFormat ""
  where
    bulkFormat jsonLines (BulkIndexAuto (IndexName n) (MappingName _) value) =
      jsonLines
      <> encode (object [ "index" .= object [ "_index" .= n ] ])
      <> "\n"
      <> encode value
      <> "\n"

-- |Helper to take a response from Fastly and form it into a bulk operation for
-- Elasticsearch. The output from this is expected to be fed into
-- `indexAnalytics`.
toBulkOperations
  :: (POSIXTime -> IndexName) -- ^ Index prefix name generator
  -> Text                     -- ^ Fastly service ID
  -> Text                     -- ^ Human-readable name for service
  -> Analytics                -- ^ Actual response from Fastly that needs to be converted
  -> [BulkOperation]          -- ^ List of resultant `BulkOperation`s
toBulkOperations indexNamer serviceId serviceName metrics = map toOperation . normalize $ metrics
  where
    -- `normalize` in this context means taking an `Analytics` value and massaging it
    -- into the Aeson `Value` (or JSON) that we'd ultimately like it to be
    -- represented as in Elasticsearch. Note that because each response from
    -- Fastly includes an array of metrics from each `PointOfPresence`,
    -- we'll get a list of `Value`s from one `Analytics` value here.
    normalize analytics = fData analytics >>= toESDoc
    -- Given a `Datacenter`, extract the list of metrics, and fold the
    -- `HashMap` into a list of `Value`s.
    toESDoc metrics' = HML.foldlWithKey' (encodeMetrics serviceId serviceName $ recorded metrics') []
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
  :: Text            -- ^ Fastly service ID
  -> Text            -- ^ Human-readable service name
  -> POSIXTime       -- ^ Time that the metrics were recorded
  -> [Value]         -- ^ Accumulated value
  -> PointOfPresence -- ^ Where the metrics were recorded
  -> Metrics         -- ^ Metrics we'd like to munge into a new `Value`
  -> [Value]         -- ^ Accumulated value from the `fold`
encodeMetrics serviceId serviceName ts acc pop metrics = mergedObject : acc
  -- While the vanilla `Metrics` we get from Fastly are fine, enriching the
  -- value with a top-level key for the datacenter it came from along with the
  -- timestamp makes the documents easier to visualize and query.
  where mergedObject = mergeAeson
                       [ object
                         [ "pointofpresence" .= pop
                         , "service" .= serviceName
                         , "service_id" .= serviceId
                         -- Formatting the timestamp explicitly instead of
                         -- using a unix-style epoch timestamp avoids ambiguity
                         -- that I observed between versions 6 and 7 of
                         -- Elasticsearch.
                         , "timestamp" .= formattedTimestamp
                         ]
                       , toJSON metrics
                       ]
        -- This value ends up looking like "2019-09-13T18:00:00Z"
        formattedTimestamp = formatTime defaultTimeLocale (iso8601DateFormat $ Just "%H:%M:%SZ") $
                               posixSecondsToUTCTime ts

-- |Small helper function to take a list of Aeson `Value`s and merge them together.
-- |source: https://stackoverflow.com/questions/44408359/how-to-merge-aeson-objects
mergeAeson :: [Value] -> Value
mergeAeson = Object . HML.unions . map (\(Object x) -> x)
