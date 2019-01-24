{-# LANGUAGE RecordWildCards #-}
module Main where

import Beefheart

-- Note the use of `ClassyPrelude` here in lieu of the typical `Prelude`. This
-- is done primarily in order to get a baseline library with more
-- community-standardized tools like `text` and `async`.
import ClassyPrelude hiding (atomically)
import Control.Concurrent
import Control.Concurrent.STM.TBQueue
import Control.Monad.STM
import Control.Monad.Loops (iterateM_)
import Data.HashMap.Lazy (foldlWithKey')
import Data.Time.Clock.POSIX
-- This is our Elasticsearch library.
import Database.V5.Bloodhound hiding (esUsername, esPassword, name)
import qualified Network.HTTP.Client as HTTP
import Network.HTTP.Req hiding (header)
-- CLI option parsing.
import Options.Applicative
-- Environment variable parsing.
import System.Envy hiding (Parser)
import System.Exit
-- EKG is a high-level process metrics collection and introspection library - by
-- default, its interface will be available over http://localhost:8000 after
-- starting the application.
import qualified System.Metrics as EKG
import qualified System.Metrics.Counter as Counter
import qualified System.Metrics.Gauge as Gauge
import qualified System.Remote.Monitoring as EKG

-- |Enumerates all the environment variables we expect.
data EnvOptions = EnvOptions
                  { fastlyKey  :: Text
                  , esUsername :: Maybe Text
                  , esPassword :: Maybe Text
                  } deriving (Generic, Show)

-- |Although `envy` does support deriving Generic, this and `ToEnv` are
-- |explicitly defined because `Maybe a` support isn't quite as clean without
-- |explicit typeclasses.
instance FromEnv EnvOptions where
  fromEnv = EnvOptions <$> env "FASTLY_KEY"
                       <*> envMaybe "ES_USERNAME"
                       <*> envMaybe "ES_PASSWORD"

instance ToEnv EnvOptions where
  toEnv EnvOptions {..} = makeEnv
                          [ "FASTLY_KEY"  .= fastlyKey
                          , "ES_USERNAME" .= esUsername
                          , "ES_PASSWORD" .= esPassword
                          ]

-- |Command-line arguments are defined at the top-level as a well-defined type.
data CliOptions = CliOptions
  { elasticsearchUrl :: Server -- ^ Where we'll index logs to.
  , esIndex          :: Text   -- ^ Index prefix for Elasticsearch documents.
  , esDatePattern    :: Text   -- ^ Date pattern suffix for Elasticsearch indices
  , metricsPort      :: Int    -- ^ Optional port to expose metrics over.
  , services         :: [Text] -- ^ Which services to collect analytics for.
  }

-- |This is the actual parser that will be run over the executable's CLI
-- |arguments.
cliOptions :: Parser CliOptions
cliOptions = CliOptions
  -- Parser we define later for the Elasticsearch URL.
  <$> serverOption
  -- Simple `Text` argument for the index prefix.
  <*> strOption
    ( long "index-url"
      <> help "index name prefix for Elasticsearch"
      <> metavar "INDEX"
      <> showDefault
      <> value "fastly-metrics"
    )
  <*> strOption
    ( long "date-pattern"
      <> help "date pattern suffix for Elasticsearch index"
      <> metavar "PATTERN"
      <> showDefault
      <> value "%Y.%m.%d"
    )
  -- How to parse the metrics port.
  <*> option auto
    ( long "metric-port"
      <> help "Which port to expose metrics over."
      <> showDefault
      <> value 8000
      <> metavar "PORT"
    )
  -- Finally, some (>= 1) positional arguments for each Fastly service ID.
  <*> some (argument str (metavar "SERVICE <SERVICE> ..."))

-- |Here we break up the parser a little bit: this parser indicates how to
-- |process the elasticsearch argument.
serverOption :: Parser Server
serverOption = Server
  -- A string option parser - pretty self-explanatory.
  <$> strOption
      ( long "elasticsearch-url"
     <> help "destination URL for elasticsearch documents"
     <> metavar "URL"
     <> showDefault
     <> value "http://localhost:9200"
      )

-- Executable entrypoint.
main :: IO ()
main = do
  -- Convenient, simple argument parser. This will short-circuit execution in
  -- the event of --help or missing required arguments.
  options <- execParser opts
  -- Similar case for environment variables.
  env <- decodeEnv :: IO (Either String EnvOptions)

  case env of
    -- `EnvOptions` only strictly requires a Fastly key, which is guaranteed
    -- present if we make it this far.
    Right vars -> do
      -- For convenience, we run EKG.
      metricsStore <- EKG.newStore
      EKG.registerGcMetrics metricsStore
      EKG.forkServerWith metricsStore "localhost" (metricsPort options)

      -- We create a dedicated Bloodhound environment value here which lets us
      -- potentially authenticate to Elasticsearch if those credentials are
      -- present.
      httpManager <- HTTP.newManager HTTP.defaultManagerSettings
      let bhEnv' = mkBHEnv (elasticsearchUrl options) httpManager
          bhEnv = case (esUsername vars, esPassword vars) of
                    (Just u, Just p) ->
                      bhEnv' { bhRequestHook = basicAuthHook (EsUsername u) (EsPassword p) }
                    _ ->
                      bhEnv'

      -- At this point our options are parsed and the API key is available, so
      -- start executing some IO actions:
      --
      -- Create any necessary index templates.
      bootstrapElasticsearch bhEnv (esIndex options)

      -- To retrieve and index our metrics safely between threads, use an STM
      -- Queue to communicate between consumers and producers. Important to note
      -- that the queue is bounded to avoid consumer/producer deadlock problems.
      metricsQueue <- atomically $ newTBQueue 500

      -- Spawn threads for each service which will fetch and queue up documents to be indexed.
      threadIds <- forM (services options) $ \service ->
        forkIO $ metricsRunner metricsStore metricsQueue (esIndex options) (esDatePattern options) (fastlyKey vars) service

      forkIO $ queueWatcher metricsStore metricsQueue

      -- Run our consumer to read metrics from the bounded queue.
      indexingRunner bhEnv metricsQueue

    -- finding `Nothing` means the API key isn't present.
    Left error -> do
      putStrLn $ "Error: missing key environment variables: " <> tshow error
      exitFailure

  -- This is where we instantiate our option parser.
  where opts = info (cliOptions <**> helper)
               ( fullDesc -- beefheart
                 <> progDesc
                  ( "Siphons Fastly real-time analytics to Elasticsearch."
                 <> "Requires FASTLY_KEY environment variable for authentication "
                 <> "and optional ES_USERNAME and ES_PASSWORD for Elasticsearch auth.")
                 <> header ( "a natural black angus beef heart slow-cooked in a "
                          <> "fiery sriracha honey wasabi BBQ sauce and set on "
                          <> "a rotating lazy Susan.")
               )

queueWatcher ekg q = do
  gauge <- EKG.createGauge "metricsQueue" ekg
  forever $ do
    queueLength <- atomically $ lengthTBQueue q
    Gauge.set gauge $ fromIntegral queueLength
    -- 10 seconds
    threadDelay (1 * 1000 * 1000)

metricsRunner ekg q indexPrefix datePattern key service = do
  -- Create a metrics counter for this service.
  counter <- EKG.createCounter ("requests-" <> service) ekg

  -- Fetch the service ID's details (to get the human-readable name)
  serviceDetails <- withRetries $ fastlyReq FastlyRequest
    { apiKey = key
    , timestampReq = Nothing
    , serviceId = service
    , service = ServiceAPI
    } :: IO (Either HttpException (JsonResponse ServiceDetails))

  case serviceDetails of
    Left err -> do
      putStrLn $ "Skipping service " <> service <> ": " <> tshow err
    Right serviceDetailsResponse -> do
      flip iterateM_ 0 $ \ts -> do
        response <- fetchMetrics counter key ts service
        case response of
          Right metrics -> do
            atomically $ do
              mapM_ (writeTBQueue q) (asBulks indexPrefix datePattern serviceName (responseBody metrics))
            threadDelay (1 * 1000 * 1000)
            return $ timestamp $ responseBody metrics
          where serviceName = name $ responseBody serviceDetailsResponse

asBulks prefix datePattern serviceName metrics = map toOperation . normalize $ metrics
  where indexSuffix = formatTime defaultTimeLocale date $ posixSecondsToUTCTime (timestamp metrics)
        date = show datePattern
        indexName = IndexName $ prefix <> "-" <> pack (filter (/= '"') indexSuffix)
        toOperation doc = BulkIndexAuto indexName mappingName doc
        -- Here, `normalize` means taking an `Analytics` value and massaging it
        -- into the Aeson `Value` (or JSON) that we'd ultimately like it to be
        -- represented as in Elasticsearch. Note that because each response from
        -- Fastly includes an array of metrics from each `PointOfPresence`,
        -- we'll get a list of `Value`s from one `Analytics` value here.
        normalize analytics = fData analytics >>= toESDoc
        -- Given a `Datacenter`, extract the list of metrics, and fold the
        -- `HashMap` into a list of `Value`s.
        toESDoc metrics = foldlWithKey' (encodeMetrics serviceName $ recorded metrics) []
                          $ datacenter metrics

indexingRunner :: BHEnv -> TBQueue BulkOperation -> IO b
indexingRunner bh q = do
  forever $ do
    docs <- atomically $ getMetrics q
    esResponse <- indexAnalytics bh docs
    case esResponse of
      Left esError -> print esError
      Right _ -> pure ()
    threadDelay (10 * 1000 * 1000)

getMetrics :: TBQueue BulkOperation -> STM [BulkOperation]
getMetrics q = do
  queueIsEmpty <- isEmptyTBQueue q
  check $ not queueIsEmpty
  flushTBQueue q

-- A few types to aid in abstracting some of the operations and type signatures
-- later.

-- |Sum type of the errors we can expect when interacting with Fastly and
-- |Elasticsearch.
data AnalyticsError = ES EsError                -- ^ Indicates that Elasticsearch
                                                -- returned with a response that we
                                                -- couldn't parse.
                    | HTTP HttpException        -- ^ An HTTP timeout or otherwise
                                                -- unexpected result.
                    | CustomAnalyticsError Text -- ^ A custom error type
                    deriving (Show)

-- |Small alias for what we expect for successful indexing executions: The time
-- |of the analytics to feed into the new request, and result of the bulk ES
-- |indexing call.
data AnalyticsResponse = AnalyticsResponse POSIXTime BulkResponse
                       deriving (Show)

-- |Accepts the requisite arguments to perform an analytics query and
-- |Elasticsearch bulk index for a particular Fastly service.
-- processAnalytics :: CliOptions      -- ^ Command-line options
--                  -> Text            -- ^ Fastly API key
--                  -> BHEnv           -- ^ Bloodhound environment
--                  -> Text            -- ^ Human-readable service name
--                  -> Counter.Counter -- ^ EKG Counter to measure requests
--                  -> Maybe POSIXTime -- ^ Timestamp for analytics request
--                  -> Text            -- ^ Fastly Service ID
--                  -> IO (Either AnalyticsError AnalyticsResponse) -- ^ Return either an error or response
fetchMetrics counter key ts service = do
  -- Immediately prior to the Fastly request, hit the counter.
  Counter.inc counter
  -- Perform actual API call to Fastly asking for metrics for a service. A type
  -- hint is required here to ensure that `fastlyReq` knows what type of
  -- `JsonResponse` to return.
  withRetries $ fastlyReq FastlyRequest
                { apiKey = key
                , timestampReq = Just ts
                , serviceId = service
                , service = AnalyticsAPI
                } :: IO (Either HttpException (JsonResponse Analytics))

  -- -- `serviceAnalytics` behaves by returning a `Left` in case of unexpected
  -- -- behavior and `Right` in the event of success.
  -- case metrics of
  --   Right metrics -> do
  --     let response = responseBody metrics -- get the JSON response as an Analytics record type
  --         ts'' = timestamp response       -- save the timestamp from the json response

  --     -- Index these analytics into Elasticsearch.
  --     indexResponse <- withRetries $ indexAnalytics
  --                                      serviceName
  --                                      bhEnv
  --                                      (esIndex opts)
  --                                      (esDatePattern opts)
  --                                      response

  --     case indexResponse of
  --       -- An `EsError` indicates an unparseable result.
  --       Left esError -> return $ Left $ ES esError
  --       Right bulkResponse -> do
  --         -- TODO although a `Right` indicates a successful bulk indexing
  --         -- response, the structure of an Elasticsearch bulk response may
  --         -- contain indications of some errors or otherwise failed indexing
  --         -- events. In the future, this `BulkResponse` record type should be
  --         -- checked more thoroughly to either fail out or retry documents that
  --         -- weren't indexed correctly.
  --         --
  --         -- Don't hammer Fastly or ES APIs too hard (wait one second -
  --         -- threadDelay is in microseconds) before proceeding to the next
  --         -- invocation
  --         threadDelay (1 * 1000 * 1000)
  --         -- Recurse to continue the event loop.
  --         processAnalytics opts key bhEnv serviceName counter (Just ts'') service

  --         -- Maybe for use later: how to return a successful request instead of
  --         -- recursing.
  --         -- return $ Right $ AnalyticsResponse ts' bulkResponse

  --   -- Getting a `Left` back means our REST call to Fastly failed for some reason.
  --   Left httpErr -> return $ Left $ HTTP httpErr

  -- -- truncate our `POSIXTime` into a simple integer.
  -- where ts' = case ts of
  --               Nothing -> (0 :: POSIXTime)
  --               Just theTime -> theTime
