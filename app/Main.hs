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
import Data.Time.Clock.POSIX
import GHC.Natural (Natural, intToNatural)
-- This is our Elasticsearch library.
import Database.V5.Bloodhound hiding (esUsername, esPassword, key, name)
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
import qualified System.Metrics.Label as Label
import qualified System.Remote.Monitoring as EKG

-- |Just so we define it in one place
applicationName :: Text
applicationName = "beefheart"

-- |Helper to create EKG metric names
metricN :: Text -> Text
metricN n = applicationName <> "." <> n

-- |Enumerates all the environment variables we expect.
data EnvOptions =
  EnvOptions
  { fastlyKey  :: Text
  , esUsername :: Maybe Text
  , esPassword :: Maybe Text
  } deriving (Generic, Show)

-- |Although `envy` does support deriving Generic, this and `ToEnv` are
-- |explicitly defined because `Maybe a` support isn't quite as clean without
-- |explicit typeclasses.
instance FromEnv EnvOptions where
  fromEnv =
    EnvOptions
    <$> env "FASTLY_KEY"
    <*> envMaybe "ES_USERNAME"
    <*> envMaybe "ES_PASSWORD"

instance ToEnv EnvOptions where
  toEnv EnvOptions {..} =
    makeEnv
    [ "FASTLY_KEY"  .= fastlyKey
    , "ES_USERNAME" .= esUsername
    , "ES_PASSWORD" .= esPassword
    ]

-- |Command-line arguments are defined at the top-level as a well-defined type.
data CliOptions =
  CliOptions
  { elasticsearchUrl   :: Server  -- ^ Where we'll index logs to.
  , esFlushDelay       :: Int     -- ^ Period in seconds to sleep between bulk
                                  -- indexing flushes to Elasticsearch
  , esIndex            :: Text    -- ^ Index prefix for Elasticsearch documents.
  , esDatePattern      :: Text    -- ^ Date pattern suffix for Elasticsearch indices
  , fastlyBackoff      :: Int     -- ^ Seconds to sleep when encountering Fastly API errors.
  , metricsPort        :: Int     -- ^ Optional port to expose metrics over.
  , queueMetricsWakeup :: Int     -- ^ Period in seconds that the queue metrics thread will wakeup
  , queueScalingFactor :: Natural -- ^ Factor applied to service count for metrics queue
  , serviceScalingCap  :: Int     -- ^ A maximum value for how to scale the in-memory metrics document queue.
  , services           :: [Text]  -- ^ Which services to collect analytics for.
  }

-- |This is the actual parser that will be run over the executable's CLI
-- |arguments.
cliOptions :: Parser CliOptions -- ^ Defines each argument as a `Parser`
cliOptions = CliOptions
  -- Parser we define later for the Elasticsearch URL.
  <$> serverOption
  -- Parse seconds between bulk indexing events
  <*> option auto
    ( long "bulk-flush-period"
      <> help "seconds between bulk indexing flushes"
      <> metavar "SECONDS"
      <> showDefault
      <> value 10
    )
  -- Simple `Text` argument for the index prefix.
  <*> strOption
    ( long "index-prefix"
      <> help "index name prefix for Elasticsearch"
      <> metavar "INDEX"
      <> showDefault
      <> value "fastly-metrics"
    )
  -- Date pattern in classic unix form
  <*> strOption
    ( long "date-pattern"
      <> help "date pattern suffix for Elasticsearch index"
      <> metavar "PATTERN"
      <> showDefault
      <> value "%Y.%m.%d"
    )
  -- Parse the Fastly backoff seconds.
  <*> option auto
    ( long "fastly-backoff"
      <> help "How many seconds to wait when backing off from Fastly API errors"
      <> showDefault
      <> value 60
      <> metavar "SECONDS"
    )
  -- How to parse the metrics port.
  <*> option auto
    ( long "metric-port"
      <> help "Which port to expose metrics over."
      <> showDefault
      <> value 8000
      <> metavar "PORT"
    )
  -- How to parse the metrics thread wakeup period.
  <*> option auto
    ( long "metric-watcher-period"
      <> help "Period (in seconds) in which EKG queue metrics should be polled"
      <> showDefault
      <> value 1
      <> metavar "SECONDS"
    )
  -- Parse the multiplicative queue scaling factor
  <*> option auto
    ( long "queue-factor"
      <> help "Number to multiply by service count to construct doc queue size"
      <> showDefault
      <> value 1000
      <> metavar "FACTOR"
    )
  -- Parse the service queue scaling maximum.
  <*> option auto
    ( long "queue-service-scaling-max"
      <> help "A maximum value for how to scale the in-memory metrics document queue"
      <> showDefault
      <> value 100
      <> metavar "MAX"
    )
  -- Finally, some (>= 1) positional arguments for each Fastly service ID.
  <*> some (argument str (metavar "SERVICE <SERVICE> ..."))

-- |Here we break up the parser a little bit: this parser indicates how to
-- |process the elasticsearch argument.
serverOption :: Parser Server -- ^ Just knows how to parse an Bloodhound/Elasticsearch `Server`
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
  env' <- decodeEnv :: IO (Either String EnvOptions)

  case env' of
    -- finding `Nothing` means the API key isn't present, so fail fast here.
    Left envError -> do
      putStrLn $ "Error: missing key environment variables: " <> tshow envError
      exitFailure

    -- `EnvOptions` only strictly requires a Fastly key, which is guaranteed
    -- present if we make it this far.
    Right vars -> do
      -- For convenience, we run EKG.
      metricsStore <- EKG.newStore
      EKG.registerGcMetrics metricsStore
      _ <- EKG.forkServerWith metricsStore "localhost" (metricsPort options)

      -- With our metrics/EKG value, let's record some of our runtime configuration:
      EKG.createLabel (metricN "elasticsearch-url") metricsStore
        >>= flip Label.set (tshow $ elasticsearchUrl options)

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
      --
      -- The actual queue size is a little arbitary - enough breathing room for
      -- us to funnel metrics in from Fastly while we pull them off the queue
      -- for bulk indexing, but small enough to not bloat memory and halt if ES
      -- experiences backpressure for some reason.
      --
      -- In the absence of a perfect value, scale it roughly linearly with how
      -- many services we're watching, with a hard cap to avoid blowing up
      -- resident memory if we end up watching a /whole/ lot of services.
      metricsQueue <- atomically $
        newTBQueue
          -- Scale that number up by a factor, because a service is comprised of
          -- many endpoints.
          $ ((*) (queueScalingFactor options) . intToNatural)
          -- Find the smaller between how many Fastly services we want to watch
          -- versus a hard limit.
          $ min (length $ services options) (serviceScalingCap options)

      -- Spawn threads for each service which will fetch and queue up documents to be indexed.
      _metricsThreadIds <- forM (services options) $ \service ->
        forkIO $ metricsRunner metricsStore
                               (fastlyBackoff options)
                               metricsQueue
                               (esIndex options)
                               (esDatePattern options)
                               (fastlyKey vars)
                               service

      -- Spin up another thread to report our queue size metrics to EKG.
      _watcherThreadId <- forkIO $ queueWatcher (queueMetricsWakeup options) metricsStore metricsQueue

      -- Finally, run our consumer to read metrics from the bounded queue in our
      -- main thread. Because we're already bulking index requests to
      -- Elasticsearch, there's not really a need to aggressively parallelize
      -- this.
      indexingRunner bhEnv (esFlushDelay options) metricsQueue

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

-- |Small utility to keep an eye on our bulk operations queue.
queueWatcher
  :: Int       -- ^ Period to sleep in between queue polling
  -> EKG.Store -- ^ EKG metrics
  -> TBQueue a -- ^ Queue to watch
  -> IO b
queueWatcher period ekg q = do
  gauge <- EKG.createGauge (metricN "metricsQueue") ekg
  forever $ do
    queueLength <- atomically $ lengthTBQueue q
    Gauge.set gauge $ fromIntegral queueLength
    sleepSeconds period

-- |Self-contained IO action to regularly fetch and queue up metrics for storage
-- in Elasticsearch.
metricsRunner
  :: EKG.Store             -- ^ EKG metrics
  -> Int                   -- ^ Fastly API backoff factor
  -> TBQueue BulkOperation -- ^ Where we'll enqueue our ES docs
  -> Text                  -- ^ ES index prefix
  -> Text                  -- ^ ES index date pattern
  -> Text                  -- ^ Fastly API key
  -> Text                  -- ^ Fastly service ID
  -> IO ()                 -- ^ Negligible return type
metricsRunner ekg backoff q indexPrefix datePattern key service = do
  -- Create a metrics counter for this service.
  counter <- EKG.createCounter (metricN $ "requests-" <> service) ekg

  -- Fetch the service ID's details (to get the human-readable name)
  serviceDetails <- withRetries ifLeft $ fastlyReq FastlyRequest
    { apiKey       = key
    , timestampReq = Nothing
    , serviceId    = service
    , service      = ServiceAPI
    }

  case serviceDetails of
    Left err ->
      putStrLn $ "Skipping service " <> service <> ": " <> tshow err
    Right serviceDetailsResponse -> do
      -- Before entering the metrics fetching loop, record the service's details in EKG.
      EKG.createLabel (metricN service) ekg >>= flip Label.set serviceName

      -- iterateM_ executes a monadic action (here, IO) and runs forever,
      -- feeding the return value into the next iteration, which fits well with
      -- our use case: keep hitting Fastly and feed the previous timestamp into
      -- the next request.
      iterateM_ (queueMetricsFor backoff q toDocs getMetrics) 0
      where getMetrics = fetchMetrics counter key service
            toDocs = toBulkOperations indexPrefix datePattern serviceName
            serviceName = name $ responseBody serviceDetailsResponse

-- |Higher-order function suitable to be fed into iterate that will be the main
-- metrics retrieval loop. Get a timestamp, fetch some metrics for that
-- timestamp, enqueue them, sleep, repeat.
queueMetricsFor
  :: Int -- ^ API backoff factor
  -> TBQueue BulkOperation -- ^ Our application's metrics queue.
  -> (Analytics -> [BulkOperation]) -- ^ A function to transform our metrics into ES bulk operations
  -> (POSIXTime -> IO (Either HttpException (JsonResponse Analytics))) -- ^ Function to get metrics for a timestamp
  -> POSIXTime -- ^ The actual metrics timestamp we want
  -> IO POSIXTime -- ^ Return the new POSIXTime for the subsequent request
queueMetricsFor backoff q f getter ts = do
  response <- getter ts
  case response of
    Right metrics -> do
      atomically $ mapM_ (writeTBQueue q) (f (responseBody metrics))
      sleepSeconds 1
      return $ timestamp $ responseBody metrics
    Left httpException -> do
      putStrLn
        ( "Error from Fastly: " <> tshow httpException <> ". "
        <> "Easing off the API for " <> tshow backoff <> " seconds."
        )
      sleepSeconds backoff
      return ts

-- |A self-contained indexing runner intended to be run within a thread. Wakes
-- up periodically to bulk index documents that it finds in our queue.
indexingRunner
  :: BHEnv                 -- ^ Bloodhound (Elasticsearch) env
  -> Int                   -- ^ Seconds to sleep between bulk flushes
  -> TBQueue BulkOperation -- ^ Queue to pull documents from
  -> IO b                  -- ^ Negligible return type
indexingRunner bh delay q =
  forever $ do
    -- See the comment on dequeueOperations for additional information, but
    -- tl;dr, we should always get _something_ from this action. Note that
    -- Elasticsearch will error out if we try and index nothing ([])
    docs <- atomically $ dequeueOperations q
    esResponse <- indexAnalytics bh docs
    case esResponse of
      Left esError -> print esError
      Right _ -> return ()
    -- Sleep for a time before performing another bulk operation.
    sleepSeconds delay

-- |Flush Elasticsearch bulk operations that need to be indexed from our queue.
dequeueOperations
  :: TBQueue BulkOperation -- ^ Our thread-safe queue
  -> STM [BulkOperation]   -- ^ Monadic STM return value
dequeueOperations q = do
  -- STM paradigms in the Haskell world are very powerful but do require some
  -- context - here, if `check` fails, the transaction is restarted (it's a form
  -- of `retry` from the STM library.) However, the runtime will know that we
  -- tried to read from our queue when we bailed out to retry, so upon finding
  -- that the queue is empty, the transaction will then block until the queue is
  -- written to. This is why we don't need to wait/sleep before checking if the
  -- queue is empty again.
  queueIsEmpty <- isEmptyTBQueue q
  check $ not queueIsEmpty
  flushTBQueue q

-- |Retrieve real-time analytics from Fastly (with retries)
fetchMetrics
  :: Counter.Counter -- ^ An EKG counter to track request activity
  -> Text            -- ^ Fastly API key
  -> Text            -- ^ ID of the Fastly service we want to inspect
  -> POSIXTime       -- ^ Timestamp for analytics
  -> IO (Either HttpException (JsonResponse Analytics))
fetchMetrics counter key service ts = do
  -- Immediately prior to the Fastly request, hit the counter.
  Counter.inc counter
  -- Perform actual API call to Fastly asking for metrics for a service.
  withRetries ifLeft $
    fastlyReq FastlyRequest
              { apiKey = key
              , timestampReq = Just ts
              , serviceId = service
              , service = AnalyticsAPI
              }
