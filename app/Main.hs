module Main where

import Beefheart

-- Note the use of `RIO` here in lieu of the typical `Prelude`. This
-- is done primarily in order to get a baseline library with more
-- community-standardized tools like `text` and `async`.
import RIO
import RIO.Text hiding (length, null)

import Control.Lens hiding (argument)
import Data.Aeson
import Data.Aeson.Lens
import Data.Text (splitOn)
import GHC.Natural (intToNatural)
-- This is our Elasticsearch library.
import Database.V5.Bloodhound hiding (esUsername, esPassword, key, name)
-- Think of the equivalent to python's `requests`
import qualified Network.HTTP.Client as HTTP
import Network.HTTP.Req hiding (header)
-- CLI option parsing.
import Options.Applicative
-- Environment variable parsing.
import System.Envy hiding (Parser)
import Text.Read (readMaybe)

-- EKG is a high-level process metrics collection and introspection library - by
-- default, its interface will be available over http://localhost:8000 after
-- starting the application.
import qualified System.Metrics as EKG
import qualified System.Metrics.Label as Label
import qualified System.Remote.Monitoring as EKG

-- |This is the actual parser that will be run over the executable's CLI
-- |arguments.
cliOptions :: Parser CliOptions -- ^ Defines each argument as a `Parser`
cliOptions = CliOptions
  -- Parser we define later for the Elasticsearch URL.
  <$> strOption
    ( long "elasticsearch-url"
      <> help "destination URL for elasticsearch documents"
      <> metavar "URL"
      <> showDefault
      <> value "http://localhost:9200"
    )
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
      <> help ("date pattern suffix for Elasticsearch index to use when ILM "
            <> "isn't used to deal with index rollover"
            )
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
  -- ILM date cutoff to delete indices
  <*> option auto
    ( long "ilm-delete-days"
      <> help "Maximum number of days to retain ILM-managed indices."
      <> showDefault
      <> value 180
      <> metavar "DAYS"
    )
  -- ILM active index rollover size
  <*> option auto
    ( long "ilm-active-rollover"
      <> help "Size (in GB) that ILM-managed indices will rollover at."
      <> showDefault
      <> value 20
      <> metavar "GB"
    )
  <*> switch
    ( long "no-ilm"
      <> help ("Whether or not to rely on ILM for index rotation and curation. "
            <> "Requires basic (non-OSS) Elasticsearch distribution. "
            <> "If set, rely on date pattern strategy instead."
            )
      <> showDefault
    )
  -- Simple verbose switch
  <*> switch
    ( long "verbose"
      <> help "Verbose logging."
      <> showDefault
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
  -- Finally, positional arguments for each Fastly service ID. This isn't
  -- `some` since we support service list autodiscovery.
  <*> many (argument str (metavar "SERVICE <SERVICE> ..."))

-- Executable entrypoint.
main :: IO ()
main = do
  -- Convenient, simple argument parser. This will short-circuit execution in
  -- the event of --help or missing required arguments.
  options <- execParser opts
  -- Similar case for environment variables.
  env' <- decodeEnv :: IO (Either String EnvOptions)
  let parsedPort = case splitOn ":" (elasticsearchUrl options) of
                     (_scheme:_host:p:[]) -> readMaybe $ unpack p
                     _ -> Nothing

  -- A top-level case pattern match is easier to grok for particular failures
  case (env', (parseUrl $ encodeUtf8 $ elasticsearchUrl options), parsedPort) of
    -- finding `Nothing` means the API key isn't present, so fail fast here.
    (Left envError, _, _) -> do
      abort $ pack $ "Error: missing key environment variables: " <> envError

    -- Getting `Nothing` from parseUrl is no good, either
    (_, Nothing, _) -> do
      abort $ "Error: couldn't parse elasticsearch URL "
           <> elasticsearchUrl options

    -- A `Nothing` port means we can't `read` that, either
    (_, _, Nothing) -> do
      abort $ "Error: couldn't parse elasticsearch port for "
           <> elasticsearchUrl options

    -- `EnvOptions` only strictly requires a Fastly key, which is guaranteed
    -- present if we make it this far.
    (Right vars, (Just parsedUrl), (Just port')) -> do
      -- Two modes of operation are supported: explicit list of Fastly
      -- services, or if they aren't passed, pull in all that we can find over
      -- the API.
      services <- if (null $ servicesCli options)
                  then do
                    serviceListResponse <- withRetries ifLeft $ fastlyReq FastlyRequest
                      { apiKey       = (fastlyKey $ vars)
                      , service      = ServicesAPI
                      }
                    case serviceListResponse of
                      Left err -> abort $ tshow err
                      Right serviceListJson -> do
                        let serviceList :: [Text]
                            serviceList = ((responseBody serviceListJson) :: Value) ^.. key "data" . values . key "id" . _String
                        runSimpleApp $ do
                          case serviceList of
                            [] -> abort $ ("Didn't find any Fastly services to monitor." :: Text)
                            _ -> logInfo . display $
                              "Found " <> (tshow $ length serviceList) <> " services."
                        return serviceList
                  else
                    return $ servicesCli options

      -- For convenience, we run EKG.
      metricsStore <- EKG.newStore
      EKG.registerGcMetrics metricsStore
      _ <- EKG.forkServerWith metricsStore "0.0.0.0" (metricsPort options)

      -- With our metrics/EKG value, let's record some of our runtime configuration:
      EKG.createLabel (metricN "elasticsearch-url") metricsStore
        >>= flip Label.set (tshow $ elasticsearchUrl options)

      -- We create a dedicated Bloodhound environment value here which lets us
      -- potentially authenticate to Elasticsearch if those credentials are
      -- present.
      httpManager <- HTTP.newManager HTTP.defaultManagerSettings
      let bhEnv' = mkBHEnv (Server $ elasticsearchUrl options) httpManager
          bhEnv = case (esUsername vars, esPassword vars) of
                    (Just u, Just p) ->
                      bhEnv' { bhRequestHook = basicAuthHook (EsUsername u) (EsPassword p) }
                    _ ->
                      bhEnv'

      -- To retrieve and index our metrics safely between threads, use an STM
      -- Queue to communicate between consumers and producers. Important to note
      -- that the queue is bounded to avoid consumer/producer deadlock problems.
      --
      -- The actual queue size is a little arbitrary - enough breathing room for
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
          $ min (length services) (serviceScalingCap options)

      -- Setup a log function, then...
      logOptions <- logOptionsHandle stderr (logVerbose options)
      -- nest our application within a context that has a log handling function
      -- (`lf`)
      withLogFunc logOptions $ \lf -> do
        -- This is our core datatype; our `App` that houses our logging hook,
        -- configuration information, etc.
        let app = App
                  { appBH = bhEnv
                  , appCli = options
                  , appEKG = metricsStore
                  , appESPort = port'
                  , appEnv = vars
                  , appFastlyServices = services
                  , appLogFunc = lf
                  , appQueue = metricsQueue
                  }

        -- The default Haskell `Prelude` replacement we're using is `RIO`. RIO
        -- runs its main logic inside `runRIO`, which accepts our application
        -- environment as an argument, and everything after this point lives
        -- within `RIO`, so note that anything `IO`-related needs a `liftIO`.
        runRIO app $ do
          -- At this point our options are parsed and the API key is available, so
          -- start executing some IO actions:
          --
          -- Create any necessary index templates.
          -- TODO should the http request manager be shared?
          let bootstrap :: (Url scheme, b) -> IO (Either HttpException IgnoreResponse)
              bootstrap = (\x -> bootstrapElasticsearch app (fst x))
          resp <- liftIO $ withRetries ifLeft $ do
            either bootstrap bootstrap parsedUrl

          case resp of
               Left e ->
                 logError . display $ tshow e
               Right _r ->
                 logDebug . display $ "Successfully created ES templates for " <> (esIndex options)

          -- Because we support either timestamp-appended indices or automagic
          -- ILM index rollover, naming the index varies depending on whether
          -- ILM is in-use or not.
          let indexNamer = if (noILM options)
                          then
                            datePatternIndexName (esIndex options) (esDatePattern options)
                          else
                            (\_ -> IndexName (esIndex options))

          -- Spawn threads for each service which will fetch and queue up documents to be indexed.
          _metricsThreads <- forM services $ \service ->
            async $ metricsRunner app indexNamer service

          -- Spin up another thread to report our queue size metrics to EKG.
          gauge <- liftIO $ EKG.createGauge (metricN "metricsQueue") (appEKG app)
          _watcherThread <- async $ queueWatcher app gauge

          -- Finally, run our consumer to read metrics from the bounded queue in our
          -- main thread. Because we're already bulking index requests to
          -- Elasticsearch, there's not really a need to aggressively parallelize
          -- this.
          indexingRunner app

  -- This is where we instantiate our option parser.
  where opts = info (cliOptions <**> helper)
               ( fullDesc -- beefheart
                 <> progDesc
                  ( "Siphons Fastly real-time analytics to Elasticsearch."
                 <> "Requires FASTLY_KEY environment variable for authentication "
                 <> "and optional ES_USERNAME and ES_PASSWORD for Elasticsearch auth."
                 <> "Without a list of service IDs, autodiscover all in Fastly.")
                 <> header ( "a natural black angus beef heart slow-cooked in a "
                          <> "fiery sriracha honey wasabi BBQ sauce and set on "
                          <> "a rotating lazy Susan.")
               )

-- |Log an error and exit the program.
abort
  :: (MonadIO m, Display a)
  => a -- ^ Message to log
  -> m b
abort message = do
  runSimpleApp $ do
    logError . display $ message
  exitFailure
