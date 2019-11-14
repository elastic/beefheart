module Main where

import Beefheart

-- Note the use of `RIO` here in lieu of the typical `Prelude`. This
-- is done primarily in order to get a baseline library with more
-- community-standardized tools like `text` and `async`.
--
-- We hide `tryAny`, since we're using a different exceptions package.
import RIO hiding (tryAny)
import RIO.Orphans ()
import RIO.Text (pack)

-- A third-party exceptions package that offers a few more guarantees
import Control.Exception.Safe
import GHC.Natural (intToNatural)
-- Think of the equivalent to python's `requests`
import Network.HTTP.Req hiding (header)
-- CLI option parsing.
import Options.Applicative
-- Environment variable parsing.
import System.Envy hiding (Option, Parser)

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
    ( long "fastly-period"
      <> help "Polling frequency against the Fastly real-time metrics API"
      <> showDefault
      <> value 1
      <> metavar "SECONDS"
    )
  -- Customizable HTTP backoff factor
  <*> option auto
    ( long "http-retry-limit"
      <> help ( "Hard limit to honor when retrying HTTP requests to Fastly or Elasticsearch, in minutes. "
             <> "Once this time limit is reached, the program will exit.")
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
  options <- execParser cliOpts
  -- Similar case for environment variables.
  env' <- decodeEnv :: IO (Either String EnvOptions)

  -- A top-level case pattern match is easier to grok for particular failures.
  -- Note that CLI parsing will bailout when things like required arguments
  -- aren't present, but parsing expected environment variables won't, which is
  -- why our case statement is only checking for the parsed environment and
  -- whether the Elasticsearch URL is well-formed.
  case (env', parseUrl $ encodeUtf8 $ elasticsearchUrl options) of
    -- finding `Nothing` means the API key isn't present, so fail fast here.
    (Left envError, _) ->
      abort $ pack $ "Error: missing key environment variables: " <> envError

    -- Getting `Nothing` from parseUrl is no good, either
    (_, Nothing) ->
      abort $ "Error: couldn't parse elasticsearch URL "
         <> elasticsearchUrl options

    -- `EnvOptions` only strictly requires a Fastly key, which is guaranteed
    -- present if we make it this far.
    (Right vars, Just parsedUrl) -> do
      -- Two modes of operation are supported: explicit list of Fastly
      -- services, or if they aren't passed, pull in all that we can find over
      -- the API.
      services <- if null $ servicesCli options
                  then
                    runReq defaultHttpConfig $ autodiscoverServices (fastlyKey vars)
                  else
                    return $ servicesCli options

      -- For convenience, we run EKG.
      metricsStore <- EKG.newStore
      EKG.registerGcMetrics metricsStore
      _ <- EKG.forkServerWith metricsStore "0.0.0.0" (metricsPort options)

      -- With our metrics/EKG value, let's record some of our runtime configuration:
      EKG.createLabel (metricN "elasticsearch-url") metricsStore
        >>= flip Label.set (tshow $ elasticsearchUrl options)

      -- We check whether HTTP basic auth credentials have been passed, and
      -- amend our HTTP value with the necessary options if so. From here on
      -- out, we use `esURI`, which should hold all the connection information
      -- for ES that we need like port, hostname, auth, etc.
      let reqAuth :: Option scheme
          reqAuth = case (esUsername vars, esPassword vars) of
            (Just u, Just p) -> basicAuthUnsafe (encodeUtf8 u) (encodeUtf8 p)
            _ -> mempty
          esURI = applyAuth parsedUrl
                  where applyAuth :: ElasticsearchURI -> ElasticsearchURI
                        applyAuth (Left (u, o)) = Left (u, o <> reqAuth)
                        applyAuth (Right (u, o)) = Right (u, o <> reqAuth)

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
                  { appCli = options
                  , appEKG = metricsStore
                  , appEnv = vars
                  , appESURI = esURI
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
          let bootstrap :: (Url scheme, Option scheme) -> RIO App IgnoreResponse
              bootstrap =
                bootstrapElasticsearch (noILM options)
                                       (ilmMaxSize options)
                                       (ilmDeleteDays options)
                                       (esIndex options)

          -- Run the bootstrapping logic. Our HTTP request library will retry
          -- responses that make sense to retry, such as network timeouts, but
          -- will fail and bailout for other response codes, like if our
          -- request is malformed.
          resp <- tryAny (either bootstrap bootstrap esURI)
          case resp of
            Left e -> do
              logError . display $ tshow e
              exitFailure
            Right _r ->
              logDebug . display $ "Successfully created ES templates for " <> esIndex options

          -- Because we support either timestamp-appended indices or automagic
          -- ILM index rollover, naming the index varies depending on whether
          -- ILM is in-use or not.
          let indexNamer =
                if noILM options
                then
                  datePatternIndexName (esIndex options) (esDatePattern options)
                else
                  (\_ -> IndexName (esIndex options))

          -- Create a gauge to measure our main application queue
          gauge <- liftIO $ EKG.createGauge (metricN "metricsQueue") (appEKG app)

          -- Run each of our threads concurrently:
          queueWatcher app gauge -- Our monitoring/instrumentation thread
            `concurrently_` indexingRunner -- Elasticsearch bulk indexer
            `concurrently_` forConcurrently services -- And, concurrently for each service:
              (metricsRunner indexNamer) -- Spin off a thread to poll metrics regularly.

  -- This is where we instantiate our option parser.
  where cliOpts = info (cliOptions <**> helper)
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
