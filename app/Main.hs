module Main where

import Beefheart

-- Note the use of `ClassyPrelude` here in lieu of the typical `Prelude`. This
-- is done primarily in order to get a baseline library with more
-- community-standardized tools like `text` and `async`.
import ClassyPrelude
import Control.Concurrent
import Data.Time.Clock.POSIX (POSIXTime)
-- This is our Elasticsearch library.
import Database.V5.Bloodhound hiding (name)
import qualified Network.HTTP.Client as HTTP
import Network.HTTP.Req hiding (header)
-- CLI option parsing.
import Options.Applicative
import System.Environment (lookupEnv)
import System.Exit
-- EKG is a high-level process metrics collection and introspection library - by
-- default, its interface will be available over http://localhost:8000 after
-- starting the application.
import qualified System.Metrics as EKG
import qualified System.Metrics.Counter as Counter
import qualified System.Remote.Monitoring as EKG

-- |What the expected environment variable is.
apiKeyVariable :: String
apiKeyVariable = "FASTLY_KEY"

-- |Command-line arguments are defined at the top-level as a well-defined type.
data CliOptions = CliOptions
  { elasticsearchUrl :: Server -- ^ Where we'll index logs to.
  , esIndex          :: Text   -- ^ Index prefix for Elasticsearch documents.
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
  -- Fetching an environment variable is an I/O action, do so here.
  apiKey <- lookupEnv apiKeyVariable

  case apiKey of
    -- If there's an API key in the environment:
    Just key -> do
      -- For convenience, we run EKG.
      metricsStore <- EKG.newStore
      EKG.registerGcMetrics metricsStore
      EKG.forkServerWith metricsStore "localhost" (metricsPort options)

      -- At this point our options are parsed and the API key is available, so
      -- start executing some IO actions:
      --
      -- Create any necessary index templates.
      bootstrapElasticsearch (elasticsearchUrl options) (esIndex options)

      -- Map our Fastly/Elasticsearch IO over each Fastly service ID (in
      -- parallel). We don't ever expect the event loop to exit in normal
      -- operation, as the function recurses infinitely in order to loop.
      responses <- forConcurrently (services options) $ \service -> do
        -- Create a metrics counter for this service.
        counter <- EKG.createCounter ("requests-" <> service) metricsStore
        -- Fetch the service ID's details (to get the human-readable name)
        serviceDetails <- fastlyReq FastlyRequest
          { apiKey = key
          , timestampReq = Nothing
          , serviceId = service
          , service = ServiceAPI
          } :: IO (Either HttpException (JsonResponse ServiceDetails))
        case serviceDetails of
          Left err -> return $ Left $ CustomAnalyticsError $ "Skipping service " <> service <> ": " <> tshow err
          Right resp -> do
            -- In this thread, enter the main event loop for the Fastly `service`
            processAnalytics options (name $ responseBody resp) counter key Nothing service
      -- If this point is reached, it's safe to assume the infinite recursion
      -- returned prematurely, so print out the reason.
      print responses

    -- finding `Nothing` means the API key isn't present.
    Nothing -> do
      putStrLn $ "Error: missing key environment variable " <> pack apiKeyVariable
      exitFailure

  -- This is where we instantiate our option parser.
  where opts = info (cliOptions <**> helper)
               ( fullDesc -- beefheart
                 <> progDesc
                  ( "Siphons Fastly real-time analytics to Elasticsearch."
                 <> "Expects environment variable " <> apiKeyVariable <> " for authentication."
                  )
                 <> header ( "a natural black angus beef heart slow-cooked in a "
                          <> "fiery sriracha honey wasabi BBQ sauce and set on "
                          <> "a rotating lazy Susan.")
               )

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
processAnalytics :: CliOptions      -- ^ Command-line options
                 -> Text            -- ^ Human-readable service name
                 -> Counter.Counter -- ^ EKG Counter to measure requests
                 -> String          -- ^ Fastly API Key
                 -> Maybe POSIXTime -- ^ Timestamp for analytics request
                 -> Text            -- ^ Fastly Service ID
                 -> IO (Either AnalyticsError AnalyticsResponse) -- ^ Return either an error or response
processAnalytics options serviceName counter key ts service = do
  -- Immediately prior to the Fastly request, hit the counter.
  Counter.inc counter
  -- Perform actual API call to Fastly asking for metrics for a service. A type
  -- hint is required here to ensure that `fastlyReq` knows what type of
  -- `JsonResponse` to return.
  metrics <-
    ( fastlyReq FastlyRequest
                { apiKey = key
                , timestampReq = Just ts'
                , serviceId = service
                , service = AnalyticsAPI
                } :: IO (Either HttpException (JsonResponse Analytics)))

  -- `serviceAnalytics` behaves by returning a `Left` in case of unexpected
  -- behavior and `Right` in the event of success.
  case metrics of
    Right metrics -> do
      let response = responseBody metrics -- get the JSON response as an Analytics record type
          ts'' = timestamp response       -- save the timestamp from the json response

      -- Index these analytics into Elasticsearch.
      indexResponse <- indexAnalytics
                         serviceName
                         (elasticsearchUrl options)
                         (esIndex options)
                         response

      case indexResponse of
        -- An `EsError` indicates an unparseable result.
        Left esError -> return $ Left $ ES esError
        Right bulkResponse -> do
          -- TODO although a `Right` indicates a successful bulk indexing
          -- response, the structure of an Elasticsearch bulk response may
          -- contain indications of some errors or otherwise failed indexing
          -- events. In the future, this `BulkResponse` record type should be
          -- checked more thoroughly to either fail out or retry documents that
          -- weren't indexed correctly.
          --
          -- Don't hammer Fastly or ES APIs too hard (wait one second -
          -- threadDelay is in microseconds) before proceeding to the next
          -- invocation
          threadDelay (1 * 1000 * 1000)
          -- Recurse to continue the event loop.
          processAnalytics options serviceName counter key (Just ts'') service

          -- Maybe for use later: how to return a successful request instead of
          -- recursing.
          -- return $ Right $ AnalyticsResponse ts' bulkResponse

    -- Getting a `Left` back means our REST call to Fastly failed for some reason.
    Left httpErr -> return $ Left $ HTTP httpErr

  -- truncate our `POSIXTime` into a simple integer.
  where ts' = case ts of
                Nothing -> (0 :: POSIXTime)
                Just theTime -> theTime
