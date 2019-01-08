{-# LANGUAGE RecordWildCards #-}
module Main where

import Beefheart

-- Note the use of `ClassyPrelude` here in lieu of the typical `Prelude`. This
-- is done primarily in order to get a baseline library with more
-- community-standardized tools like `text` and `async`.
import ClassyPrelude
import Control.Concurrent
import Data.Time.Clock.POSIX (POSIXTime)
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

      -- Map our Fastly/Elasticsearch IO over each Fastly service ID (in
      -- parallel). We don't ever expect the event loop to exit in normal
      -- operation, as the function recurses infinitely in order to loop.
      responses <- forConcurrently (services options) $ \service -> do
        -- Create a metrics counter for this service.
        counter <- EKG.createCounter ("requests-" <> service) metricsStore
        -- Fetch the service ID's details (to get the human-readable name)
        serviceDetails <- fastlyReq FastlyRequest
          { apiKey = fastlyKey vars
          , timestampReq = Nothing
          , serviceId = service
          , service = ServiceAPI
          } :: IO (Either HttpException (JsonResponse ServiceDetails))
        case serviceDetails of
          Left err -> return $ Left $ CustomAnalyticsError $ "Skipping service " <> service <> ": " <> tshow err
          Right resp -> do
            -- In this thread, enter the main event loop for the Fastly `service`
            processAnalytics
              (esIndex options)
              (fastlyKey vars)
              bhEnv
              (name $ responseBody resp)
              counter
              Nothing
              service

      -- If this point is reached, it's safe to assume the infinite recursion
      -- returned prematurely, so print out the reason.
      print responses

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
processAnalytics :: Text            -- ^ Index prefix
                 -> Text            -- ^ Fastly API key
                 -> BHEnv           -- ^ Bloodhound environment
                 -> Text            -- ^ Human-readable service name
                 -> Counter.Counter -- ^ EKG Counter to measure requests
                 -> Maybe POSIXTime -- ^ Timestamp for analytics request
                 -> Text            -- ^ Fastly Service ID
                 -> IO (Either AnalyticsError AnalyticsResponse) -- ^ Return either an error or response
processAnalytics prefix key bhEnv serviceName counter ts service = do
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
                         bhEnv
                         prefix
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
          processAnalytics prefix key bhEnv serviceName counter (Just ts'') service

          -- Maybe for use later: how to return a successful request instead of
          -- recursing.
          -- return $ Right $ AnalyticsResponse ts' bulkResponse

    -- Getting a `Left` back means our REST call to Fastly failed for some reason.
    Left httpErr -> return $ Left $ HTTP httpErr

  -- truncate our `POSIXTime` into a simple integer.
  where ts' = case ts of
                Nothing -> (0 :: POSIXTime)
                Just theTime -> theTime
