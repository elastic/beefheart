{-# LANGUAGE DuplicateRecordFields #-}

module Beefheart.Types
  ( Analytics(..)
  , AnalyticsMapping(..)
  , App(..)
  , Bucket
  , BulkItem(..)
  , BulkOperation(..)
  , BulkResponse(..)
  , CliOptions(..)
  , Datacenter(..)
  , ElasticsearchURI
  , EnvOptions(..)
  , FastlyRequest(..)
  , FastlyService(..)
  , IndexName(..)
  , LogFormat(..)
  , MappingName(..)
  , Metrics
  , MetricIndexed(..)
  , PointOfPresence
  , ServiceDetails(..)
  , readLogFormat
  ) where

import RIO
import RIO.Char
import RIO.List (intercalate)
import RIO.Time
import RIO.Text (pack)

import Control.Exception.Safe
import Data.Aeson
-- My initial type specified simple numeric types, but some values from Fastly
-- (particularly initial values from the first request) can be exceptionally
-- big. Simply using Scientific numbers alleviates most of those observed
-- problems.
import Data.Scientific
import Data.Time.Clock.POSIX (POSIXTime)
import Katip
import Network.HTTP.Req
import Options.Applicative
import System.Envy hiding (Option, Parser, (.=))

import qualified System.Envy as E
import qualified System.Metrics as EKG

import Beefheart.Utils

-- |This is a little type alias to shorten some signatures later on. The tl;dr
-- is that although the `Req` library is great, it has one trait that makes it
-- kind of hard to use: it is _very_ strict about HTTP versus HTTPS. It'll
-- parse a URL but won't let you dynamically write some function signatures in
-- terms of the schema dynamically, so we end up having to preserve the
-- possibility of either value sort of laboriously. Not really ideal, but it is
-- what it is.
type ElasticsearchURI = Either (Url 'Http, Option 'Http) (Url 'Https, Option 'Https)

-- |Command-line arguments are defined at the top-level as a well-defined type.
data CliOptions =
  CliOptions
  { elasticsearchUrl   :: Text      -- ^ Where we'll index logs to.
  , esFlushDelay       :: Int       -- ^ Period in seconds to sleep between bulk
                                    -- indexing flushes to Elasticsearch
  , esIndex            :: Text      -- ^ Index prefix for Elasticsearch documents.
  , esDatePattern      :: Text      -- ^ Date pattern suffix for Elasticsearch
                                    -- indices (when not using ILM)
  , fastlyPeriod       :: Int       -- ^ How often to request metrics from the Fastly API.
  , httpRetryLimit     :: Int       -- ^ Maximum time (in seconds) HTTP requests
                                    -- should be limited to before exiting the
                                    -- program.
  , ilmDeleteDays      :: Int       -- ^ Max number of days to retain indices
  , ilmMaxSize         :: Int       -- ^ Max size for active index rollover in GB
  , logFormat          :: LogFormat -- ^ What logging format to use.
  , logVerbose         :: Bool      -- ^ Whether to log verbosely
  , metricsPort        :: Int       -- ^ Optional port to expose metrics over.
  , metricsWakeup      :: Int       -- ^ Period in seconds that the queue metrics
                                    -- thread will wait for in between
                                    -- instrumentation measurements
  , noILM              :: Bool      -- ^ Whether or not to use ILM for index rotation.
  , queueScalingFactor :: Natural   -- ^ Factor applied to service count for metrics queue
  , serviceScalingCap  :: Int       -- ^ A maximum value for how to scale the in-memory metrics document queue.
  , servicesCli        :: [Text]    -- ^ Which services to collect analytics for.
  }

-- |Represents what format to keep logs in
data LogFormat = LogFormatJSON | LogFormatBracket
               deriving (Enum, Eq, Bounded)

-- |This just helps us have a user-friendly way of showing log format types to
-- users when printing CLI options
instance Show LogFormat where
  show LogFormatJSON = "json"
  show LogFormatBracket = "bracket"

-- |Easy way to configure log formatting at the type level for
-- optparse-applicative
readLogFormat :: ReadM LogFormat
readLogFormat = eitherReader $ \cliArg ->
  case lookup cliArg mapping  of
    Just format -> Right format
    Nothing -> Left $ "Accepted types for log formats: "
                   <> intercalate ", " (map show constructors)
  where
    constructors = [minBound..maxBound]
    mapping = map (\x -> (show x, x)) constructors

-- |Enumerates all the environment variables we expect.
data EnvOptions =
  EnvOptions
  { appEnvironment :: Environment
  , fastlyKey      :: Text
  , esUsername     :: Maybe Text
  , esPassword     :: Maybe Text
  } deriving (Generic, Show)

-- |Although `envy` does support deriving Generic, this and `ToEnv` are
-- |explicitly defined because `Maybe a` support isn't quite as clean without
-- |explicit typeclasses.
-- |
-- |This instance instructs how to _get_ variables from the environment
instance FromEnv EnvOptions where
  fromEnv _ =
    EnvOptions
    <$> envMaybe "ENVIRONMENT" E..!= "development"
    <*> env "FASTLY_KEY"
    <*> envMaybe "ES_USERNAME"
    <*> envMaybe "ES_PASSWORD"

-- |This instance represents how to show our record in errors/CLI
-- documentation.
instance ToEnv EnvOptions where
  toEnv EnvOptions {..} =
    makeEnv
    [ "ENVIRONMENT" E..= appEnvironment
    , "FASTLY_KEY"  E..= fastlyKey
    , "ES_USERNAME" E..= esUsername
    , "ES_PASSWORD" E..= esPassword
    ]

-- |Minor addition to support constructing an `Environment` from environment
-- variables.
instance Var Environment where toVar = show ; fromVar = Just . Environment . pack

-- |This is the application environment that RIO requires as its ReaderT value.
-- This value will be readable from our application context wherever we run it.
data App = App
  { appCli            :: CliOptions
  , appEKG            :: EKG.Store
  , appEnv            :: EnvOptions
  , appESURI          :: ElasticsearchURI
  , appFastlyServices :: [Text]
  , appLogContext     :: LogContexts
  , appLogEnv         :: LogEnv
  , appLogNamespace   :: Namespace
  , appQueue          :: TBQueue BulkOperation
  }

-- |This is what defines how we run HTTP requests in our application. In order
-- to run `req` requests, we have to know how to a) retrieve an HTTP
-- configuration that defines things like how retries work as well as b) how to
-- handle HTTP exceptions.
--
-- In previous revisions of this application, I retried _everything_ and
-- avoided exceptions entirely in favor of pure return types. It turns out that
-- `Req` already retries response codes that make sense to retry, but bails out
-- with an exception for others. I'm now convinced that this is the preferable
-- behavior: I want to retry without much fuss if there's a gateway or network
-- timeout, but would rather hard fail if, for example, we send malformed JSON
-- for some reason, because those sort of failures probably aren't going to
-- change by waiting and just ends repeatedly sending garbage out needlessly.
instance MonadHttp (RIO App) where
  getHttpConfig = do
    app <- ask
    -- Use the defaults, except to override exception throwing logic and what
    -- our default retry policy looks like.
    return $ defaultHttpConfig
      { httpConfigCheckResponse = reqCheckResponse
      , httpConfigRetryJudge = reqRetryJudge
      , httpConfigRetryPolicy = backoffThenGiveUp (app & appCli & httpRetryLimit)
      }
  handleHttpException = throw

-- |Plumbing to let our App work as a Katip, or logging, Monad
instance Katip (RIO App) where
  getLogEnv = asks appLogEnv
  localLogEnv f (RIO app) = RIO (local (\s -> s { appLogEnv = f (appLogEnv s)}) app)

-- |As with `Katip m`, but for the more powerful `KatipContext` Monad
instance KatipContext (RIO App) where
  getKatipContext = asks appLogContext
  localKatipContext f (RIO app) = RIO (local (\s -> s { appLogContext = f (appLogContext s)}) app)

  getKatipNamespace = asks appLogNamespace
  localKatipNamespace f (RIO app) = RIO (local (\s -> s { appLogNamespace = f (appLogNamespace s)}) app)

-- Fastly types
--
-- Whenever we need to translate `data` types to/from json, we use the
-- `FromJSON` and `ToJSON` Aeson typeclasses. The ability to derive `Generic`
-- for datatypes lets us use these without the need to explicitly explain how to
-- convert these types to and from JSON.

-- |Represents a JSON response from the Fastly real-time analytics API.
-- Ref: https://docs.fastly.com/api/analytics
data Analytics =
  Analytics
  { aggregateDelay :: Maybe Int    -- ^ `Maybe` since it's not present in every response
  , fData          :: [Datacenter] -- ^ List of metrics by `Datacenter`
  , fError         :: Maybe Text   -- ^ Possible error message
  , timestamp      :: POSIXTime    -- ^ Should be fed back in subsequent requests
  } deriving (Generic, Show)

-- |Typically a simple `FromJSON` is enough, but because `data` is a keyword, we
-- must override `parseJSON` in order to use a custom parsing mechanism
-- (`fastlyApiEncoding`) to override some specific parts.
instance FromJSON Analytics where
  parseJSON = genericParseJSON fastlyApiEncoding
instance ToJSON Analytics

-- |A parseJSON utility that aids in parsing a Fastly response to an `Analytics`
-- type. There's really only two things that need to happen here, namely, to
-- expect JSON keys as capitalized values, and help avoid collisions with the
-- "data" Haskell keyword.
fastlyApiEncoding :: Options
fastlyApiEncoding = defaultOptions { fieldLabelModifier = capitalizeOrScrub }
  where capitalizeOrScrub "fData" = "Data"
        capitalizeOrScrub "fError" = "Error"
        capitalizeOrScrub s = capitalize s

-- |Capitalizes a string.
capitalize :: String -> String
capitalize (head':tail') = toUpper head' : tail'
capitalize [] = []

-- |A `Datacenter` encapsulates a point in time that a Fastly datacenter's
-- `PointOfPresence` endpoints are measured.
data Datacenter =
  Datacenter
  { aggregated :: Metrics
  , datacenter :: HashMap PointOfPresence Metrics
  , recorded   :: POSIXTime
  } deriving (Generic, Show)

instance FromJSON Datacenter
instance ToJSON Datacenter

-- |Minor type alias to help clarify type signatures.
type PointOfPresence = String

-- |These are all documented at the source via:
-- https://docs.fastly.com/api/analytics
data Metrics =
  Metrics
  { requests          :: Scientific -- Number of requests
  , resp_header_bytes :: Scientific -- Number of bytes transmitted in headers
  , resp_body_bytes   :: Scientific -- Number of bytes transmitted in bodies
  , hits              :: Scientific -- Number of hits
  , miss              :: Scientific -- Number of misses
  , synth             :: Scientific -- Number of synthetic responses
  , errors            :: Scientific -- Number of errors
  , hits_time         :: Scientific -- Amount of time spent delivering hits
  , miss_time         :: Scientific -- Amount of time spent delivering misses
  , miss_histogram    :: HashMap Bucket Scientific
  } deriving (Generic, Show)

instance FromJSON Metrics
instance ToJSON Metrics

-- |Another helper type alias.
type Bucket = Int

-- |Service detail responses are JSON structures returned from the Fastly
-- Service API.
data ServiceDetails =
  ServiceDetails
  { deleted_at  :: Maybe UTCTime
  , created_at  :: UTCTime
  , comment     :: Text
  , customer_id :: Text
  , updated_at  :: UTCTime
  , id          :: Text
  , publish_key :: Text
  , name        :: Text
  , versions    :: [ServiceVersion]
  } deriving (Generic, Show)

instance FromJSON ServiceDetails
instance ToJSON ServiceDetails

-- |These just represent the changes made to a service over time.
data ServiceVersion =
  ServiceVersion
  { testing    :: Bool
  , locked     :: Bool
  , number     :: Int
  , active     :: Bool
  , service_id :: Text
  , staging    :: Bool
  , created_at :: UTCTime
  , deleted_at :: Maybe UTCTime
  , comment    :: Text
  , updated_at :: UTCTime
  , deployed   :: Bool
  } deriving (Generic, Show)

instance FromJSON ServiceVersion
instance ToJSON ServiceVersion

-- |A well-structured datatype that we can pass into `fastlyReq`.
data FastlyRequest =
  FastlyRequest
  { apiKey       :: Text
  , service      :: FastlyService
  } deriving (Show)

data FastlyService = AnalyticsAPI Text POSIXTime
                   | ServiceAPI Text
                   | ServicesAPI
                   deriving (Show)

-- Elasticsearch types
--
-- Although the Bloodhound library provides us with many high-level
-- abstractions, the myriad of potential Elasticsearch responses aren't
-- codified. We define some manually here.

-- |Just a type for bulk API response from Elasticsearch.
data BulkResponse =
  BulkResponse
  { bulkErrors :: Bool
  , items      :: [HashMap Text BulkItem] -- ^ Per-document indexing results.
  , took       :: Int
  } deriving (Generic, Show)

-- |We have to munge "errors" from the API response to avoid keyword collisions
-- - aside from that, nothing unusual here.
instance FromJSON BulkResponse where
  parseJSON = genericParseJSON $
    defaultOptions { fieldLabelModifier = mungeError }
    where mungeError "bulkErrors" = "errors"
          mungeError s = s
instance ToJSON BulkResponse

-- |Per-document response from a _bulk API request.
data BulkItem =
  BulkItem
  { _id           :: Text
  , _index        :: Text
  , _primary_term :: Maybe Int
  , _seq_no       :: Maybe Int
  , _shards       :: Maybe ShardStatus
  , _type         :: Text
  , _version      :: Maybe Int
  , error         :: Maybe BulkError
  , result        :: Maybe Text
  , status        :: Int
  } deriving (Generic, Show)

instance FromJSON BulkItem
instance ToJSON BulkItem

data ShardStatus =
  ShardStatus
  { failed     :: Int
  , successful :: Int
  , total      :: Int
  } deriving (Generic, Show)

instance FromJSON ShardStatus
instance ToJSON ShardStatus

-- |These types mimic those of the community Elasticsearch client library
-- `Bloodhound`. Mocking them here means that we can write some functions later
-- on that are agnostic as to whether we end up using our own indexing logic or
-- `Bloodhound`'s.
newtype MappingName = MappingName Text
  deriving (Eq, Show, ToJSON, FromJSON)
newtype IndexName = IndexName Text
  deriving (Eq, Show, ToJSON, FromJSON)
data BulkOperation = BulkIndexAuto IndexName MappingName Value
  deriving (Eq, Show)

data BulkError =
  BulkError
  { _type :: Text
  , caused_by :: BulkErrorReason
  , reason :: Text
  } deriving (Generic, Show)

instance FromJSON BulkError where
  parseJSON = genericParseJSON $
    defaultOptions { fieldLabelModifier = mungeType }
instance ToJSON BulkError

data BulkErrorReason =
  BulkErrorReason
  { _type :: Text
  , reason :: Text
  } deriving (Generic, Show)

instance FromJSON BulkErrorReason where
  parseJSON = genericParseJSON $
    defaultOptions { fieldLabelModifier = mungeType }
instance ToJSON BulkErrorReason

-- |Another modifier since `type` is a keyword
mungeType :: String -> String
mungeType "_type" = "type"
mungeType s = s

-- |Just a helper to define index mappings.
data AnalyticsMapping = AnalyticsMapping deriving (Show)

-- |Used by our HTTP request to define an index mapping. We manually define
-- `toJSON` for the data type since we'll never actually need to create an
-- `AnalyticsMapping` value - just hand it to Elasticsearch once to create the
-- mapping.
instance ToJSON AnalyticsMapping where
  toJSON AnalyticsMapping =
    object
    [ "properties" .= object
      [ "timestamp" .= object
        [ "type" .= ("date" :: Text) ]
      , "requests" .= object
        [ "type" .= ("long" :: Text) ]
      , "resp_header_bytes" .= object
        [ "type" .= ("long" :: Text) ]
      , "resp_body_bytes" .= object
        [ "type" .= ("long" :: Text) ]
      , "hits" .= object
        [ "type" .= ("long" :: Text) ]
      , "miss" .= object
        [ "type" .= ("long" :: Text) ]
      , "synth" .= object
        [ "type" .= ("long" :: Text) ]
      , "errors" .= object
        [ "type" .= ("long" :: Text) ]
      , "hits_time" .= object
        [ "type" .= ("float" :: Text) ]
      , "miss_time" .= object
        [ "type" .= ("float" :: Text) ]
      -- TODO This mapping isn't right - it should be an object with properties.
      -- , "miss_histogram" .= object
      --   [ "type" .= ("long" :: Text) ]
      ]
    ]

-- |A type representing a metric that signals how many documents have been
-- indexed
newtype MetricIndexed = MetricIndexed Int
-- |How this metric should be rendered as JSON
instance ToJSON MetricIndexed where
  toJSON (MetricIndexed n) = object [ "indexed_documents" .= n ]
-- |How our logging driver should interpret the JSON
instance ToObject MetricIndexed
-- |How the logging driver should honor verbosity settings for this log item.
-- This is pretty simple for now.
instance LogItem MetricIndexed where
  payloadKeys _verb _a = AllKeys
