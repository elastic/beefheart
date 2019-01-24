{-# LANGUAGE DuplicateRecordFields #-}

module Beefheart.Types
  ( Analytics(..)
  , AnalyticsMapping(..)
  , BulkResponse(..)
  , Datacenter(..)
  , FastlyRequest(..)
  , FastlyService(..)
  , Metrics
  , PointOfPresence
  , ServiceDetails(..)
  ) where

import ClassyPrelude
import Data.Aeson
-- My initial type specified simple numeric types, but some values from Fastly
-- (particularly initial values from the first request) can be exceptionally
-- big. Simply using Scientific numbers alleviates most of those observed
-- problems.
import Data.Scientific
import Data.Time.Clock.POSIX (POSIXTime)

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
        capitalizeOrScrub s = capitalize s

-- |Capitalizes a string.
capitalize :: String -> String
capitalize (head':tail') = charToUpper head' : tail'
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
  , serviceId    :: Text
  , timestampReq :: Maybe POSIXTime
  , service      :: FastlyService
  } deriving (Show)

data FastlyService = AnalyticsAPI | ServiceAPI
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
  , _primary_term :: Int
  , _seq_no       :: Int
  , _shards       :: ShardStatus
  , _type         :: Text
  , _version      :: Int
  , result        :: Text
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

-- |Just a helper to define index mappings.
data AnalyticsMapping = AnalyticsMapping deriving (Show)

-- |Used by `Bloodhound` to define an index mapping. We manually define `toJSON`
-- for the data type since we'll never actually need to create an
-- `AnalyticsMapping` value - just hand it to `Bloodhound`.
instance ToJSON AnalyticsMapping where
  toJSON AnalyticsMapping =
    object
    [ "doc" .= object
      [ "properties" .= object
        [ "timestamp" .= object
          [ "type" .= ("date" :: Text)]
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
    ]
