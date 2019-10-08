module Beefheart.Fastly
    ( autodiscoverServices
    , fastlyReq
    ) where

import RIO

import Control.Lens hiding (argument)
import Data.Aeson
import Data.Aeson.Lens
import Network.HTTP.Req

import Beefheart.Types
import Beefheart.Utils

-- |Retrieve a JSON response from Fastly. This function's signature errs on the
-- loosey-goosey side of polymorphism, so the `JsonResponse` need only be a
-- member of the `FromJSON` typeclass. Although this makes the function very
-- generic, a type annotation may be necessary when called in order to nudge the
-- compiler in the right direction.
fastlyReq
  :: (FromJSON a , MonadHttp m)
  => FastlyRequest -- ^ We wrap various request parameters in a record to avoid
                   -- a massive function signature.
  -> m (JsonResponse a)
fastlyReq requestPayload =
  req GET (fastlyUrl $ service requestPayload) NoReqBody jsonResponse options
  where options = authHeader <> rTimeout
        authHeader = header "Fastly-Key" $ encodeUtf8 $ apiKey requestPayload
        rTimeout = (responseTimeout $ 60 * 1000 * 1000)

-- |Helper to form a request URL given a `FastlyRequest`. Broken apart via
-- pattern matching to make it clear how we treat different requests.
fastlyUrl
  :: FastlyService
  -> Url 'Https

-- When we want to hit the real-time analytics API, interpolate the timestamp
-- with the service ID.
fastlyUrl (AnalyticsAPI serviceId timestampReq) =
  https "rt.fastly.com" /: "v1" /: "channel" /: serviceId /: "ts" /: tshow (floor timestampReq :: Int)

-- Service API requests help us get details like human-readable name from the service.
fastlyUrl (ServiceAPI serviceId) =
  https "api.fastly.com" /: "service" /: serviceId

-- Service API requests help us get details like human-readable name from the service.
fastlyUrl ServicesAPI =
  https "api.fastly.com" /: "services"

-- |Helper function that grabs all available services within a Fastly account.
autodiscoverServices
  :: MonadHttp m -- ^ Monad to run our HTTP requests within
  => Text -- ^ Fastly API Key
  -> m [Text] -- ^ List of Fastly service IDs
autodiscoverServices key' = do
  serviceListResponse <- fastlyReq FastlyRequest
    { apiKey  = key'
    , service = ServicesAPI
    }

  -- In order to pull out the list of service IDs from the Fastly response,
  -- we use some lens operators like `^..` to poke at the json response in
  -- a more succinct way. This sequence of functions says "grab a list of
  -- values from the 'data' key, flatten out the structure into plain
  -- key/value pairs, and return the 'id' key of each as a `String`".
  let serviceList = ((responseBody serviceListResponse) :: Value) ^.. key "data" . values . key "id" . _String
  runSimpleApp $ do
    case serviceList of
      [] -> abort $ ("Didn't find any Fastly services to monitor." :: Text)
      _ -> logInfo . display $
        "Found " <> (tshow $ length serviceList) <> " services."
  return serviceList
