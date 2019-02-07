module Beefheart.Fastly
    ( fastlyReq
    ) where

import           ClassyPrelude
import           Control.Monad.Except     (ExceptT, runExceptT, throwError)
import           Data.Aeson
import           Network.HTTP.Req

import Beefheart.Types

-- |Required in order to run our HTTP request later.
instance (MonadIO m) => MonadHttp (ExceptT HttpException m) where
  handleHttpException = throwError

-- |Retrieve a JSON response from Fastly. This function's signature errs on the
-- loosey-goosey side of polymorphism, so the `JsonResponse` need only be a
-- member of the `FromJSON` typeclass. Although this makes the function very
-- generic, a type annotation may be necessary when called in order to nudge the
-- compiler in the right direction.
--
-- This function makes an effort to avoid exceptions by using `Either` so that
-- error handling can be explicitly checked by the compiler.
--
-- The `req` library makes this fairly straightforward, and running it in
-- `runExceptT` ensures any exceptions are caught within the `Either` monad.
fastlyReq
  :: (FromJSON a , MonadIO m)
  => FastlyRequest -- ^ We wrap various request parameters in a record to avoid
                   -- a massive function signature.
  -> m (Either HttpException (JsonResponse a))
fastlyReq requestPayload =
  runExceptT $ req GET (fastlyUrl requestPayload) NoReqBody jsonResponse options
  where options = header "Fastly-Key" $ encodeUtf8 $ apiKey requestPayload

-- |Helper to form a request URL given a `FastlyRequest`. Broken apart via
-- pattern matching to make it clear how we treat different requests.
fastlyUrl
  :: FastlyRequest
  -> Url 'Https

-- When we want to hit the real-time analytics API, interpolate the timestamp
-- with the service ID.
fastlyUrl FastlyRequest { service = AnalyticsAPI, serviceId = id', timestampReq = t } =
  https "rt.fastly.com" /: "v1" /: "channel" /: id' /: "ts" /: toTime t
  where toTime Nothing        = "0"
        -- Failing to truncate/`floor` this time value can have weird effects (a
        -- POSIXTime isn't strictly a normal unix timestamp under the covers)
        toTime (Just theTime) = tshow (floor theTime :: Int)

-- Service API requests help us get details like human-readable name from the service.
fastlyUrl FastlyRequest { service = ServiceAPI, serviceId = id' } =
  https "api.fastly.com" /: "service" /: id'
