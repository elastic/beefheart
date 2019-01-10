module Beefheart.Fastly
    ( fastlyReq
    ) where

import           ClassyPrelude
import           Control.Monad.Except     (ExceptT, runExceptT, throwError)
import           Data.Aeson
import qualified Data.ByteString.Char8 as B
import           Network.HTTP.Req

import Beefheart.Types

-- |Required in order to run our HTTP request later.
instance (MonadIO m) => MonadHttp (ExceptT HttpException m) where
  handleHttpException = throwError

-- |Retrieve a JSON response from Fastly. This function's signature errs on the
-- |loosey-goosey side of polymorphism, so the `JsonResponse` need only be a
-- |member of the `FromJSON` typeclass. Although this makes the function very
-- |generic, a type annotation may be necessary when called in order to nudge the
-- |compiler in the right direction.
-- |
-- |This function makes an effort to avoid exceptions by using `Either` so that
-- |error handling can be explicitly checked by the compiler.
-- |
-- |The `req` library makes this fairly straightforward, and running it in
-- |`runExceptT` ensures any exceptions are caught within the `Either` monad.
fastlyReq :: (FromJSON a)
          => FastlyRequest
          -> IO (Either HttpException (JsonResponse a))
fastlyReq requestPayload = do
  runExceptT $ req GET (fastlyUrl requestPayload) NoReqBody jsonResponse options
  where options = header "Fastly-Key" $ encodeUtf8 $ apiKey requestPayload

fastlyUrl :: FastlyRequest -> Url 'Https
fastlyUrl r@(FastlyRequest { service = AnalyticsAPI, timestampReq = t }) =
  https "rt.fastly.com" /: "v1" /: "channel" /: (serviceId r) /: "ts" /: toTime t
  where toTime Nothing        = "0"
        toTime (Just theTime) = tshow $ floor theTime

fastlyUrl r@(FastlyRequest { service = ServiceAPI }) =
  https "api.fastly.com" /: "service" /: (serviceId r)
