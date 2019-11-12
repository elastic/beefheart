module Beefheart.Utils
  ( abort
  , applicationName
  , backoffThenGiveUp
  , metricN
  , reqCheckResponse
  , sleepSeconds
  , waitSecUntilMin
  )
where

import RIO

import Control.Retry
import Network.HTTP.Client
import Network.HTTP.Types

-- |Log an error and exit the program.
abort
  :: (MonadIO m, Display a)
  => a -- ^ Message to log
  -> m b
abort message = do
  runSimpleApp $ do
    logError . display $ message
  exitFailure

-- |Just so we define it in one place
applicationName :: Text
applicationName = "beefheart"

-- |Initially wait one second, backoff exponentially, then concede to the
-- impossibility of the request if retries reach `seconds` minutes.
backoffThenGiveUp :: Monad m => Int -> RetryPolicyM m
backoffThenGiveUp seconds =
  exponentialBackoff (1 * 1000 * 1000)
  & limitRetriesByDelay (seconds * 1000 * 1000)

-- |`Req` will use this function to determine whether or not to immediately
-- throw an exception. We define our own custom function here since we rely on
-- 404's as an indicator for the presence of resources in our application.
reqCheckResponse :: p -> Response a -> ByteString -> Maybe HttpExceptionContent
reqCheckResponse _ response preview =
  let scode = statusCode $ responseStatus response
  in if (200 <= scode && scode < 300) || scode == 404 || scode == 503
     then Nothing
     else Just (StatusCodeException (void response) preview)

-- |Helper to create EKG metric names
metricN :: Text -> Text
metricN n = applicationName <> "." <> n

-- |Sleep for a given number of seconds in a given thread.
sleepSeconds :: (MonadIO m)
             => Int   -- ^ Seconds to sleep
             -> m ()
sleepSeconds = threadDelay . (*) (1000 * 1000)

-- |Pause for one second between retries giving up after `minutes` minutes.
waitSecUntilMin :: Monad m => Int -> RetryPolicyM m
waitSecUntilMin minutes =
  constantDelay (1 * 1000 * 1000)
  & limitRetriesByCumulativeDelay (60 * minutes * 1000 * 1000)
