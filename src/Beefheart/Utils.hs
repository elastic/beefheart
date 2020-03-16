module Beefheart.Utils
  ( abort
  , applicationName
  , backoffThenGiveUp
  , metricN
  , reqCheckResponse
  , reqRetryJudge
  , sleepSeconds
  , waitSecUntilMin
  )
where

import RIO

import Control.Retry
import Network.HTTP.Client
import Network.HTTP.Req (HttpConfig(..), defaultHttpConfig)
import Network.HTTP.Types

-- |Log an error and exit the program.
abort
  :: (MonadIO m, Display a)
  => a -- ^ Message to log
  -> m b
abort message = do
  runSimpleApp $
    logError . display $ message
  exitFailure

-- |Just so we define it in one place
applicationName :: IsString a => a
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
  -- This predicate is largely based upon observed responses from the Fastly
  -- API. That is: when we see Fastly return one of these response codes, don't
  -- immediately throw a synchronous exception, and potentially let the response
  -- be retried later, depending on our retry policy.
  in if (200 <= scode && scode < 300) || scode `elem` 404 : retryableCodes
     then Nothing
     else Just (StatusCodeException (void response) preview)

-- |To be used to determine whether an HTTP request should be retried. A
-- `RetryStatus` comes out of the `retry` package with some data like the number
-- of retries so far, and the `Response b` is just the HTTP response value. We
-- could do more to inspect the body, but status codes are easy to make
-- decisions on.
reqRetryJudge :: forall b . RetryStatus -> Response b -> Bool
reqRetryJudge retryStatus response = myJudge || defaultJudge
  where
    myJudge = statusCode (responseStatus response) `elem` retryableCodes
    -- We're going to logical OR the default judge with our own.
    defaultJudge = httpConfigRetryJudge defaultHttpConfig retryStatus response

-- |Define the custom response codes we want to retry in a single place.
retryableCodes :: [Int]
retryableCodes =
  -- These are simple remote "I'm not available _right now_" response codes.
  [ 502, 503 ]

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
