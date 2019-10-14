module Beefheart.Internal
  ( dequeueOperations
  , fetchMetrics
  , indexingRunner
  , metricsRunner
  , queueMetricsFor
  , queueWatcher
  ) where

import RIO hiding (Handler, catchAny, error, try)
import qualified RIO.HashMap as HM
import RIO.Orphans ()
import RIO.Text (pack)

import Control.Exception.Safe
import Control.Concurrent.STM.TBQueue (flushTBQueue, lengthTBQueue)
import Control.Monad.Loops (iterateM_)
import Control.Retry
import Data.Aeson (FromJSON)
import Data.Time.Clock.POSIX
import qualified Network.HTTP.Client as HTTP
import Network.HTTP.Req hiding (header)

import qualified System.Metrics as EKG
import qualified System.Metrics.Counter as Counter
import qualified System.Metrics.Gauge as Gauge
import qualified System.Metrics.Label as Label

import Beefheart.Elasticsearch
import Beefheart.Fastly
import Beefheart.Types
import Beefheart.Utils

-- |Small utility to keep an eye on our bulk operations queue.
queueWatcher
  :: (MonadIO m)
  => App         -- ^ Our RIO application env
  -> Gauge.Gauge -- ^ The EKG value we'll send measurements to
  -> m b         -- ^ Negligible return type
queueWatcher app gauge = do
  queueLength <- atomically $ lengthTBQueue (appQueue app)
  liftIO $ Gauge.set gauge $ fromIntegral queueLength
  sleepSeconds (metricsWakeup $ appCli app)
  queueWatcher app gauge

-- |Self-contained IO action to regularly fetch and queue up metrics for storage
-- in Elasticsearch.
metricsRunner
  :: (POSIXTime -> IndexName) -- ^ How to name indices
  -> Text -- ^ The Fastly service ID
  -> RIO App () -- ^ Our application monad
metricsRunner indexNamer service = do
  ekg <- asks appEKG
  envVars <- asks appEnv
  cli <- asks appCli
  q <- asks appQueue
  let apiKey = fastlyKey envVars

  counter <- liftIO $ EKG.createCounter (metricN $ "requests-" <> service) ekg

  -- Fetch the service ID's details (to get the human-readable name)
  serviceDetails <- fastlyReq FastlyRequest
    { apiKey       = apiKey
    , service      = ServiceAPI service
    }
  let serviceName = name $ responseBody serviceDetails

  -- Before entering the metrics fetching loop, record the service's details in EKG.
  liftIO $ EKG.createLabel (metricN service) ekg >>= flip Label.set serviceName

  logDebug . display $ "Entering metrics loop for service '" <> serviceName <> "'"
  let
    -- A function that accepts a timestamp and spits back `Analytics` values.
    getMetrics = fetchMetrics counter apiKey service
    -- Helper to take raw `Analytics` and transform them into Elasticsearch
    -- bulk items.
    toDocs = toBulkOperations indexNamer serviceName
  -- iterateM_ executes a monadic action (here, IO) and runs forever,
  -- feeding the return value into the next iteration, which fits well with
  -- our use case: keep hitting Fastly and feed the previous timestamp into
  -- the next request.
  iterateM_ (queueMetricsFor (fastlyPeriod cli) q toDocs getMetrics) 0
    -- In the course of our event loop, we want to notice if something _really_
    -- fatal happens, and tell us why.
    `catchAny` \anyException ->
      logError . display $ "Killing metrics loop for '" <> serviceName
                        <> "'. Encountered: " <> tshow anyException

-- |Higher-order function suitable to be fed into iterate that will be the main
-- metrics retrieval loop. Get a timestamp, fetch some metrics for that
-- timestamp, enqueue them, sleep, repeat.
queueMetricsFor
  :: MonadIO m
  => Int -- ^ Period between API calls
  -> TBQueue BulkOperation -- ^ Our application's metrics queue.
  -> (Analytics -> [BulkOperation]) -- ^ A function to transform our metrics into ES bulk operations
  -> (POSIXTime -> m (JsonResponse Analytics)) -- ^ Function to get metrics for a timestamp
  -> POSIXTime -- ^ The actual metrics timestamp we want
  -> m POSIXTime -- ^ Return the new POSIXTime for the subsequent request
queueMetricsFor period q f getter ts = do
  metrics <- getter ts
  let bulkOperations = f $ responseBody metrics
  atomically $ mapM_ (writeTBQueue q) bulkOperations
  sleepSeconds period
  return $ timestamp $ responseBody metrics

-- |A self-contained indexing runner intended to be run within a thread. Wakes
-- up periodically to bulk index documents that it finds in our queue.
-- indexingRunner
indexingRunner :: RIO App ()
indexingRunner = do
  -- Pull some values out of our application environment
  queue <- asks appQueue
  cliArgs <- asks appCli
  esURI <- asks appESURI

  -- See the comment on dequeueOperations for additional information, but
  -- tl;dr, we should always get _something_ from this action. Note that
  -- Elasticsearch will error out if we try and index nothing ([])
  docs <- atomically $ dequeueOperations queue
  let
    -- This is the indexing action to call on the bulk API. We wrap it a little
    -- so that our retrying library can use it.
    runIndex = \_unusedRetryStatus ->
      either (indexAnalytics docs) (indexAnalytics docs) esURI
  -- Indefinitely retry (on most exceptions) our indexing action.
  bulkResponse <- recovering backoffAndKeepTrying [handler] runIndex
  let indexed = display $ length $ filter isNothing $ map error $ concatMap HM.elems $ items $ responseBody bulkResponse
  logDebug $ "Indexed " <> indexed <> " docs"
  -- Sleep for a time before performing another bulk operation.
  sleepSeconds (esFlushDelay cliArgs)
  indexingRunner
  where
    -- This function is consulted each time an attempt fails to determine
    -- whether to try again (`True`) or let the exception propagate (`False`).
    handler retryStatus = Handler $ \exception -> do
      let retryNote = "Retry attempt " <> tshow (rsIterNumber retryStatus) <> "."
      case exception of
        (VanillaHttpException (HTTP.HttpExceptionRequest _ e)) -> do
          logError . display $
            "Encountered ES indexing error: " <> (tshow e)
            <> ". " <> retryNote
          return True
        (VanillaHttpException (HTTP.InvalidUrlException _ _)) -> do
          logError "Somehow we malformed a URL to Elasticsearch - how did that happen? Bailing out."
          return False
        (JsonHttpException e) -> do
          logError . display $
            "Bad response from Elasticsearch: " <> pack e
            <> ". " <> retryNote
          return True
    -- Our retry policy for ES is to backoff exponentially and cap our retry
    -- limit, so we end up always retrying when we can.
    backoffAndKeepTrying =
      -- Backoff by one second, increasing exponentially
      exponentialBackoff (1_000_000)
      -- But don't wait longer than 10 minutes between attempts
      & capDelay (1_000_000 * 10)

-- |Flush Elasticsearch bulk operations that need to be indexed from our queue.
dequeueOperations
  :: TBQueue BulkOperation -- ^ Our thread-safe queue
  -> STM [BulkOperation]   -- ^ Monadic STM return value
dequeueOperations q = do
  -- STM paradigms in the Haskell world are very powerful but do require some
  -- context - here, if `check` fails, the transaction is restarted (it's a form
  -- of `retry` from the STM library.) However, the runtime will know that we
  -- tried to read from our queue when we bailed out to retry, so upon finding
  -- that the queue is empty, the transaction will then block until the queue is
  -- written to. This is why we don't need to wait/sleep before checking if the
  -- queue is empty again.
  queueIsEmpty <- isEmptyTBQueue q
  checkSTM $ not queueIsEmpty
  flushTBQueue q

-- |Retrieve real-time analytics from Fastly (with retries)
-- fetchMetrics
--   :: Counter.Counter -- ^ An EKG counter to track request activity
--   -> Text            -- ^ Fastly API key
--   -> Text            -- ^ ID of the Fastly service we want to inspect
--   -> POSIXTime       -- ^ Timestamp for analytics
--   -> IO (Either HttpException (JsonResponse Analytics))
fetchMetrics
  :: (MonadHttp m, MonadIO m, FromJSON a)
  => Counter.Counter
  -> Text
  -> Text
  -> POSIXTime
  -> m (JsonResponse a)
fetchMetrics counter key service ts = do
  -- Immediately prior to the Fastly request, hit the counter.
  liftIO $ Counter.inc counter
  -- Perform actual API call to Fastly asking for metrics for a service.
  fastlyReq FastlyRequest
            { apiKey = key
            , service = AnalyticsAPI service ts
            }
