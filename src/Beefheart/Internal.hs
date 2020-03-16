module Beefheart.Internal
  ( dequeueOperations
  , fetchMetrics
  , indexingRunner
  , metricsRunner
  , queueMetricsFor
  , queueWatcher
  ) where

import RIO hiding (Handler, catch, error, try)
import qualified RIO.HashMap as HM
import RIO.Orphans ()
import RIO.Text (pack)

import Control.Exception.Safe
import Control.Concurrent.STM.TBQueue (flushTBQueue, lengthTBQueue)
import Control.Monad.Loops (iterateM_)
import Control.Retry
import Data.Aeson (FromJSON)
import Data.Time.Clock.POSIX
import Katip
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

-- |Self-contained function suitable to live in a standalone thread that regularly fetches and queues up metrics for storage
-- in Elasticsearch.
metricsRunner
  :: (POSIXTime -> IndexName) -- ^ How to name indices
  -> Text -- ^ The Fastly service ID
  -> RIO App () -- ^ Our application monad
metricsRunner indexNamer service = katipAddNamespace "metrics" $ do
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

  logLocM DebugS . ls $ "Entering metrics loop for service '" <> serviceName <> "'"
  let
    -- A function that accepts a timestamp and spits back `Analytics` values.
    getMetrics = fetchMetrics counter apiKey service
    -- Helper to take raw `Analytics` and transform them into Elasticsearch
    -- bulk items.
    toDocs = toBulkOperations indexNamer serviceName
    -- In practice, the `req` library does miss _some_ exceptions that we would
    -- want to retry, because the library assumes that a request will at least
    -- reach the point that we can pull out a status code, but results like
    -- timeouts can't get that far. So we do use a small `retry` wrapper to get
    -- ahead of some exceptions we want to natively retry before admitting
    -- defeat. Also note that this means we'll wrap our IO action thread in a
    -- retry higher-order function before a traditional `catch`-style exception
    -- handler, so when a request fails, we run it through a quick retry check,
    -- then only let it past that gate once we're sure it's time to kill
    -- something.
    shouldRetry retryStatus = Handler $ \case
      -- A simple timeout - these are safe to retry.
      (VanillaHttpException (HTTP.HttpExceptionRequest _request HTTP.ResponseTimeout)) -> do
        logLocM WarningS . ls $ "Encountered HTTP response timeout fetching metrics for '"
          <> serviceName <> "'. Retry attempt: " <> tshow (rsIterNumber retryStatus)
        return True
      -- Similarly, we don't consider connection timeouts catastrophic events.
      (VanillaHttpException (HTTP.HttpExceptionRequest _request HTTP.ConnectionTimeout)) -> do
        logLocM WarningS . ls $ "Encountered connection timeout fetching metrics for '"
          <> serviceName <> "'. Retry attempt: " <> tshow (rsIterNumber retryStatus)
        return True
      _ -> do
        logLocM WarningS "Encountered a non-recoverable error. Bailing out."
        return False
    -- Our exception handler for HTTP exceptions. Elsewhere, we define the
    -- conditions for why we would want to retry something, so if we hit this
    -- point, it stands to reason that the exception is unrecoverable, so offer
    -- as much information as possible before ending this process/thread. Note
    -- that all the possible exceptions to follow will come from HTTP calls to
    -- Fastly, not Elasticsearch.
    --
    -- A bad response that we either don't want to retry, or has been retried
    -- too many times. Log the reason, then we end the loop - other threads live
    -- on. This might occur, for example, if the Fastly service disappears
    -- unexpectedly.
    handleException (VanillaHttpException (HTTP.HttpExceptionRequest _request exception)) =
      logLocM WarningS . ls $ "Killing metrics loop for '" <> serviceName
                        <> "'. Encountered: " <> tshow exception
    -- This means Fastly returned malformed json. Assume something is awry
    -- upstream, let the process die by re-throwing and thus tearing all other
    -- threads down along with it.
    handleException e@(JsonHttpException reason) = do
      logLocM ErrorS . ls $ "Fastly handed us back bad json: " <> reason
      throw e
    -- We've malformed our request URL? Something is _wrong_ in our code, log
    -- the reason and similarly let the exception propagate up because we don't
    -- want to spew malformed content.
    handleException e@(VanillaHttpException (HTTP.InvalidUrlException url reason)) = do
      logLocM ErrorS . ls $ "We malformed this URL: " <> url <> ". Reason: " <> reason
      throw e
  -- We wrap the whole thing in `recovering`, because we want to retry a few
  -- situations at the top-level before falling through to the exception
  -- handler.
  recovering
    (backoffThenGiveUp (cli & httpRetryLimit)) -- how to back off
    [shouldRetry] -- "which exceptions should we retry?"
    -- iterateM_ executes a monadic action (here, IO) and runs forever,
    -- feeding the return value into the next iteration, which fits well with
    -- our use case: keep hitting Fastly and feed the previous timestamp into
    -- the next request.
    (\_ -> iterateM_ (queueMetricsFor (fastlyPeriod cli) q toDocs getMetrics) 0)
      -- Finally, if `recovering` bails out, this is our cleanup function. In
      -- practice, we just interrogate the exception to provide some logging
      -- explanation for terminating this thread (and potentially the whole
      -- process).
      `catch` handleException

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
indexingRunner =  do
  -- Pull some values out of our application environment
  queue <- asks appQueue
  cliArgs <- asks appCli
  esURI <- asks appESURI

  -- Adds some scoping to our log messages that occur later
  katipAddNamespace "indexer" $
    -- Index documents indefinitely
    forever $ do
      -- Attempt to dequeue documents to index, then...
      bulkIndexer backoffAndKeepTrying handler esURI queue
      -- ...sleep the thread for a time.
      sleepSeconds (esFlushDelay cliArgs)
  where
    -- This function is consulted each time an attempt fails to determine
    -- whether to try again (`True`) or let the exception propagate (`False`).
    -- This encapsulating function (`indexingRunner`) just talks to
    -- Elasticsearch, so we'll only need to handle those cases in our exception
    -- handler.
    handler retryStatus = Handler $ \exception -> do
      let retryNote = "Retry attempt " <> tshow (rsIterNumber retryStatus) <> "."
      case exception of
        (VanillaHttpException (HTTP.HttpExceptionRequest _ e)) -> do
          logLocM WarningS . ls $
            "Encountered ES indexing error: " <> tshow e
            <> ". " <> retryNote
          return True
        (VanillaHttpException (HTTP.InvalidUrlException _ _)) -> do
          logLocM ErrorS $ ls ("Somehow we malformed a URL to Elasticsearch - how did that happen? Bailing out." :: Text)
          return False
        (JsonHttpException e) -> do
          logLocM WarningS . ls $
            "Bad response from Elasticsearch: " <> pack e
            <> ". " <> retryNote
          return True
    -- Our retry policy for ES is to backoff exponentially and cap our retry
    -- limit, so we end up always retrying when we can.
    backoffAndKeepTrying =
      -- Backoff by one second, increasing exponentially
      exponentialBackoff 1000000
      -- But don't wait longer than 10 minutes between attempts
      & capDelay (1000000 * 10)

-- |Pull bulk operations out of a queue and index them.
bulkIndexer
  :: (MonadHttp m, MonadMask m, KatipContext m)
  => RetryPolicyM m -- ^ Dictates how retries are performed
  -> (RetryStatus -> Handler m Bool) -- ^ Retry logic/exception handler
  -> ElasticsearchURI -- ^ ES Connection details
  -> TBQueue BulkOperation -- ^ Our document queue
  -> m ()
bulkIndexer retryStrategy handler es queue = do
  -- See the comment on dequeueOperations for additional information, but
  -- tl;dr, we should always get _something_ from this action. Note that
  -- Elasticsearch will error out if we try and index nothing ([])
  docs <- atomically $ dequeueOperations queue
  let
    -- This is the indexing action to call on the bulk API. We wrap it a
    -- little so that our retrying library can use it.
    runIndex _unusedRetryStatus =
      either (indexAnalytics docs) (indexAnalytics docs) es
  -- Indefinitely retry (on most exceptions) our indexing action.
  bulkResponse <- recovering retryStrategy [handler] runIndex
  let indexed = length . filter isNothing . map error . concatMap HM.elems . items . responseBody
      logMetric = MetricIndexed $ indexed bulkResponse
  -- Nest our debug logging message underneath a small black of code that
  -- tacks on a metrics value onto our log message. This becomes important if
  -- the user wants to log in json, as they'll end up with nicely formatted
  -- fields for metrics.
  katipAddContext logMetric $
    -- The numbers are all we're interested in, so leave the textual string
    -- empty
    logLocM DebugS mempty

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
fetchMetrics
  :: (MonadHttp m, MonadIO m, FromJSON a)
  => Counter.Counter    -- ^ An EKG counter to track request activity
  -> Text               -- ^ Fastly API key
  -> Text               -- ^ ID of the Fastly service we want to inspect
  -> POSIXTime          -- ^ Timestamp for analytics
  -> m (JsonResponse a) -- ^ We expect json metrics back
fetchMetrics counter key service ts = do
  -- Immediately prior to the Fastly request, hit the counter.
  liftIO $ Counter.inc counter
  -- Perform actual API call to Fastly asking for metrics for a service.
  fastlyReq FastlyRequest
            { apiKey = key
            , service = AnalyticsAPI service ts
            }
