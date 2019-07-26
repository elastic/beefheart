module Beefheart.Utils
  ( backoffThenGiveUp
  , ifLeft
  , sleepSeconds
  , withRetries
  )
where

import ClassyPrelude
import Control.Concurrent
import Control.Retry
import Data.Either
import Network.HTTP.Req

-- |Given a monadic action, perform it with retries with a default policy.
withRetries
  :: MonadIO m
  => (RetryStatus -> b -> m Bool)
  -> m b
  -> m b
withRetries check m =
  retrying backoffThenGiveUp check $ const m

-- Initially wait one second, backoff exponentially, then concede to the
-- impossibility of the request if retries reach 5 minutes.
backoffThenGiveUp :: Monad m => RetryPolicyM m
backoffThenGiveUp = limitRetriesByDelay (60 * 5 * 1000 * 1000)
                    $ exponentialBackoff (1 * 1000 * 1000)

ifLeft
  :: (MonadIO m, HttpResponse a)
  => RetryStatus
  -> Either HttpException a
  -> m Bool
ifLeft = const (return . isLeft)

-- |Sleep for a given number of seconds in a given thread.
sleepSeconds :: Int   -- ^ Seconds to sleep
             -> IO ()
sleepSeconds = threadDelay . (*) (1000 * 1000)
