module Beefheart.Utils
  ( withRetries
  )
where

import ClassyPrelude
import Control.Retry
import Data.Either

-- |Given a monadic action, perform it with retries with a default policy.
withRetries :: MonadIO m
            => m (Either a b)
            -> m (Either a b)
withRetries m = do
  retrying withPolicy (const $  return . isLeft) $ \_ -> m
  where -- Initially wait one second, backoff exponentially, then concede to the
        -- impossibility of the request if retries reach 5 minutes.
        withPolicy = limitRetriesByDelay (60 * 5 * 1000 * 1000)
                      $ exponentialBackoff (1 * 1000 * 1000)
