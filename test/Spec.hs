import RIO hiding (assert)
import RIO.Orphans ()

import Control.Retry
import Data.Aeson
import Network.HTTP.Req
import qualified Network.HTTP.Req as Req
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck

import Beefheart
import Util

main :: IO ()
main = do
  defaultMain tests

-- |High-level test entrypoint for unit tests and integration tests.
tests :: TestTree
tests =
  testGroup "Tests"
  [ unitTests
  , integrationTests
  ]

-- |Simple unit tests
unitTests :: TestTree
unitTests =
  testGroup "Unit tests"
  [ testCase "Aeson merging" $
    mergeAeson
    [ object [ "foo" .= ("bar" :: Text) ]
    , object [ "baz" .= ("bar" :: Text) ]
    ] @?= object [ "foo" .= ("bar" :: Text) , "baz" .= ("bar" :: Text) ]
  ]

-- |Tests that do a bit more, i.e. talk to Elasticsearch to verify indexing
-- works.
integrationTests :: TestTree
integrationTests =
  withResource (elasticsearchContainer "7.4.0") containerCleanup $ \_cid ->
    testGroup "Integration Tests"
    -- First, confirm the API is responsive.
    [ testCase "Elasticsearch is UP" $ do
        resp <- recoverAll (waitSecUntilMin 1) $ \_unusedRetryInfo ->
          rr $ req GET (http "localhost") NoReqBody ignoreResponse (Req.port 9200)
        responseStatusCode resp @?= 200
    , after AllSucceed "Elasticsearch is UP" $
    -- Then, try installing the index template.
      testCase "Install template" $ do
        response <- rr $ bootstrapElasticsearch False 10 10 "fastly" (http "localhost", Req.port 9200)
        responseStatusCode response @?= 200
    , after AllSucceed "Install template" $
    -- Finally, try indexing a set of random `Analytics`
      testCase "Documents can be indexed" $ do
        let toBulk = toBulkOperations "fastly" "%Y" "myservice"
        analytics <- (generate $ listOf1 arbitrary) :: IO ([Analytics])
        let documents = analytics >>= toBulk
        bulkResp <- rr $ indexAnalytics documents (http "localhost", Req.port 9200)
        let esResp = Req.responseBody bulkResp
        assertBool "Indexing errors" (not $ bulkErrors esResp)
        assertEqual "Missing documents" (length documents) (length $ items esResp)
    ]
  where rr = runReq defaultHttpConfig { httpConfigCheckResponse = reqCheckResponse
                                      , httpConfigRetryPolicy = waitSecUntilMin 1
                                      }
