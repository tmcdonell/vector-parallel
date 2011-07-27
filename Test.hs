{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.Vector.Generic		as G
import qualified Data.Vector.Parallel           as V
import qualified Data.Vector.Unboxed.Parallel   as U
import qualified Data.Vector                    as V hiding ( map )
import qualified Data.Vector.Unboxed            as U hiding ( map )

import Data.Word
import Control.Monad
import Criterion.Main
import System.Random.MWC
import Test.QuickCheck
import Text.Printf


{-# INLINE collatzLen #-}
collatzLen :: Int -> Word32 -> Int
collatzLen c 1 = c
collatzLen c n | n `mod` 2 == 0 = collatzLen (c+1) $ n `div` 2
               | otherwise      = collatzLen (c+1) $ 3*n+1

{-# INLINE hailstone #-}
hailstone :: Word32 -> (Int, Word32)
hailstone n = (collatzLen 1 n, n)


randomVector :: forall v a. (G.Vector v a, Variate a, Floating a) => Int -> IO (v a)
randomVector n
  = withSystemRandom
  $ \gen -> G.replicateM n (uniformR (-1,1) gen :: IO a)


--
-- Properties ------------------------------------------------------------------
--
infix 4 .==.
(.==.) :: (Show a, Eq a) => a -> a -> Property
(.==.) ans ref = printTestCase message (ref == ans)
  where
    message = unlines ["*** Expected:", show ref
                      ,"*** Received:", show ans ]

prop_map :: Property
prop_map =
  forAll (listOf (arbitrary `suchThat` (>0))) $ \xs ->
    let vec = U.fromList xs
    in  U.toList (U.map hailstone vec) .==. map hailstone xs

prop_fold :: Property
prop_fold =
  forAll arbitrary $ \(xs :: [Int]) ->
    let vec = U.fromList xs
    in  U.fold (+) 0 vec .==. sum xs

prop_foldMap :: Property
prop_foldMap =
  forAll (listOf (arbitrary `suchThat` (>0))) $ \xs ->
    let vec = U.fromList xs
    in  U.foldMap hailstone max (1,1) vec .==. foldl max (1,1) (map hailstone xs)


runTests :: Testable prop => [(String, prop)] -> IO Bool
runTests xs = foldl (\r x -> r && ok x) True `fmap` mapM test xs
  where
    ok (Success _ _ _) = True
    ok _               = False

test :: Testable prop => (String, prop) -> IO Result
test (name, prop) = printf "%-30s: " name >> quickCheckResult prop


--
-- main ------------------------------------------------------------------------
--
main :: IO ()
main =
  let u1 = U.enumFromN 2 100000
      v1 = V.enumFromN 2 100000
  in do
    u2 <- randomVector 10000000 :: IO (U.Vector Float)
    v2 <- randomVector 10000000 :: IO (V.Vector Float)

    -- derived from a common implementation, so just test the (slightly quicker)
    -- unboxed instantiations
    --
    ok <- runTests [("map",     prop_map)
                   ,("fold",    prop_fold)
                   ,("foldMap", prop_foldMap) ]

    -- now run performance tests for boxed and unboxed vectors
    --
    when ok $
      defaultMain
        [ bgroup "boxed"   [ bench "map/hailstone"     $ nf (V.map hailstone) v1
                           , bench "fold/sum"          $ nf (V.fold (+) 0) v2
                           , bench "foldMap/collatz"   $ nf (V.foldMap hailstone max (1,1)) v1
                           , bench "fold.map/collatz"  $ nf (V.fold max (1,1) . V.map hailstone) v1
                           ]

        , bgroup "unboxed" [ bench "map/hailstone"     $ nf (U.map hailstone) u1
                           , bench "fold/sum"          $ nf (U.fold (+) 0) u2
                           , bench "foldMap/collatz"   $ nf (U.foldMap hailstone max (1,1)) u1
                           , bench "fold.map/collatz"  $ nf (U.fold max (1,1) . U.map hailstone) u1
                           ]
        ]

