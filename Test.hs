{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.Vector                    as SV
import qualified Data.Vector.Unboxed            as SU
import qualified Data.Vector.Parallel           as V
import qualified Data.Vector.Unboxed.Parallel   as U

import Data.Word
import Control.Monad
import Control.DeepSeq
import Criterion.Main
import System.Random.MWC
import Test.QuickCheck
import Text.Printf

instance NFData a => NFData (V.Vector a) where
  rnf = V.foldl' (flip deepseq) ()

instance U.Unbox a => NFData (U.Vector a)


{-# INLINE collatzLen #-}
collatzLen :: Int -> Word32 -> Int
collatzLen c 1 = c
collatzLen c n | n `mod` 2 == 0 = collatzLen (c+1) $ n `div` 2
               | otherwise      = collatzLen (c+1) $ 3*n+1

{-# INLINE hailstone #-}
hailstone :: Word32 -> (Int, Word32)
hailstone n = (collatzLen 1 n, n)


{-# NOINLINE randomVector #-}
randomVector :: Int -> IO (U.Vector Float)
randomVector n
  = withSystemRandom
  $ \gen -> U.replicateM n (uniformR (-1,1) gen :: IO Float)


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

prop_fold_map :: Property
prop_fold_map =
  forAll (listOf (arbitrary `suchThat` (>0))) $ \xs ->
    let vec = U.fromList xs
    in  U.fold max (1,1) (U.map hailstone vec) .==. foldl max (1,1) (map hailstone xs)


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
main = do
  u2 <- randomVector 1000000
  let u1 = U.enumFromN 2 100000
      v1 = V.enumFromN 2 100000
      v2 = V.convert u2

  -- derived from a common implementation, so just test the (slightly quicker)
  -- unboxed instantiations
  --
  ok <- runTests [("map",             prop_map)
                 ,("fold",            prop_fold)
                 ,("foldMap",         prop_foldMap)
                 ,("fold . map",      prop_fold_map) ]

  -- now run performance tests for boxed and unboxed vectors
  --
  when ok $
    defaultMain
      [ bgroup "seq-boxed"
          [ bench "map"         $ nf (SV.map hailstone) v1
          , bench "fold"        $ nf (SV.foldl' (+) 0) v2
          , bench "fold . map"  $ nf (SV.foldl' max (1,1) . SV.map hailstone) v1
          ]

      , bgroup "seq-unboxed"
          [ bench "map"         $ nf (SU.map hailstone) u1
          , bench "fold"        $ nf (SU.foldl' (+) 0) u2
          , bench "fold . map"  $ nf (SU.foldl' max (1,1) . SU.map hailstone) u1
          ]

      , bgroup "par-boxed"
          [ bench "map"         $ nf (V.map hailstone) v1
          , bench "fold"        $ nf (V.fold (+) 0) v2
          , bench "fold.map"    $ nf (V.fold max (1,1) . V.map hailstone) v1
          , bench "foldMap"     $ nf (V.foldMap hailstone max (1,1)) v1
          ]

      , bgroup "par-unboxed"
          [ bench "map"         $ nf (U.map hailstone) u1
          , bench "fold"        $ nf (U.fold (+) 0) u2
          , bench "fold.map"    $ nf (U.fold max (1,1) . U.map hailstone) u1
          , bench "foldMap"     $ nf (U.foldMap hailstone max (1,1)) u1
          ]
      ]

