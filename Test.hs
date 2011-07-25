{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Main where

import Data.Vector.Unboxed                      ( Vector, Unbox )
import qualified Data.Vector.Unboxed            as U
import qualified Data.Vector.Unboxed.Parallel   as P

import Data.Word
import Control.DeepSeq
import Control.Monad
import Criterion.Main
import System.Random.MWC
import Test.QuickCheck
import Text.Printf

instance Unbox a => NFData (U.Vector a)


{-# INLINE collatzLen #-}
collatzLen :: Int -> Word32 -> Int
collatzLen c 1 = c
collatzLen c n | n `mod` 2 == 0 = collatzLen (c+1) $ n `div` 2
               | otherwise      = collatzLen (c+1) $ 3*n+1

{-# INLINE hailstone #-}
hailstone :: Word32 -> (Int, Word32)
hailstone n = (collatzLen 1 n, n)


randomVector :: Int -> IO (Vector Float)
randomVector n
  = withSystemRandom
  $ \gen -> uniformVector gen n :: IO (Vector Float)


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
    in  U.toList (P.map hailstone vec) .==. map hailstone xs

prop_fold :: Property
prop_fold =
  forAll arbitrary $ \(xs :: [Int]) ->
    let vec = U.fromList xs
    in  P.fold (+) 0 vec .==. sum xs

prop_foldMap :: Property
prop_foldMap =
  forAll (listOf (arbitrary `suchThat` (>0))) $ \xs ->
    let vec = U.fromList xs
    in  P.foldMap hailstone max (1,1) vec .==. foldl max (1,1) (map hailstone xs)


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
  let v1 = U.enumFromN 2 100000
  in do
--    print . P.foldMap hailstone max (1,1)      $ v1
--    print . P.fold max (1,1) . P.map hailstone $ v1

    {--}
    v2 <- randomVector 1000000
    ok <- runTests [("map",     prop_map)
                   ,("fold",    prop_fold)
                   ,("foldMap", prop_foldMap) ]

    when ok $
      defaultMain
        [ bgroup "" [ bench "map/hailstone"     $ nf (P.map hailstone) v1
                    , bench "fold/sum"          $ nf (P.fold (+) 0) v2
                    , bench "foldMap/collatz"   $ nf (P.foldMap hailstone max (1,1)) v1
                    , bench "fold.map/collatz"  $ nf (P.fold max (1,1) . P.map hailstone) v1
                    ]
        ]
    --}

