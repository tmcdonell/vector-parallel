-- |
-- Module      : Data.Vector.Parallel
-- Copyright   : [2011] Trevor L. McDonell
-- License     : BSD3
--
-- Maintainer  : Trevor L. McDonell <tmcdonell@cse.unsw.edu.au>
-- Stability   : experimental
-- Portability : non-portable (GHC Extensions)
--

module Data.Vector.Parallel (

  map, fold, foldMap

) where

import Prelude                                  hiding ( map )

import Control.DeepSeq
import Control.Monad.Par
import Data.List                                ( mapAccumL, foldl' )
import Data.Tuple                               ( swap )
import Data.Vector                              ( Vector )
import GHC.Conc                                 ( numCapabilities )

import qualified Data.Vector                    as V


instance NFData a => NFData (Vector a) where
  rnf v = V.foldl' (\x y -> y `deepseq` x) () v


-- | Map a function to each element of an array.
--
{-# INLINE map #-}
map :: NFData b => (a -> b) -> Vector a -> Vector b
map f
  = V.concat
  . parSplit (V.map f)


-- | Reduce an array to a single value. The combination function must be an
-- associative operation, and the stating element must be neutral with respect
-- to this operator; i.e. the pair must form a monoid.
--
-- For example, @0@ is neutral with respect to @(+)@, as @0 + a = a@.
--
-- These restrictions are required to support efficient parallel evaluation, as
-- the starting value may be used many times depending on the number of threads.
--
{-# INLINE fold #-}
fold :: NFData a => (a -> a -> a) -> a -> Vector a -> a
fold c z
  = foldl' c z
  . parSplit (V.foldl' c z)


-- | A combination of 'map' followed by 'fold', but computed more efficiently.
-- The same restrictions apply to the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: NFData b => (a -> b) -> (b -> b -> b) -> b -> Vector a -> b
foldMap f c z
  = foldl' c z
  . parSplit (V.foldl' (flip $ c . f) z)

{-# RULES
      "map/fold"        forall f c z. fold c z . map f = foldMap f c z
  #-}


-- Auxiliary
-- ---------

-- Split a vector into chunks and apply the given operation to each section in
-- parallel. Return the list of partial results.
--
{-# INLINE parSplit #-}
parSplit :: NFData b => (Vector a -> b) -> Vector a -> [b]
parSplit f vec
  = runPar . parMap f
  . snd . mapAccumL (\v -> swap . flip V.splitAt v) vec
  $ splitChunk n l
  where
    n = auto_partition_factor * numCapabilities
    l = V.length vec


-- How many tasks per process should we aim for.  Higher numbers improve load
-- balance but put more pressure on the scheduler.
--
{-# INLINE auto_partition_factor #-}
auto_partition_factor :: Int
auto_partition_factor = 4


-- Split a range into the given number of chunks as evenly as possible. Returns
-- a list of the size of each successive chunk.
--
{-# INLINE splitChunk #-}
splitChunk :: Int -> Int -> [Int]
splitChunk pieces len =
  replicate remain (step + 1) ++ replicate (pieces - remain) step
  where
    (step, remain) = len `quotRem` pieces

