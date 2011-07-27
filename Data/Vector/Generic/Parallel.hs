-- |
-- Module      : Data.Vector.Generic.Parallel
-- Copyright   : [2011] Trevor L. McDonell
-- License     : BSD3
--
-- Maintainer  : Trevor L. McDonell <tmcdonell@cse.unsw.edu.au>
-- Stability   : experimental
-- Portability : non-portable (GHC Extensions)
--

module Data.Vector.Generic.Parallel (

  map, fold, foldMap

) where

import Prelude                                  hiding ( map )

import Control.DeepSeq
import Control.Monad.Par
import Data.List                                ( mapAccumL, foldl' )
import Data.Tuple                               ( swap )
import GHC.Conc                                 ( numCapabilities )
import Data.Vector.Generic                      ( Vector )
import qualified Data.Vector.Generic            as G


-- | Map a function to each element of an array, in parallel.
--
{-# INLINE map #-}
map :: (Vector v a, Vector v b, NFData (v b)) => (a -> b) -> v a -> v b
map f = G.concat . parSplit (G.map f)


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
fold :: (Vector v a, NFData a) => (a -> a -> a) -> a -> v a -> a
fold c z = foldl' c z . parSplit (G.foldl' c z)


-- | A combination of 'map' followed by 'fold', but computed more efficiently.
-- The same restrictions apply to the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: (Vector v a, NFData b) => (a -> b) -> (b -> b -> b) -> b -> v a -> b
foldMap f c z = foldl' c z . parSplit (G.foldl' (flip $ c . f) z)

{-# RULES
"map/map"       forall f g.     map f . map g         = map (f . g)
"fold/map"      forall f c z.   fold c z . map f      = foldMap f c z
"mapFold/map"   forall f g c z. foldMap f c z . map g = foldMap (f . g) c z
  #-}


-- Auxiliary
-- ---------

-- Split a vector into chunks and apply the given operation to each section in
-- parallel. Return the list of partial results.
--
parSplit :: (Vector v a, NFData b) => (v a -> b) -> v a -> [b]
parSplit f vec
  = runPar . parMap f
  . snd . mapAccumL (\v -> swap . flip G.splitAt v) vec
  $ splitChunk n l
  where
    n = auto_partition_factor * numCapabilities
    l = G.length vec

-- Split a range into the given number of chunks as evenly as possible. Returns
-- a list of the size of each chunk.
--
splitChunk :: Int -> Int -> [Int]
splitChunk pieces len =
  replicate remain (step + 1) ++ replicate (pieces - remain) step
  where
    (step, remain) = len `quotRem` pieces

-- How many tasks per process should we aim for.  Higher numbers improve load
-- balance but put more pressure on the scheduler.
--
auto_partition_factor :: Int
auto_partition_factor = 4

