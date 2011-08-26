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

  map, fold, foldMap,
  map_, fold_, foldMap_

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
map f vec = splitjoin (G.map f) G.concat vec


-- | Reduce an array to a single value. The combination function must be an
-- associative operation, and the stating element must be neutral with respect
-- to this operator; i.e. the pair must form a monoid. For example, @0@ is
-- neutral with respect to @(+)@, as @0 + a = a@.
--
-- These restrictions are required to support efficient parallel evaluation, as
-- the starting value may be used many times depending on the number of threads.
--
{-# INLINE fold #-}
fold :: (Vector v a, NFData a) => (a -> a -> a) -> a -> v a -> a
fold c z vec = splitjoin (G.foldl' c z) (foldl' c z) vec


-- | A combination of 'map' followed by 'fold'. The same restrictions apply to
-- the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: (Vector v a, NFData b) => (a -> b) -> (b -> b -> b) -> b -> v a -> b
foldMap f c z vec = splitjoin (G.foldl' f' z) (foldl' c z) vec
  where
    f' k x = k `c` f x


-- | Like 'map' but only head strict, not fully strict.
--
{-# INLINE map_ #-}
map_ :: (Vector v a, Vector v b) => (a -> b) -> v a -> v b
map_ f vec = splitjoin_ (G.map f) G.concat vec

-- | Like 'fold' but only head strict, not fully strict.
--
{-# INLINE fold_ #-}
fold_ :: Vector v a => (a -> a -> a) -> a -> v a -> a
fold_ c z vec = splitjoin_ (G.foldl' c z) (foldl' c z) vec

-- | Like 'foldMap' but only head strict, not fully strict.
--
{-# INLINE foldMap_ #-}
foldMap_ :: Vector v a => (a -> b) -> (b -> b -> b) -> b -> v a -> b
foldMap_ f c z vec = splitjoin_ (G.foldl' f' z) (foldl' c z) vec
  where
    f' k x = k `c` f x


-- Auxiliary
-- ---------

-- Split a vector into chunks, apply the given operation to each section in
-- parallel, then join the chunks to yield the final result.
--
{-# INLINE [1] splitjoin #-}
splitjoin :: (Vector v a, NFData b) => (v a -> b) -> ([b] -> b) -> v a -> b
splitjoin f g vec
  | numCapabilities == 1 = f vec
  | otherwise            = runPar $ parJoin g =<< parSplit f vec

{-# INLINE [1] splitjoin_ #-}
splitjoin_ :: Vector v a => (v a -> b) -> ([b] -> b) -> v a -> b
splitjoin_ f g vec
  | numCapabilities == 1 = f vec
  | otherwise            = runPar $ parJoin_ g =<< parSplit_ f vec

{-# RULES
"split/join"    forall f g p x v. splitjoin  f p (splitjoin  g x v)     = splitjoin  (f . g) p v
"split_/join_"  forall f g p x v. splitjoin_ f p (splitjoin_ g x v)     = splitjoin_ (f . g) p v
  #-}


-- Split a vector into chunks and apply the given operation to each section in
-- parallel. Return the list of partial results.
--
parSplit :: (Vector v a, NFData b) => (v a -> b) -> v a -> Par [b]
parSplit f = parMap f . splitVec

parJoin :: NFData b => ([b] -> b) -> [b] -> Par b
parJoin _ [x] = return x
parJoin c xs  = parJoin c =<< parMap c (splitList xs)

-- Like 'parSplit', but the function applied to the vector is only head strict,
-- not fully strict.
--
parSplit_ :: Vector v a => (v a -> b) -> v a -> Par [b]
parSplit_ f = parMap_ f . splitVec

parJoin_ :: ([b] -> b) -> [b] -> Par b
parJoin_ _ [x] = return x
parJoin_ c xs  = parJoin_ c =<< parMap_ c (splitList xs)

pval_ :: a -> Par (IVar a)
pval_ = spawn_ . return

parMap_ :: (a -> b) -> [a] -> Par [b]
parMap_ f xs = mapM (pval_ . f) xs >>= mapM get

-- Split a vector into evenly sized chunks.
--
splitVec :: Vector v a => v a -> [v a]
splitVec vec = snd $ mapAccumL (\v -> swap . flip G.splitAt v) vec cut
  where
    len            = G.length vec
    pieces         = auto_partition_factor * numCapabilities
    (step, remain) = len `quotRem` pieces
    cut            = replicate remain (step + 1) ++ replicate (pieces - remain) step

splitList :: [a] -> [[a]]
splitList [] = []
splitList xs = case splitAt 2 xs of (a,b) -> a : splitList b


-- How many tasks per process should we aim for.  Higher numbers improve load
-- balance but put more pressure on the scheduler.
--
auto_partition_factor :: Int
auto_partition_factor = 4

