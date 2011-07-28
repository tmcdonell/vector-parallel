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
map f = G.concat . parSplit (G.map f)


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
fold = foldMap id


-- | A combination of 'map' followed by 'fold'. The same restrictions apply to
-- the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: (Vector v a, NFData b) => (a -> b) -> (b -> b -> b) -> b -> v a -> b
foldMap f c z = foldl' c z . parSplit (G.foldl' (flip (c . f)) z)


-- | Like 'map' but only head strict, not fully strict.
--
{-# INLINE map_ #-}
map_ :: (Vector v a, Vector v b) => (a -> b) -> v a -> v b
map_ f = G.concat . parSplit_ (G.map f)

-- | Like 'fold' but only head strict, not fully strict.
--
{-# INLINE fold_ #-}
fold_ :: Vector v a => (a -> a -> a) -> a -> v a -> a
fold_ = foldMap_ id

-- | Like 'foldMap' but only head strict, not fully strict.
--
{-# INLINE foldMap_ #-}
foldMap_ :: Vector v a => (a -> b) -> (b -> b -> b) -> b -> v a -> b
foldMap_ f c z = foldl' c z . parSplit_ (G.foldl' (flip (c . f)) z)


-- Auxiliary
-- ---------

-- Split a vector into chunks and apply the given operation to each section in
-- parallel. Return the list of partial results.
--
parSplit :: (Vector v a, NFData b) => (v a -> b) -> v a -> [b]
parSplit f = runPar . parMap f . splitVec

-- Like 'parSplit', but the function applied to the vector is only head strict,
-- not fully strict.
--
parSplit_ :: Vector v a => (v a -> b) -> v a -> [b]
parSplit_ f = runPar . parMap_ f . splitVec

pval_ :: a -> Par (IVar a)
pval_ = spawn_ . return

parMap_ :: (a -> b) -> [a] -> Par [b]
parMap_ f xs = mapM (pval_ . f) xs >>= mapM get


-- Split a vector into evenly sized chunks.
--
splitVec :: Vector v a => v a -> [v a]
splitVec vec
  = snd $ mapAccumL (\v -> swap . flip G.splitAt v) vec cut
  where
    len            = G.length vec
    pieces         = auto_partition_factor * numCapabilities
    (step, remain) = len `quotRem` pieces
    cut            = replicate remain (step + 1) ++ replicate (pieces - remain) step


-- How many tasks per process should we aim for.  Higher numbers improve load
-- balance but put more pressure on the scheduler.
--
auto_partition_factor :: Int
auto_partition_factor = 4


{-# RULES
"map/map"       forall f g.     map f . map g         = map (f . g)
"fold/map"      forall f c z.   fold c z . map f      = foldMap f c z
"mapFold/map"   forall f g c z. foldMap f c z . map g = foldMap (f . g) c z

"map_/map_"     forall f g.     map_ f . map_ g         = map_ (f . g)
"fold_/map_"    forall f c z.   fold_ c z . map_ f      = foldMap_ f c z
"mapFold_/map_" forall f g c z. foldMap_ f c z . map_ g = foldMap_ (f . g) c z
  #-}

