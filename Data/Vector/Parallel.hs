{-# LANGUAGE NoImplicitPrelude #-}
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

  -- * Parallel collective operations
  map, fold, foldMap,

  -- * Re-exported for convenience
  module Data.Vector

) where

import Data.Vector                              hiding ( map )
import qualified Data.Vector.Generic.Parallel   as G


-- | Map a function to each element of an array, in parallel.
--
{-# INLINE map #-}
map :: (a -> b) -> Vector a -> Vector b
map = G.map


-- | Reduce an array to a single value. The combination function must be an
-- associative operation, and the stating element must be neutral with respect
-- to this operator; i.e. the pair must form a monoid. For example, @0@ is
-- neutral with respect to @(+)@, as @0 + a = a@.
--
-- These restrictions are required to support efficient parallel evaluation, as
-- the starting value may be used many times depending on the number of threads.
--
{-# INLINE fold #-}
fold :: (a -> a -> a) -> a -> Vector a -> a
fold = G.fold


-- | A combination of 'map' followed by 'fold'. The same restrictions apply to
-- the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: (a -> b) -> (b -> b -> b) -> b -> Vector a -> b
foldMap = G.foldMap

