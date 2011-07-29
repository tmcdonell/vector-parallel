{-# OPTIONS_GHC -fno-warn-orphans #-}
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

  map, fold, foldMap,
  map_, fold_, foldMap_

) where

import Prelude                                  hiding ( map )
import Control.DeepSeq
import Data.Vector                              ( Vector )
import qualified Data.Vector                    as V
import qualified Data.Vector.Generic.Parallel   as G

instance NFData a => NFData (Vector a) where
  rnf = V.foldl' (flip deepseq) ()


-- | Map a function to each element of an array, in parallel.
--
{-# INLINE map #-}
map :: NFData b => (a -> b) -> Vector a -> Vector b
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
fold :: NFData a => (a -> a -> a) -> a -> Vector a -> a
fold = G.fold


-- | A combination of 'map' followed by 'fold'. The same restrictions apply to
-- the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: NFData b => (a -> b) -> (b -> b -> b) -> b -> Vector a -> b
foldMap = G.foldMap


-- | Like 'map' but only head strict, not fully strict.
--
{-# INLINE map_ #-}
map_ :: (a -> b) -> Vector a -> Vector b
map_ = G.map_

-- | Like 'fold' but only head strict, not fully strict.
--
{-# INLINE fold_ #-}
fold_ :: (a -> a -> a) -> a -> Vector a -> a
fold_ = G.fold_

-- | Like 'foldMap' but only head strict, not fully strict.
--
{-# INLINE foldMap_ #-}
foldMap_ :: (a -> b) -> (b -> b -> b) -> b -> Vector a -> b
foldMap_ = G.foldMap_

